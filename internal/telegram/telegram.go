package telegram

import (
	"air_tgbot/internal/domain"
	"air_tgbot/internal/metrics"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"strings"
	"sync"
	"time"

	"github.com/ikermy/air_common/pkg/com"
	"github.com/ikermy/air_common/pkg/comdb"
	"github.com/ikermy/air_common/pkg/crm"
	"github.com/ikermy/air_common/pkg/crypto"
	"github.com/ikermy/air_common/pkg/endpoint"
	"github.com/ikermy/air_common/pkg/mode"
	"github.com/ikermy/air_common/pkg/model"
	"github.com/ikermy/air_common/pkg/operator"
	"github.com/ikermy/air_logger/v2/pkg/logger"
	"github.com/redis/go-redis/v9"
	tele "gopkg.in/telebot.v4"
)

type Inter interface {
	GetBotUsername(userID uint32) string
	StopUserBot(userID uint32) error
	RestartUserBot(userID uint32) error
	StartUserBot(userID uint32) error
	ProcessWebhookUpdate(token string, body []byte) error
	SetWebhookUserBot(userID uint32) error
}

// IntDB интерфейс внутренних операций с БД специфичных для TgBot
// Только методы которых нет в comdb.Exterior
type IntDB interface {
	GetTgBotUsers(ctx context.Context) ([]domain.UserDetails, error)
	GetTgBotUser(ctx context.Context, userId uint32) (*domain.UserDetails, error)
}

type DB interface {
	ExtDB
	IntDB
}

// ORCClient интерфейс для получения MasterKey пользователя через Landing gRPC
type ORCClient interface {
	GetUserMasterKey(ctx context.Context, userId uint32) ([32]byte, error)
}

type Model = model.Inter
type Endpoint = endpoint.Inter
type Operator = operator.Inter
type ExtDB = comdb.Exterior
type CRM = crm.Inter

// DeltaProcessor — интерфейс для обработки стриминговых дельт.
// Реализуется startpoint.Start (AiR_Common v1.50.33+).
type DeltaProcessor interface {
	ProcessStreamDelta(respId uint64, rawChunk string) (model.StreamDeltaResult, error)
	GetStreamDisplayText(respId uint64) string
	ResetStreamAccumulator(respId uint64)
}

// streamState хранит состояние стриминга для одного респондента.
// rawAccumulated, displayText и messageDone теперь в библиотечном streamAccumulator.
type streamState struct {
	mu                sync.Mutex
	placeholder       *tele.Message // отправленное placeholder-сообщение
	lastDisplayedText string        // последний текст, отправленный в Telegram (для throttle)
	lastEdit          time.Time     // время последнего bot.Edit (throttle)
}

const streamEditThrottle = 100 * time.Millisecond // минимальный интервал между bot.Edit

type Bot struct {
	ctx              context.Context
	cancel           context.CancelFunc
	botReady         chan struct{} // Канал уведомления о готовности бота
	bot              *tele.Bot
	token            *string
	user             *User
	c                *crm.User
	db               DB
	mod              Model
	end              Endpoint
	assist           *model.Assistant
	userId           uint32         // ID пользователя-владельца бота
	isTestBot        bool           // Флаг тестового бота (для unit/integration тестов)
	firstInteraction sync.Map       // key: respId (int64), value: bool (true если это первое сообщение от респондента)
	respIdentifiers  sync.Map       // key: respId (int64), value: string (идентификатор респондента для CRM)
	firstCache       CacheMethods   // при первоначальной загрузке тащит первые контакты из redis
	deltaMode        bool           // режим streamMsgs если включён в настройках бота
	webhookMode      bool           // режим webhook если включён в настройках бота
	streamMsgs       sync.Map       // key: respId (int64), value: *streamState
	deltaProcessor   DeltaProcessor // внедрённый ProcessStreamDelta из startpoint.Start
}

type User struct {
	ctx                  context.Context
	cancel               context.CancelFunc
	mu                   sync.RWMutex
	db                   DB
	mod                  Model
	end                  Endpoint
	crm                  CRM
	rpc                  ORCClient
	firstCache           CacheMethods
	bot                  map[uint32]*Bot
	botByToken           map[string]*Bot // для быстрого поиска бота по токену в режиме webhook
	operatorModeByDialog sync.Map        // key: dialogId (uint64), value: bool
	op                   Operator
	deltaProcessor       DeltaProcessor // ProcessStreamDelta из startpoint.Start

	// колбэк для проброса входящих сообщений в вызывающий слой
	onIncoming func(userID uint32, dialogID uint64) error
}

// SetDeltaProcessor внедряет DeltaProcessor (startpoint.Start) для использования
// в handleDeltaMessage вместо локального дублирования extractStreamText.
func (u *User) SetDeltaProcessor(dp DeltaProcessor) {
	u.deltaProcessor = dp
}

var StartCh = make(chan model.StartCh, 100) // Канал для запуска горутины слушателя

func New(parent context.Context, d DB, m Model, e Endpoint, c CRM, o ORCClient, redisClient redis.UniversalClient) *User {
	ctx, cancel := context.WithCancel(parent)
	t := &User{
		ctx:        ctx,
		cancel:     cancel,
		end:        e,
		db:         d,
		mod:        m,
		crm:        c,
		rpc:        o,
		firstCache: newRedisFirstInteractionCache(redisClient),
		bot:        make(map[uint32]*Bot),
		botByToken: make(map[string]*Bot),
	}

	return t
}

func (u *User) SetOperator(op Operator) { u.op = op }

func (u *User) StopBot() {
	logger.Infoln("Telegram: получен сигнал завершения, закрытие Telegram ботов...")

	// Сначала отменяем контекст для всех горутин
	u.cancel()

	u.mu.RLock()
	bots := make([]*tele.Bot, 0, len(u.bot))
	for _, tg := range u.bot {
		if tg != nil && tg.bot != nil {
			bots = append(bots, tg.bot)
		}
	}
	u.mu.RUnlock()

	if len(bots) == 0 {
		logger.Info("Telegram: Нет активных ботов для остановки")
		return
	}

	// Останавливаем боты параллельно с таймаутом
	var wg sync.WaitGroup
	for _, b := range bots {
		wg.Add(1)
		go func(bot *tele.Bot) {
			defer wg.Done()
			defer func() {
				if r := recover(); r != nil {
					logger.Warn("Паника при остановке бота %s: %v", bot.Me.Username, r)
				}
			}()

			// Создаём канал для отслеживания завершения
			done := make(chan struct{})
			go func() {
				bot.Stop()
				close(done)
			}()

			// Ждём остановки с таймаутом 2 секунды
			select {
			case <-done:
				logger.Info("Бот %s успешно остановлен", bot.Me.Username)
			case <-time.After(2 * time.Second):
				logger.Warn("Таймаут остановки бота %s", bot.Me.Username)
			}
		}(b)
	}

	// Ждём завершения всех ботов не более 5 секунд
	done := make(chan struct{})
	go func() {
		wg.Wait()
		close(done)
	}()

	select {
	case <-done:
		logger.Info("Telegram: Все боты остановлены")
	case <-time.After(5 * time.Second):
		logger.Warn("Telegram: Принудительное завершение после таймаута")
	}
}

func (u *User) StartBots() error {
	logger.Infoln("Запуск Telegram ботов...")

	// Однократно получаем пользователей из БД и запускаем ботов
	if err := u.GetTgBotUsers(); err != nil {
		return fmt.Errorf("ошибка получения пользователей: %w", err)
	}

	logger.Info("Все боты успешно запущены")
	return nil
}

// GetTgBotUsers получает пользователей из БД и создает ботов для них
func (u *User) GetTgBotUsers() error {
	// Получаем данные о пользователях из БД
	details, err := u.db.GetTgBotUsers(u.ctx)
	if err != nil {
		return fmt.Errorf("ошибка получения данных пользователей: %w", err)
	}

	// Если данные пусты, просто выходим
	if len(details) == 0 {
		return nil
	}

	// Обрабатываем каждого активного пользователя из БД
	for _, user := range details {
		userID := uint32(user.UserId)

		// Пропускаем отключенных ботов
		if !user.TgBotEnabled || user.TgBot == "" {
			metrics.ObserveBotLifecycle(userID, "load", "skipped")
			continue
		}

		// Проверяем баланс пользователя
		if err = u.checkUserSubscription(userID); err != nil {
			metrics.ObserveBotLifecycle(userID, "load", "subscription_error")
			continue
		}

		// Созданию/обновлению ботов...
		token, delta, webhook, err := u.parseTokenFromJSON(u.ctx, user.TgBot, userID)
		if err != nil {
			metrics.ObserveBotLifecycle(userID, "load", "token_error")
			continue
		}

		// Создаем или обновляем бота
		assist := u.createAssistantModel(user)
		bot, err := u.createOrUpdateBot(userID, token, delta, webhook, assist, false)
		if err != nil {
			metrics.ObserveBotLifecycle(userID, "load", "failed")
			fmt.Printf("Ошибка при создании/обновлении бота для пользователя %d: %v", userID, err)
			continue
		}

		u.mu.Lock()
		u.bot[userID] = bot
		if webhook {
			u.botByToken[token] = bot
		}
		u.mu.Unlock()
		metrics.ObserveBotLifecycle(userID, "load", "success")
	}

	return nil
}

// StartUserBot запускает бота для конкретного пользователя по userId
func (u *User) StartUserBot(userID uint32) error {
	metrics.ObserveBotLifecycle(userID, "start", "attempt")
	// Получаем данные пользователя из БД
	userDetails, err := u.db.GetTgBotUser(u.ctx, userID)
	if err != nil {
		metrics.ObserveBotLifecycle(userID, "start", "db_error")
		return fmt.Errorf("ошибка получения данных пользователя %d: %w", userID, err)
	}

	// Если пользователь не найден или отключен
	if userDetails == nil {
		metrics.ObserveBotLifecycle(userID, "start", "not_found")
		return fmt.Errorf("пользователь %d не найден или бот отключен", userID)
	}

	// Проверяем, что бот включен и токен не пустой
	if !userDetails.TgBotEnabled || userDetails.TgBot == "" {
		metrics.ObserveBotLifecycle(userID, "start", "disabled")
		return fmt.Errorf("бот для пользователя %d отключен или токен пустой", userID)
	}

	// Проверяем подписку пользователя
	if err := u.checkUserSubscription(userID); err != nil {
		metrics.ObserveBotLifecycle(userID, "start", "subscription_error")
		return fmt.Errorf("ошибка проверки подписки пользователя %d: %w", userID, err)
	}

	// Парсим токен из JSON
	token, delta, webhook, err := u.parseTokenFromJSON(u.ctx, userDetails.TgBot, userID)
	if err != nil {
		metrics.ObserveBotLifecycle(userID, "start", "token_error")
		return fmt.Errorf("ошибка парсинга токена для пользователя %d: %w", userID, err)
	}
	// Создаем модель ассистента
	assist := u.createAssistantModel(*userDetails)

	// Создаем или обновляем бота
	bot, err := u.createOrUpdateBot(userID, token, delta, webhook, assist, true)
	if err != nil {
		metrics.ObserveBotLifecycle(userID, "start", "failed")
		return fmt.Errorf("ошибка создания/обновления бота для пользователя %d: %w", userID, err)
	}

	// Сохраняем бота в карте
	u.mu.Lock()
	u.bot[userID] = bot
	if webhook {
		u.botByToken[token] = bot
	}
	u.mu.Unlock()
	metrics.ObserveBotLifecycle(userID, "start", "success")

	logger.Info("Бот для успешно запущен", userID)
	return nil
}

// parseTokenFromJSON расшифровывает (если нужно) и парсит токен из JSON строки.
// Если значение зашифровано MasterKey ($mk$...), получает ключ через orcClient.
func (u *User) parseTokenFromJSON(ctx context.Context, tokenJSON string, userID uint32) (string, bool, bool, error) {
	raw := tokenJSON

	// Если поле зашифровано MasterKey — расшифровываем
	if crypto.IsEncryptedWithMasterKey(raw) {
		if u.rpc == nil {
			metrics.ObserveDecryptError(userID, "missing_orc_client")
			logger.Error("orcClient не инициализирован, невозможно расшифровать токен", userID)
			return "", false, false, fmt.Errorf("orcClient не инициализирован")
		}

		mk, err := u.rpc.GetUserMasterKey(ctx, userID)
		if err != nil {
			metrics.ObserveDecryptError(userID, "master_key")
			logger.Error("Ошибка получения MasterKey: %v (требуется вход на Landing)", err, userID)

			notifyMsg := com.CarpCh{
				Event:  "reauth-userkey",
				UserID: userID,
			}
			if err := u.end.SendNotification(notifyMsg); err != nil {
				logger.Error("Ошибка отправки уведомления о повторной аутентификации %v", err, userID)
			}

			return "", false, false, fmt.Errorf("ошибка получения MasterKey для пользователя %d: %w", userID, err)
		}

		decrypted, err := crypto.DecryptFieldWithMasterKey(mk, raw)
		if err != nil {
			metrics.ObserveDecryptError(userID, "decrypt")
			logger.Error("Ошибка расшифрования токена: %v", userID, err)
			return "", false, false, fmt.Errorf("ошибка расшифрования токена: %w", err)
		}
		raw = decrypted
	}

	type options struct {
		Delta   bool `json:"delta"`
		WebHook bool `json:"webhook"`
	}

	var config struct {
		Token   string   `json:"token"`
		Options *options `json:"options,omitempty"`
	}

	if err := json.Unmarshal([]byte(raw), &config); err != nil {
		metrics.ObserveDecryptError(userID, "json_parse")
		logger.Error("Ошибка парсинга JSON токена: %v", userID, err)
		return "", false, false, err
	}
	if config.Token == "" {
		metrics.ObserveDecryptError(userID, "empty_token")
		logger.Error("Пустой токен в JSON", userID)
		return "", false, false, fmt.Errorf("пустой токен")
	}

	delta := config.Options != nil && config.Options.Delta
	webhook := config.Options != nil && config.Options.WebHook

	return config.Token, delta, webhook, nil
}

// Создает модель ассистента
func (u *User) createAssistantModel(user domain.UserDetails) *model.Assistant {
	return &model.Assistant{
		UserID:     uint32(user.UserId),
		AssistName: user.AssistName,
		AssistId:   user.AssistantId,
		Provider:   user.Provider,
		Espero:     user.Espero,
		Limit:      user.AskLimit,
		Ignore:     user.Ignore,
		Events: model.Notifications{
			Start:  user.Events.Start,
			End:    user.Events.End,
			Target: user.Events.Target,
		},
		Metas: model.Target{
			MetaAction: user.MetaAction,
			Triggers:   user.Triggers,
		},
	}
}

// Проверяет подписку пользователя
func (u *User) checkUserSubscription(userID uint32) error {
	err := com.CheckUserSubscription(u.db, userID)
	if err != nil {
		var commonErr *com.SubscriptionError
		ok := errors.As(err, &commonErr)
		if ok {
			u.SendSubscriptionError(commonErr)
		} else {
			logger.Error("Неизвестная ошибка проверки подписки: %v", err)
		}
		logger.Error("Ошибка проверки подписки: %v", userID, err)
		return err
	}
	return nil
}

// Создает нового бота или обновляет существующего
func (u *User) createOrUpdateBot(userID uint32, token string, delta, webhook bool, assist *model.Assistant, forceUpdate bool) (*Bot, error) {
	// Проверяем, существует ли уже такой бот
	existingBot, exists := u.bot[userID]

	// Если бот существует
	if exists {
		currentToken := ""
		if existingBot.token != nil {
			currentToken = *existingBot.token
		}

		existingBot.deltaMode = delta
		existingBot.webhookMode = webhook

		// Если не требуется принудительное обновление и токен не изменился
		if !forceUpdate && currentToken == token {
			return existingBot, nil
		}

		// Если требуется обновление или токен изменился, останавливаем старый бот
		if existingBot.bot != nil {
			logger.Debug("Пересоздание бота", userID)
			existingBot.bot.Stop()
			// Удаляем из мапы по токену если был
			u.mu.Lock()
			for t, b := range u.botByToken {
				if b == existingBot {
					delete(u.botByToken, t)
					break
				}
			}
			u.mu.Unlock()
		}
	}

	// Создаем новый бот
	return u.initializeBot(userID, token, delta, webhook, assist)
}

// Инициализирует нового бота
func (u *User) initializeBot(userID uint32, token string, delta, webhook bool, assist *model.Assistant) (*Bot, error) {
	metrics.SetActiveSessions(userID, "starting", 1)
	metrics.SetActiveSessions(userID, "running", 0)
	botCtx, botCancel := context.WithCancel(u.ctx)

	// Копируем токен, чтобы сохранить его в структуре Bot
	localToken := token

	newBot := Bot{
		ctx:            botCtx,
		cancel:         botCancel,
		user:           u,
		botReady:       make(chan struct{}),
		bot:            nil, // Будет инициализирован ниже
		token:          &localToken,
		assist:         assist,
		userId:         userID,
		db:             u.db,
		mod:            u.mod,
		end:            u.end,
		firstCache:     u.firstCache,
		deltaProcessor: u.deltaProcessor, // Пробрасываем для использования в handleDeltaMessage
		deltaMode:      delta,
		webhookMode:    webhook,
	}

	go newBot.preloadFirstInteraction()

	// Для webhook-режима не используем LongPoller — он конфликтует с webhook.
	var poller tele.Poller
	if !webhook {
		poller = &tele.LongPoller{Timeout: 30 * time.Second}
	}

	botInstance, err := tele.NewBot(tele.Settings{
		Token:  token,
		Poller: poller,
	})

	if err != nil {
		metrics.ObserveReconnect(userID, "failed")
		metrics.SetActiveSessions(userID, "starting", 0)
		return &Bot{}, fmt.Errorf("ошибка создания бота: %w", err)
	}
	metrics.ObserveReconnect(userID, "success")

	newBot.bot = botInstance

	// Регистрируем обработчики
	newBot.registerMessageHandler()

	// Инициализируем CRM для этого пользователя
	crmUser, debug, err := u.crm.Init(userID)
	if err != nil {
		// Может быть не ошибка, просто не настроена или отключена CRM
		logger.Warn("Ошибка инициализации CRM: %v", err, userID)
	}

	if debug != "" {
		logger.Debug("User инициализирован с настройками CRM: %s", debug, userID)
	}

	newBot.c = crmUser

	if webhook {
		// Webhook: регистрируем синхронно ДО return, гарантируя что Telegram получит URL
		// до того, как бот попадёт в botByToken и начнёт принимать входящие запросы.
		webhookURL := fmt.Sprintf("%s/open/tgbot/webhook/%s", mode.GetRealHost(), token)
		if err := botInstance.SetWebhook(&tele.Webhook{
			Endpoint: &tele.WebhookEndpoint{PublicURL: webhookURL},
		}); err != nil {
			logger.Error("Ошибка установки Webhook для %s: %v", botInstance.Me.Username, err)
			return &Bot{}, fmt.Errorf("ошибка установки webhook: %w", err)
		}
		logger.Debug("Бот %s запущен в режиме Webhook: %s", botInstance.Me.Username, webhookURL, userID)

		go func() {
			defer func() {
				metrics.SetActiveSessions(userID, "running", 0)
				metrics.SetActiveSessions(userID, "starting", 0)
			}()
			close(newBot.botReady)
			metrics.SetActiveSessions(userID, "starting", 0)
			metrics.SetActiveSessions(userID, "running", 1)
		}()

		return &newBot, nil
	}

	errCh := make(chan error, 1)
	// Long Polling: удаляем webhook и запускаем polling в горутине.
	go func() {
		defer func() {
			metrics.SetActiveSessions(userID, "running", 0)
			metrics.SetActiveSessions(userID, "starting", 0)
			if r := recover(); r != nil {
				errCh <- fmt.Errorf("паника при запуске бота: %v", r)
			}
		}()

		close(newBot.botReady)
		metrics.SetActiveSessions(userID, "starting", 0)
		metrics.SetActiveSessions(userID, "running", 1)

		err = botInstance.RemoveWebhook()
		if err != nil {
			errCh <- fmt.Errorf("ошибка удаления webhook: %w", err)
			return
		}
		logger.Debug("Бот %s запущен в режиме Long Polling", botInstance.Me.Username, userID)

		stopCh := make(chan struct{})
		go func() {
			botInstance.Start()
			close(stopCh)
		}()

		// Ждём либо завершения контекста, либо естественной остановки бота
		select {
		case <-u.ctx.Done():
			logger.Debug("Остановка бота %s по сигналу контекста", botInstance.Me.Username, userID)
			botInstance.Stop()
			<-stopCh // Ждём фактического завершения
		case <-botCtx.Done():
			logger.Debug("Остановка бота %s по сигналу botCtx", botInstance.Me.Username, userID)
			botInstance.Stop()
			<-stopCh
		case <-stopCh:
			logger.Debug("Бот %s завершился", botInstance.Me.Username, userID)
		}

		errCh <- nil
	}()

	if err := <-errCh; err != nil {
		return &Bot{}, err
	}

	close(errCh)

	return &newBot, nil
}

// ensureResponderSession пересоздаёт модель, каналы и слушателя ответа.
func (b *Bot) ensureResponderSession(telegramID int64, senderName string) (*model.Ch, error) {
	// Получаем/создаём диалог
	dialogId, err := b.db.GetOrSetTreadAndResponder(b.userId, uint64(telegramID), senderName, comdb.TelegramBot)
	if err != nil {
		return nil, fmt.Errorf("GetOrSetTreadAndResponder: %w", err)
	}

	// Пересоздаём/получаем модель
	usrMod, err := b.mod.GetOrSetRespGPT(*b.assist, dialogId, uint64(telegramID), senderName)
	if err != nil {
		return nil, fmt.Errorf("GetOrSetRespGPT: %w", err)
	}

	// Получаем новые каналы
	usrCh, err := b.mod.GetCh(uint64(telegramID))
	if err != nil {
		return nil, fmt.Errorf("GetCh: %w", err)
	}

	// Стартуем слушателя (если прежний завершился, это безопасно)
	start := model.StartCh{
		Ctx:     b.ctx,
		Model:   usrMod,
		Chanel:  usrCh,
		TreadId: dialogId,
		RespId:  uint64(telegramID),
	}

	logger.Debug("ensureResponderSession: отправка startCh для respId=%d, dialogId=%d", uint64(telegramID), dialogId, b.userId)

	select {
	case StartCh <- start:
		logger.Debug("ensureResponderSession: startCh успешно отправлен для respId=%d", uint64(telegramID), b.userId)
	default:
		logger.Warn("Ошибка при отправке данных в StartCh (ensureResponderSession)", b.userId)
	}

	// Получаем сохранённый идентификатор или создаём fallback
	respIdentifier := ""
	if value, exists := b.respIdentifiers.Load(telegramID); exists {
		respIdentifier = value.(string)
	} else {
		// Fallback - используем tg_ID
		respIdentifier = fmt.Sprintf("tg_%d", telegramID)
	}

	b.startResponseListener(telegramID, respIdentifier)
	return usrCh, nil
}

func (b *Bot) preloadFirstInteraction() {
	if b.firstCache == nil {
		return
	}

	senderIDs, err := b.firstCache.LoadUser(b.ctx, b.userId)
	if err != nil {
		logger.Warn("Redis: не удалось прогреть firstInteraction: %v", err, b.userId)
		return
	}

	for _, senderID := range senderIDs {
		b.firstInteraction.Store(senderID, false)
	}

	if len(senderIDs) > 0 {
		logger.Debug("Redis: прогрето firstInteraction=%d", len(senderIDs), b.userId)
	}
}

// RegisterMessageHandler регистрирует обработчик сообщений для бота
func (b *Bot) registerMessageHandler() {
	if b == nil {
		logger.Warn("Ошибка: попытка регистрации обработчика для nil бота")
		return
	}

	// Middleware для инициализации сессии с респондентом
	b.bot.Use(b.sessionInitMiddleware())

	// Регистрируем обработчики для разных типов сообщений
	b.registerTextHandler()
	b.registerVoiceHandler()
	b.registerDocumentHandler()
}

// sessionInitMiddleware создаёт middleware для инициализации сессий с респондентами
func (b *Bot) sessionInitMiddleware() func(next tele.HandlerFunc) tele.HandlerFunc {
	return func(next tele.HandlerFunc) tele.HandlerFunc {
		return func(c tele.Context) error {
			sender := c.Sender()
			respName := formatSender(sender)
			respId := uint64(sender.ID)
			respIdentifier := b.getResponderIdentifier(sender)

			// Проверяем, существуют ли каналы для этого респондента
			_, err := b.mod.GetCh(respId)
			if err != nil {
				// Каналов нет - инициализируем новую сессию
				if err := b.initializeResponderSession(sender.ID, respName, respId, respIdentifier); err != nil {
					logger.Error("Ошибка инициализации сессии: %v", err, b.userId)
					return err
				}
			}

			return next(c)
		}
	}
}

// initializeResponderSession инициализирует новую сессию для респондента
func (b *Bot) initializeResponderSession(senderId int64, respName string, respId uint64, respIdentifier string) error {
	startedAt := time.Now()
	// Получаем ID диалога
	dialogId, err := b.db.GetOrSetTreadAndResponder(b.userId, respId, respName, comdb.TelegramBot)
	if err != nil {
		metrics.ObserveUserChannelInit(b.userId, "dialog_error", startedAt)
		return fmt.Errorf("ошибка при создании диалога: %w", err)
	}

	// Создаём/получаем модель пользователя
	usrMod, err := b.mod.GetOrSetRespGPT(*b.assist, dialogId, respId, respName)
	if err != nil {
		metrics.ObserveUserChannelInit(b.userId, "model_error", startedAt)
		logger.Error("Ошибка при создании модели пользователя: %v", err, b.userId)
		return err
	}

	// Получаем каналы пользователя
	usrCh, err := b.mod.GetCh(respId)
	if err != nil {
		metrics.ObserveUserChannelInit(b.userId, "channel_error", startedAt)
		logger.Error("Ошибка при получении канала пользователя: %v", err, b.userId)
		return err
	}

	// TODO разобраться с этим хотя весь смысл в том как я сохраняю b.firstInteraction.Store(senderId, first)
	// Отмечаем первое взаимодействие
	first := true
	if b.firstCache != nil {
		exists, err := b.firstCache.Has(b.ctx, b.userId, senderId)
		if err != nil {
			logger.Warn("Redis: ошибка проверки firstInteraction senderId=%d: %v", senderId, err, b.userId)
		} else if exists {
			first = false
		} else if err := b.firstCache.Set(b.ctx, b.userId, senderId); err != nil {
			logger.Warn("Redis: ошибка сохранения firstInteraction senderId=%d: %v", senderId, err, b.userId)
		}
	}
	b.firstInteraction.Store(senderId, first)

	// Запускаем слушателя ответов
	b.startResponseListener(senderId, respIdentifier)

	// Отправляем данные в канал запуска
	startCh := model.StartCh{
		Ctx:     b.ctx,
		Model:   usrMod,
		Chanel:  usrCh,
		TreadId: dialogId,
		RespId:  respId,
	}

	select {
	case StartCh <- startCh:
	default:
		metrics.ObserveUserChannelInit(b.userId, "start_channel_full", startedAt)
		logger.Warn("Ошибка при отправке данных в StartCh", b.userId)
	}

	metrics.ObserveUserChannelInit(b.userId, "success", startedAt)
	return nil
}

// registerTextHandler регистрирует обработчик текстовых сообщений
func (b *Bot) registerTextHandler() {
	b.bot.Handle(tele.OnText, func(c tele.Context) error {
		files, err := b.extractFilesFromMessage(c)
		if err != nil {
			logger.Error("Ошибка извлечения файлов: %v", err, b.userId)
			return err
		}

		content := model.AssistResponse{Message: c.Text()}
		return b.handleUserMessage(c, "user", content, files)
	})
}

// registerVoiceHandler регистрирует обработчик голосовых сообщений
func (b *Bot) registerVoiceHandler() {
	if !mode.IsAudioModeEnabled() {
		return
	}

	b.bot.Handle(tele.OnVoice, func(c tele.Context) error {
		text, files, err := b.processVoiceMessage(c)
		if err != nil {
			return err
		}

		content := model.AssistResponse{Message: text}
		return b.handleUserMessage(c, "user_voice", content, files)
	})
}

// registerDocumentHandler регистрирует обработчик документов
func (b *Bot) registerDocumentHandler() {
	b.bot.Handle(tele.OnDocument, func(c tele.Context) error {
		files, err := b.extractFilesFromMessage(c)
		if err != nil {
			logger.Error("Ошибка извлечения файлов: %v", err, b.userId)
			return err
		}

		content := model.AssistResponse{Message: c.Message().Caption}
		return b.handleUserMessage(c, "user", content, files)
	})
}

// processVoiceMessage обрабатывает голосовое сообщение и возвращает транскрибированный текст
func (b *Bot) processVoiceMessage(c tele.Context) (string, []model.FileUpload, error) {
	voice := c.Message().Voice
	if voice == nil {
		logger.Info("Нет voice в сообщении")
		return "", nil, errors.New("нет голосового сообщения")
	}

	// Скачиваем аудиофайл
	file := &tele.File{FileID: voice.FileID}
	audioReader, err := b.bot.File(file)
	if err != nil {
		logger.Error("Ошибка скачивания аудиофайла: %v", err)
		return "", nil, err
	}
	defer func() {
		if err := audioReader.Close(); err != nil {
			logger.Error("Ошибка закрытия аудиофайла: %v", err)
		}
	}()

	// Читаем данные
	audioData, err := io.ReadAll(audioReader)
	if err != nil {
		logger.Error("Ошибка чтения аудиофайла: %v", err)
		return "", nil, err
	}

	// Telegram голосовые сообщения приходят в формате OGG с кодеком Opus
	// Google Gemini требует указания кодека для OGG файлов
	// Поддерживаемые форматы: https://ai.google.dev/gemini-api/docs/audio
	mimeType := "audio/ogg; codecs=opus"

	logger.Debug("Транскрибация голосового сообщения: размер=%d байт, mime=%s", len(audioData), mimeType, b.userId)

	// Транскрибируем
	text, err := b.mod.TranscribeAudio(b.userId, audioData, mimeType)
	if err != nil {
		logger.Error("Ошибка транскрибирования аудио: %v", err)
		return "", nil, err
	}

	// Извлекаем файлы (если есть)
	files, err := b.extractFilesFromMessage(c)
	if err != nil {
		logger.Error("Ошибка извлечения файлов: %v", err, b.userId)
		files = []model.FileUpload{}
	}

	return text, files, nil
}

// handleUserMessage обрабатывает входящее сообщение от пользователя
func (b *Bot) handleUserMessage(c tele.Context, msgType string, content model.AssistResponse, files []model.FileUpload) error {
	startedAt := time.Now()
	metrics.ObserveMessageReceived(b.userId, msgType)
	sender := c.Message().Sender
	senderName := formatSender(sender)
	respId := uint64(sender.ID)
	respIdentifier := b.getResponderIdentifier(sender)

	// Сохраняем идентификатор респондента для использования в startResponseListener
	b.respIdentifiers.Store(sender.ID, respIdentifier)

	// Получаем каналы или пересоздаём сессию
	usrCh, err := b.getOrRecreateChannel(respId, sender.ID, senderName)
	if err != nil {
		metrics.ObserveMessageIgnored(b.userId, "channel_error")
		metrics.ObserveMessageProcessed(b.userId, "failed")
		metrics.ObserveMessageProcessingStage(b.userId, "handle_user_message", startedAt)
		return nil
	}

	// Показываем индикатор "печатает" пока модель генерирует ответ
	if err := b.bot.Notify(c.Message().Sender, tele.Typing); err != nil {
		logger.Debug("Ошибка отправки Typing-индикатора для respId %d: %v", sender.ID, err, b.userId)
	}

	// Определяем первое взаимодействие и отправляем уведомление
	first := b.handleFirstInteraction(sender, senderName)

	// Создаём и отправляем сообщение в модель
	msg := b.createMessage(msgType, content, files, senderName, usrCh.DialogID)

	// Отправляем в CRM
	b.sendToCRM(respIdentifier, senderName, content.Message, msgType, first, files)

	// Отправляем в канал модели
	b.sendToModel(usrCh, msg, sender.ID, senderName)
	metrics.ObserveMessageProcessed(b.userId, "success")
	metrics.ObserveMessageProcessingStage(b.userId, "handle_user_message", startedAt)

	return nil
}

// getResponderIdentifier возвращает идентификатор респондента для CRM
func (b *Bot) getResponderIdentifier(sender *tele.User) string {
	if sender.Username != "" {
		return "@" + sender.Username
	}
	return fmt.Sprintf("tg_%d", sender.ID)
}

// getOrRecreateChannel получает каналы или пересоздаёт сессию при необходимости
func (b *Bot) getOrRecreateChannel(respId uint64, senderId int64, senderName string) (*model.Ch, error) {
	usrCh, err := b.mod.GetCh(respId)
	if err != nil {
		logger.Warn("Каналы не найдены для %d, выполняю пересоздание: %v", senderId, err, b.userId)
		if usrCh, err = b.ensureResponderSession(senderId, senderName); err != nil {
			logger.Error("Не удалось пересоздать сессию для %d: %v", senderId, err, b.userId)
			return nil, err
		}
	}
	return usrCh, nil
}

// handleFirstInteraction проверяет и обрабатывает первое взаимодействие
func (b *Bot) handleFirstInteraction(sender *tele.User, senderName string) bool {
	first := false
	if value, exists := b.firstInteraction.LoadAndDelete(sender.ID); exists {
		first = value.(bool)
	}
	if !first {
		return false
	}

	// nil для тестовых вызовов
	if first && b.assist.Events.Start && b.end != nil {
		notifyMsg := com.CarpCh{
			Event:      "start",
			UserName:   senderName,
			AssistName: b.assist.AssistName,
			Target:     "",
			UserID:     b.userId,
		}
		if err := b.end.SendNotification(notifyMsg); err != nil {
			logger.Error("Ошибка отправки уведомления о первом взаимодействии: %v", err, b.userId)
		}
	}

	return first
}

// createMessage создаёт сообщение для отправки в модель
func (b *Bot) createMessage(msgType string, content model.AssistResponse, files []model.FileUpload, senderName string, dialogId uint64) model.Message {
	operatorMode := b.user.IsOperatorMode(dialogId)
	operatorInfo := model.Operator{SetOperator: operatorMode, SenderName: senderName}

	return b.mod.NewMessage(operatorInfo, msgType, &content, &senderName, files...)
}

// sendToCRM отправляет сообщение в CRM
func (b *Bot) sendToCRM(respIdentifier, senderName, message, msgType string, first bool, files []model.FileUpload) {
	if b.c == nil {
		metrics.ObserveCRMRequest(b.userId, "outbound", "skipped")
		return
	}
	startedAt := time.Now()

	isVoice := msgType == "user_voice"
	fileNames := make([]string, 0, len(files))
	for _, file := range files {
		fileNames = append(fileNames, file.Name)
	}

	csg := b.c.MSG("user", senderName, message).
		WithAltContact(respIdentifier).
		NewDialog(first).
		WithVoice(isVoice).
		WithFiles(fileNames...)

	if err := b.c.SendMessage(csg); err != nil {
		metrics.ObserveCRMRequest(b.userId, "outbound", "error")
		metrics.ObserveCRMRequestDuration(b.userId, "outbound", startedAt)
		logger.Error("Ошибка отправки сообщения в CRM: %v", err, b.userId)
		return
	}
	metrics.ObserveCRMRequest(b.userId, "outbound", "success")
	metrics.ObserveCRMRequestDuration(b.userId, "outbound", startedAt)
}

// sendToModel отправляет сообщение в канал модели с автоматическим рестартом при необходимости
func (b *Bot) sendToModel(usrCh *model.Ch, msg model.Message, senderId int64, senderName string) {
	if err := b.user.trySendToRxCh(usrCh, msg); err != nil {
		logger.Warn("RxCh недоступен (закрыт или переполнен), пробую рестарт: %v", err, b.userId)
		if usrCh, rerr := b.ensureResponderSession(senderId, senderName); rerr == nil {
			_ = b.user.trySendToRxCh(usrCh, msg)
		} else {
			logger.Error("Рестарт сессии не удался: %v", rerr, b.userId)
		}
	}
}

// trySendToRxCh пытается отправить сообщение в RxCh с таймаутом.
func (u *User) trySendToRxCh(usrCh *model.Ch, msg model.Message) error {
	if err := usrCh.SendToRx(msg); err != nil {
		return fmt.Errorf("не удалось отправить сообщение в RxCh: %w", err)
	}
	return nil
}

// processAssistantResponse обрабатывает ответ ассистента и выполняет соответствующие действия
func (b *Bot) processAssistantResponse(dialogId uint64, respId int64, msg model.Message) error {
	startedAt := time.Now()
	// Проверяем корректность инициализации бота
	if b.bot == nil {
		metrics.ObserveMessageProcessed(b.userId, "bot_nil")
		metrics.ObserveMessageProcessingStage(b.userId, "assistant_response", startedAt)
		logger.Error("b.bot равен nil в processAssistantResponse для respId %d", respId, b.userId)
		return fmt.Errorf("бот не инициализирован")
	}

	// Пропускаем пустые сообщения
	if msg.Content.Message == "" && len(msg.Content.Action.SendFiles) == 0 {
		metrics.ObserveMessageIgnored(b.userId, "empty_assistant_message")
		metrics.ObserveMessageProcessingStage(b.userId, "assistant_response", startedAt)
		logger.Warn("Пустое сообщение от ассистента", respId, b.userId)
		return nil
	}

	if msg.Type == "user" {
		metrics.ObserveMessageIgnored(b.userId, "user_echo")
		metrics.ObserveMessageProcessingStage(b.userId, "assistant_response", startedAt)
		return nil // Пропускаем пользовательские сообщения
	}

	recipient := &tele.User{ID: respId}

	// Проверяю есть ли пометка операторского сообщения
	// Проверяю в сообщении команду выключения режима оператора
	if b.user != nil && msg.Operator.Operator && msg.Operator.SetOperator && !b.user.IsOperatorMode(dialogId) {
		// Включаю для респондента режим оператора
		b.user.SetOperatorMode(dialogId, true)
		logger.Debug("Включен режим оператора для диалога %d", dialogId, b.userId)
	}

	// Обрабатываем файлы, если они есть
	if len(msg.Content.Action.SendFiles) > 0 {
		for _, file := range msg.Content.Action.SendFiles {
			if err := b.sendFileToUser(recipient, file); err != nil {
				metrics.ObserveTgBotSend(b.userId, "file_error")
				logger.Error("Ошибка отправки файла пользователю %d: %v", respId, err, b.userId)
			}
		}

		// В streaming-режиме placeholder уже мог быть отправлен до того,
		// как стало известно о файлах. Если текстового ответа нет, placeholder
		// с промежуточным JSON нужно удалить после отправки файлов.
		if msg.Content.Message == "" {
			if raw, ok := b.streamMsgs.Load(respId); ok {
				ss := raw.(*streamState)
				ss.mu.Lock()
				placeholder := ss.placeholder
				ss.mu.Unlock()
				if placeholder != nil {
					if err := b.bot.Delete(placeholder); err != nil {
						logger.Debug("Не удалось удалить streaming-placeholder для respId=%d: %v", respId, err, b.userId)
					}
				}
				b.streamMsgs.Delete(respId)
			}
			metrics.ObserveMessageProcessingStage(b.userId, "assistant_response", startedAt)
			return nil
		}
	}

	// Отправка текстового сообщения
	if msg.Content.Message != "" {
		preparedText, sendOptions := detectAndPrepareText(msg.Content.Message)

		// Проверяем, есть ли активный стриминг-placeholder для этого respId
		if raw, ok := b.streamMsgs.Load(respId); ok {
			ss := raw.(*streamState)
			ss.mu.Lock()
			placeholder := ss.placeholder
			ss.mu.Unlock()

			logger.Debug("processAssistantResponse [respId=%d]: найден streamState, placeholder=%v", respId, placeholder != nil, b.userId)

			if placeholder != nil {
				// Финальный Edit с полным текстом и форматированием
				var editErr error
				if sendOptions != nil {
					_, editErr = b.bot.Edit(placeholder, preparedText, sendOptions)
				} else {
					_, editErr = b.bot.Edit(placeholder, preparedText)
				}

				if editErr != nil {
					if strings.Contains(editErr.Error(), "message is not modified") {
						metrics.ObserveTgBotSend(b.userId, "not_modified")
						metrics.ObserveMessageProcessingStage(b.userId, "assistant_response", startedAt)
						logger.Debug("processAssistantResponse [respId=%d]: placeholder уже актуален", respId, b.userId)
						b.streamMsgs.Delete(respId)
						return nil
					}
					logger.Debug("processAssistantResponse [respId=%d]: ошибка финального Edit: %v, отправляю новым сообщением", respId, editErr, b.userId)
					sendStartedAt := time.Now()
					if sendOptions != nil {
						_, editErr = b.bot.Send(recipient, preparedText, sendOptions)
					} else {
						_, editErr = b.bot.Send(recipient, preparedText)
					}
					metrics.ObserveTgBotSendDuration(b.userId, sendStartedAt)
					if editErr != nil {
						metrics.ObserveTgBotSend(b.userId, "error")
						_, _ = b.bot.Send(recipient, msg.Content.Message)
					} else {
						metrics.ObserveTgBotSend(b.userId, "success")
					}
				} else {
					metrics.ObserveTgBotSend(b.userId, "success")
					logger.Debug("processAssistantResponse [respId=%d]: финальный Edit успешен, mode=%v, len=%d", respId, sendOptions, len(preparedText), b.userId)
				}

				b.streamMsgs.Delete(respId)
				metrics.ObserveMessageProcessingStage(b.userId, "assistant_response", startedAt)
				return nil
			}

			b.streamMsgs.Delete(respId)
		} else {
			logger.Debug("processAssistantResponse [respId=%d]: streamState НЕ найден → обычный Send", respId, b.userId)
		}

		// Стриминга не было — отправляем как обычно
		var err error
		sendStartedAt := time.Now()
		if sendOptions != nil {
			_, err = b.bot.Send(recipient, preparedText, sendOptions)
		} else {
			_, err = b.bot.Send(recipient, preparedText)
		}
		metrics.ObserveTgBotSendDuration(b.userId, sendStartedAt)

		if err != nil {
			metrics.ObserveTgBotSend(b.userId, "error")
			logger.Error("Ошибка отправки текстового сообщения пользователю %d: %v", respId, err, b.userId)
			_, retryErr := b.bot.Send(recipient, msg.Content.Message)
			if retryErr != nil {
				logger.Error("Повторная ошибка отправки (plain text) пользователю %d: %v", respId, retryErr, b.userId)
			}
		} else {
			metrics.ObserveTgBotSend(b.userId, "success")
		}
	}
	metrics.ObserveMessageProcessingStage(b.userId, "assistant_response", startedAt)

	return nil
}

// handleDeltaMessage обрабатывает дельта-сообщение от стриминга ассистента.
// Использует библиотечный ProcessStreamDelta (AiR_Common v1.50.33+) вместо
// локального extractStreamText. JSON-события (function calls, token_usage и т.п.)
// корректно детектятся библиотекой и пропускаются (Kind == StreamDeltaKindEvent).
func (b *Bot) handleDeltaMessage(respId int64, rawChunk string) {
	if rawChunk == "" {
		logger.Debug("handleDeltaMessage [respId=%d]: пустой чанк, пропускаем", respId, b.userId)
		return
	}

	// Используем библиотечный ProcessStreamDelta для обработки дельты
	result, err := b.deltaProcessor.ProcessStreamDelta(uint64(respId), rawChunk)
	if err != nil {
		logger.Debug("handleDeltaMessage [respId=%d]: ошибка ProcessStreamDelta: %v", respId, err, b.userId)
		// Продолжаем — текст может быть частично извлечён даже при ошибке
	}

	// JSON-события (function calls, token_usage и т.п.) пропускаем —
	// библиотека их корректно детектирует и обрабатывает внутри.
	if result.Kind == model.StreamDeltaKindEvent {
		logger.Debug("handleDeltaMessage [respId=%d]: пропуск JSON-события (type=%s)", respId, result.EventType, b.userId)
		return
	}

	// Текстовые дельты
	if result.Text == "" {
		logger.Debug("handleDeltaMessage [respId=%d]: пустой текст, пропускаем", respId, b.userId)
		return
	}

	displayText, _ := detectAndPrepareText(result.Text)
	if displayText == "" {
		logger.Debug("handleDeltaMessage [respId=%d]: подготовленный текст пустой, пропускаем", respId, b.userId)
		return
	}

	recipient := &tele.User{ID: respId}

	// Получаем или создаём streamState для этого respId
	raw, loaded := b.streamMsgs.LoadOrStore(respId, &streamState{})
	ss := raw.(*streamState)

	logger.Debug("handleDeltaMessage [respId=%d]: loaded=%v, chunk_len=%d, text_len=%d, complete=%v",
		respId, loaded, len(rawChunk), len(displayText), result.Complete, b.userId)

	ss.mu.Lock()
	defer ss.mu.Unlock()

	// Если текст не изменился — пропускаем Edit (throttle)
	if displayText == ss.lastDisplayedText {
		logger.Debug("handleDeltaMessage [respId=%d]: текст не изменился (%d симв.), пропускаем", respId, len(displayText), b.userId)
		return
	}

	if ss.placeholder == nil {
		// Первая значимая дельта — отправляем начальное сообщение
		logger.Debug("handleDeltaMessage [respId=%d]: первая дельта, отправляю placeholder", respId, b.userId)
		sent, err := b.bot.Send(recipient, displayText)
		if err != nil {
			logger.Debug("handleDeltaMessage [respId=%d]: ошибка отправки placeholder: %v", respId, err, b.userId)
			// Удаляем запись — финальный ответ отправится обычным Send
			// Очищаем библиотечный накопитель
			b.deltaProcessor.ResetStreamAccumulator(uint64(respId))
			b.streamMsgs.Delete(respId)
			return
		}
		ss.placeholder = sent
		ss.lastDisplayedText = displayText
		ss.lastEdit = time.Now()
		logger.Debug("handleDeltaMessage [respId=%d]: placeholder отправлен, msgID=%d", respId, sent.ID, b.userId)
		return
	}

	// Последующие дельты — Edit с throttle
	elapsed := time.Since(ss.lastEdit)
	if elapsed < streamEditThrottle {
		logger.Debug("handleDeltaMessage [respId=%d]: throttle, пропускаем Edit (elapsed=%v)", respId, elapsed, b.userId)
		return
	}

	logger.Debug("handleDeltaMessage [respId=%d]: Edit text_len=%d", respId, len(displayText), b.userId)
	_, err = b.bot.Edit(ss.placeholder, displayText)
	if err != nil {
		errStr := err.Error()
		if strings.Contains(errStr, "message is not modified") {
			return
		}
		logger.Debug("handleDeltaMessage [respId=%d]: ошибка Edit: %v", respId, err, b.userId)
		return
	}
	ss.lastDisplayedText = displayText
	ss.lastEdit = time.Now()
}

// DisableOperatorMode отключает режим оператора и уведомляет AI-модель
func (u *User) DisableOperatorMode(userId uint32, dialogId uint64, silent ...bool) error {
	// Определяем значение silent (по умолчанию false)
	isSilent := false
	if len(silent) > 0 {
		isSilent = silent[0]
	}

	// 1. Выключаем режим оператора
	u.SetOperatorMode(dialogId, false)
	logger.Debug("Выключен режим оператора для диалога %d", dialogId, userId)

	// 2. Находим respId по dialogId
	respId, err := u.mod.GetRespIdByDialogID(dialogId)
	if err != nil {
		logger.Error("Не удалось найти respId для dialogId %d: %v", dialogId, err)
		return err // Если не нашли, то и сообщение отправить не сможем
	}

	// 3. Получаем экземпляр бота
	u.mu.RLock()
	tgBot, ok := u.bot[userId]
	u.mu.RUnlock()

	// Специальная обработка для тестовых ботов (когда isTestBot=true и bot=nil)
	if ok && tgBot != nil && tgBot.isTestBot && tgBot.bot == nil {
		logger.Debug("Пропускаем отправку сообщения для тестового бота (это нормально в тестах)", userId)
		// Для тестовых ботов пропускаем отправку сообщения, но продолжаем работу (уведомим AI-модель ниже)
	} else {
		// Обычная проверка для реальных ботов
		if !ok || tgBot == nil || tgBot.bot == nil {
			logger.Error("Бот не найден, не могу отправить сообщение", userId)
			return fmt.Errorf("бот не найден")
		}

		// 4. Отправляем сообщение пользователю только если не silent режим
		if !isSilent {
			recipient := &tele.User{ID: int64(respId)}
			//messageText := "Оператор отключился. Маруся AI снова с вами!"
			if _, err := tgBot.bot.Send(recipient, u.end.TranslateMessageWithUserID(userId, "operator.disconnected")); err != nil {
				logger.Error("Ошибка отправки сообщения о выключении оператора пользователю %d: %v", respId, err, userId)
			}
		}
	}

	// 5. AI-модель уже знает что оператор отключился и работа возобновлена, пользователь уведомлён на шаге 4

	// 5. Уведомляем AI-модель о возобновлении работы
	//usrCh, err := u.mod.GetCh(respId)
	//if err != nil {
	//	logger.Error("Каналы для respId %d не найдены при отключении оператора: %v", respId, err, userId)
	//	// Можно попытаться пересоздать сессию, если это необходимо
	//	return err
	//}
	//
	//systemName := "assist"
	//operatorOffMsg := u.mod.NewMessage(
	//	model.Operator{SetOperator: false, Operator: false},
	//	"assist",
	//	&model.AssistResponse{Message: u.end.TranslateMessageWithUserID(userId, "operator.mode.is.disabled")},
	//	&systemName,
	//)
	//
	//if err := u.trySendToRxCh(usrCh, operatorOffMsg); err != nil {
	//	logger.Error("Не удалось отправить системное сообщение в модель о выключении оператора: %v", err)
	//}

	// 6. Закрываем SSE соединение для оператора, если есть хотя это уже должно быть сделано в air_oper
	if u.op != nil {
		if err := u.op.CloseOperatorSSE(u.ctx, userId, dialogId); err != nil {
			// Логируем ошибку, но не прерываем процесс, так как это не критично
			logger.Debug("Не удалось закрыть SSE сессию оператора: %v", err, userId)
		}
	}

	return nil
}

// sendFileToUser отправляет файл пользователю в зависимости от его типа
func (b *Bot) sendFileToUser(recipient *tele.User, file model.File) error {
	var reader io.Reader
	var err error

	// Получаем Reader для файла через модель
	if file.URL != "" {
		reader, err = b.mod.GetFileAsReader(b.userId, file.URL)
		if err != nil {
			return fmt.Errorf("ошибка получения файла: %w", err)
		}
	} else {
		return fmt.Errorf("файл должен содержать URL или FileID")
	}

	// Создаем файл Telegram из Reader
	telegramFile := tele.File{
		FileReader: reader,
	}

	// Отправляем файл в зависимости от типа
	switch file.Type {
	case "photo":
		photo := &tele.Photo{
			File:    telegramFile,
			Caption: file.Caption,
		}
		_, err := b.bot.Send(recipient, photo)
		return err

	case "video":
		video := &tele.Video{
			File:    telegramFile,
			Caption: file.Caption,
		}
		_, err := b.bot.Send(recipient, video)
		return err

	case "audio":
		audio := &tele.Audio{
			File:    telegramFile,
			Caption: file.Caption,
		}
		_, err := b.bot.Send(recipient, audio)
		return err

	case "doc":
		document := &tele.Document{
			File:     telegramFile,
			FileName: file.FileName,
			Caption:  file.Caption,
		}
		_, err := b.bot.Send(recipient, document)
		return err

	default:
		// Если тип не указан явно или неизвестен, отправляем как документ
		document := &tele.Document{
			File:     telegramFile,
			FileName: file.FileName,
			Caption:  file.Caption,
		}
		_, err := b.bot.Send(recipient, document)
		return err
	}
}

func (b *Bot) startResponseListener(respId int64, respIdentifier string) {
	go func() {
		defer func() {
			if r := recover(); r != nil {
				logger.Error("Паника в обработчике ответов: %v", r, b.userId)
			}
		}()

		// Проверяем корректность инициализации бота
		if b.user == nil {
			logger.Error("b.user равен nil в startResponseListener для respId %d", respId, b.userId)
			return
		}

		// Получаем каналы для пользователя
		usrCh, err := b.mod.GetCh(uint64(respId))
		if err != nil {
			logger.Error("Ошибка при получении канала: %v", respId, err, b.userId)
			return
		}

		for {
			select {
			case <-b.ctx.Done():
				logger.Debug("Закрытие слушателя ответов по контексту для respId %d", respId, b.userId)
				return
			case msg, ok := <-usrCh.TxCh:
				if !ok {
					logger.Debug("Канал TxCh закрыт, завершаем слушатель", respId)
					return
				}

				// ОТЛАДКА СТРИМИНГА: логируем каждое сообщение из TxCh
				logger.Debug("TxCh [respId=%d] получено сообщение: type=%q, len=%d, operator=%v",
					respId, msg.Type, len(msg.Content.Message), msg.Operator.Operator, b.userId)

				// Проверяем, включен ли режим оператора для этого сообщения
				if b.user != nil && msg.Operator.Operator && msg.Operator.SetOperator && !b.user.IsOperatorMode(usrCh.DialogID) {
					// Включаем режим оператора для респондента
					b.user.SetOperatorMode(usrCh.DialogID, true)
					logger.Debug("Включен режим оператора для диалога %d", usrCh.DialogID, b.userId)
				}

				// Проверяем тип сообщения - нам нужны только ответы ассистента
				if msg.Type == "assistant_delta" {
					if b.deltaMode {
						// Дельта стриминга — обновляем placeholder через bot.Edit
						logger.Debug("TxCh [respId=%d] → delta: %q", respId, msg.Content.Message, b.userId)
						b.handleDeltaMessage(respId, msg.Content.Message)
					}
					continue // В любом случае пропускаем дельту
				}

				if msg.Type == "assist" {
					logger.Debug("TxCh [respId=%d] → финальный assist, len=%d", respId, len(msg.Content.Message), b.userId)
					// Обрабатываем ответ ассистента
					if err := b.processAssistantResponse(usrCh.DialogID, respId, msg); err != nil {
						logger.Error("Ошибка отправки сообщения пользователю %d: %v", respId, err, b.userId)
					} else {
						// Отправляем ответ ассистента в CRM
						if b.c != nil {
							csg := b.c.MSG("assist", usrCh.RespName, msg.Content.Message).
								WithAltContact(respIdentifier).
								WithFiles(func() []string {
									var f []string
									for _, file := range msg.Content.Action.SendFiles {
										f = append(f, file.FileName)
									}
									return f
								}()...).
								SetMeta(msg.Content.Meta)

							if err := b.c.SendMessage(csg); err != nil {
								logger.Error("Ошибка отправки ответа ассистента в CRM: %v", err, b.userId)
							}
						}
					}
				}
			}
		}
	}()
}

// formatSender форматирует информацию об отправителе
func formatSender(sender *tele.User) string {
	if sender.Username != "" {
		return sender.Username
	}

	name := sender.FirstName
	if sender.LastName != "" {
		name += " " + sender.LastName
	}

	return name
}

func (u *User) SendSubscriptionError(error *com.SubscriptionError) {
	// Выключаю все каналы пользователя
	if dbErr := u.db.DisableAllUserChannel(error.UserID); dbErr != nil {
		logger.Error("Ошибка при выключении каналов пользвателя %d: %v", error.UserID, dbErr)
	}

	// Создаю сообщение об окончании подписки
	msg := com.CarpCh{
		Event:      "subscription",
		UserName:   "",
		AssistName: "",
		Target:     "",
		UserID:     error.UserID,
	}
	// Отправляю уведомление об окончании подписки
	if u.end != nil {
		err := u.end.SendNotification(msg)
		if err != nil {
			logger.Error("Ошибка отправки уведомления об окончании подписки: %v", err)
		}
	}

	// Помечаю уведомление как отправленное
	if err := u.db.SetUserSubscriptionNotified(error.UserID); err != nil {
		logger.Error("Ошибка при пометке уведомления об окончании подписки как отправленного: %v", error.UserID, err)
	}
}

// extractFilesFromMessage извлекает файлы из сообщения Telegram и преобразует их в формат model.FileUpload для отправки в модель
func (b *Bot) extractFilesFromMessage(c tele.Context) ([]model.FileUpload, error) {
	var files []model.FileUpload
	message := c.Message()

	// Обработка документов
	if message.Document != nil {
		fileUpload, err := b.telegramFileToFileUpload(c, message.Document.File, message.Document.FileName, message.Document.MIME)
		if err != nil {
			return nil, err
		}
		files = append(files, fileUpload)
	}

	// Обработка фото
	if message.Photo != nil {
		fileUpload, err := b.telegramFileToFileUpload(c, message.Photo.File, "photo.jpg", "image/jpeg")
		if err != nil {
			return nil, err
		}
		files = append(files, fileUpload)
	}

	// Обработка видео
	if message.Video != nil {
		fileUpload, err := b.telegramFileToFileUpload(c, message.Video.File, message.Video.FileName, message.Video.MIME)
		if err != nil {
			return nil, err
		}
		files = append(files, fileUpload)
	}

	// Обработка аудио
	if message.Audio != nil {
		fileUpload, err := b.telegramFileToFileUpload(c, message.Audio.File, message.Audio.FileName, message.Audio.MIME)
		if err != nil {
			return nil, err
		}
		files = append(files, fileUpload)
	}

	// Обработка голосовых сообщений
	if message.Voice != nil {
		fileUpload, err := b.telegramFileToFileUpload(c, message.Voice.File, "voice.ogg", message.Voice.MIME)
		if err != nil {
			return nil, err
		}
		files = append(files, fileUpload)
	}

	// Обработка видеосообщений
	if message.VideoNote != nil {
		fileUpload, err := b.telegramFileToFileUpload(c, message.VideoNote.File, "videonote.mp4", "video/mp4")
		if err != nil {
			return nil, err
		}
		files = append(files, fileUpload)
	}

	// Обработка стикеров
	if message.Sticker != nil {
		fileUpload, err := b.telegramFileToFileUpload(c, message.Sticker.File, "sticker.webp", "image/webp")
		if err != nil {
			return nil, err
		}
		files = append(files, fileUpload)
	}

	return files, nil
}

func (b *Bot) telegramFileToFileUpload(c tele.Context, file tele.File, fileName, mimeType string) (model.FileUpload, error) {
	reader, err := c.Bot().File(&file)
	if err != nil {
		return model.FileUpload{}, fmt.Errorf("ошибка получения файла: %w", err)
	}

	return model.FileUpload{
		Name:     fileName,
		Content:  reader,
		MimeType: mimeType,
	}, nil
}

// GetBotUsername возвращает имя пользователя бота для отдачи в API
func (u *User) GetBotUsername(userID uint32) string {
	u.mu.RLock()
	bot, ok := u.bot[userID]
	u.mu.RUnlock()

	if !ok || bot == nil || bot.bot == nil {
		return ""
	}

	if bot.bot.Me == nil || bot.bot.Me.Username == "" {
		return ""
	}

	return bot.bot.Me.Username
}

// SetOperatorMode выставляет/снимает режим оператора для конкретного диалога
func (u *User) SetOperatorMode(dialogId uint64, operator bool) {
	if operator {
		u.operatorModeByDialog.Store(dialogId, true)
	} else {
		u.operatorModeByDialog.Delete(dialogId)
	}
	metrics.TrackOperatorModeDialogs(0, u.operatorModeDialogsCount())
}

func (u *User) operatorModeDialogsCount() int {
	count := 0
	u.operatorModeByDialog.Range(func(_, _ any) bool {
		count++
		return true
	})
	return count
}

// IsOperatorMode возвращает текущий флаг оператора для диалога
func (u *User) IsOperatorMode(dialogId uint64) bool {
	_, ok := u.operatorModeByDialog.Load(dialogId)
	return ok
}

// StopUserBot останавливает бота пользователя по userID
func (u *User) StopUserBot(userID uint32) error {
	metrics.ObserveBotLifecycle(userID, "stop", "attempt")
	u.mu.Lock()
	defer u.mu.Unlock()

	bot, exists := u.bot[userID]
	if !exists {
		metrics.ObserveBotLifecycle(userID, "stop", "not_found")
		return fmt.Errorf("бот для пользователя %d не найден", userID)
	}

	if bot != nil && bot.bot != nil {
		logger.Debug("Остановка бота %s", bot.bot.Me.Username, userID)
		bot.bot.Stop()

		// Удаляем из реестра вебхуков по токену
		for t, b := range u.botByToken {
			if b == bot {
				delete(u.botByToken, t)
				break
			}
		}
	}

	delete(u.bot, userID)
	metrics.SetActiveSessions(userID, "running", 0)
	metrics.SetActiveSessions(userID, "starting", 0)
	metrics.ObserveBotLifecycle(userID, "stop", "success")
	logger.Debug("Бот успешно остановлен и удален", userID)

	return nil
}

// RestartUserBot перезапускает бота пользователя с пересозданием модели ассистента
func (u *User) RestartUserBot(userID uint32) error {
	metrics.ObserveBotLifecycle(userID, "restart", "attempt")
	// 1. Проверяем существование бота
	u.mu.RLock()
	bot, exists := u.bot[userID]
	u.mu.RUnlock()

	if !exists || bot == nil {
		metrics.ObserveBotLifecycle(userID, "restart", "not_found")
		return fmt.Errorf("бот для пользователя %d не найден", userID)
	}

	// 2. Останавливаем текущего бота
	if bot.bot != nil {
		logger.Debug("Остановка бота %s для пересоздания", bot.bot.Me.Username, userID)
		bot.bot.Stop()
	}

	// 3. Отменяем контекст бота
	if bot.cancel != nil {
		bot.cancel()
	}

	// 4. Удаляем бота из карты
	u.mu.Lock()
	delete(u.bot, userID)
	u.mu.Unlock()

	// 5. Запускаем бота заново используя оптимизированный метод StartUserBot
	if err := u.StartUserBot(userID); err != nil {
		metrics.ObserveBotLifecycle(userID, "restart", "failed")
		logger.Error("Ошибка запуска бота при перезапуске: %v", userID, err)
		return fmt.Errorf("ошибка запуска бота при перезапуске: %w", err)
	}

	metrics.ObserveBotLifecycle(userID, "restart", "success")
	logger.Debug("Бот для успешно перезапущен", userID)
	return nil
}

// ProcessWebhookUpdate обрабатывает входящее обновление вебхука для бота с указанным токеном
func (u *User) ProcessWebhookUpdate(token string, body []byte) error {
	u.mu.RLock()
	bot, ok := u.botByToken[token]
	u.mu.RUnlock()

	if !ok || bot == nil || bot.bot == nil {
		return fmt.Errorf("бот с токеном %s не найден", token)
	}

	logger.Debug("Processing Webhook update for bot %s", bot.bot.Me.Username)

	// Разбираем JSON обновления
	var update tele.Update
	if err := json.Unmarshal(body, &update); err != nil {
		return fmt.Errorf("ошибка парсинга обновления: %w", err)
	}

	bot.bot.ProcessUpdate(update)
	return nil
}

// SetWebhookUserBot регистрирует вебхук для бота пользователя в Telegram
func (u *User) SetWebhookUserBot(userID uint32) error {
	// Получаем бота
	u.mu.RLock()
	bot, ok := u.bot[userID]
	u.mu.RUnlock()

	if !ok || bot == nil || bot.bot == nil {
		return fmt.Errorf("бот для пользователя %d не запущен", userID)
	}

	// Получаем токен
	// Поскольку bot.token это *string, разыменовываем аккуратно
	token := ""
	u.mu.RLock()
	for t, b := range u.botByToken {
		if b == bot {
			token = t
			break
		}
	}
	u.mu.RUnlock()

	if token == "" {
		// Попробуем распарсить из конфига БД если в мапе нет
		userDetails, err := u.db.GetTgBotUser(u.ctx, userID)
		if err != nil || userDetails == nil {
			return fmt.Errorf("не удалось получить данные бота из БД")
		}
		t, _, wh, err := u.parseTokenFromJSON(u.ctx, userDetails.TgBot, userID)
		if err != nil || !wh {
			return fmt.Errorf("бот не в режиме Webhook или ошибка токена")
		}
		token = t
	}

	webhookURL := fmt.Sprintf("%s/open/tgbot/webhook/%s", mode.GetRealHost(), token)

	err := bot.bot.SetWebhook(&tele.Webhook{
		Endpoint: &tele.WebhookEndpoint{PublicURL: webhookURL},
	})

	if err != nil {
		return fmt.Errorf("ошибка установки вебхука в Telegram: %w", err)
	}

	logger.Info("Вебхук успешно установлен: %s", webhookURL, userID)
	return nil
}
