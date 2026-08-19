package telegram

import (
	deliveryhttp "air_tgbot/internal/delivery/http"
	"air_tgbot/internal/domain"
	"air_tgbot/internal/metrics"
	"context"
	"fmt"
	"os"
	"regexp"
	"strconv"
	"strings"
	"time"

	"github.com/ikermy/air_common/pkg/mode"
	"github.com/ikermy/air_common/pkg/rpc/proto"
	"github.com/ikermy/air_logger/v2/pkg/logger"
	tele "gopkg.in/telebot.v4"
)

type Telega = Inter

type Carpintero struct {
	ctx      context.Context
	cancel   context.CancelFunc
	b        *tele.Bot
	token    string
	botName  string
	botID    string
	botReady chan string    // Канал для уведомления о готовности бота
	re429    *regexp.Regexp // Ошибка 429 ТГ лимит отправки сообщений
	db       DB
	t        Telega
}

func NewCarpintero(parent context.Context, botConfig *proto.BotConfigResponse, d DB, t Telega) *Carpintero {
	ctx, cancel := context.WithCancel(parent)

	// Если конфиг не получен, используем пустые значения
	token := ""
	botName := ""
	botID := ""
	if botConfig != nil {
		token = botConfig.Token
		botName = botConfig.BotName
		if idx := strings.Index(token, ":"); idx != -1 {
			botID = token[:idx]
		}
	}

	return &Carpintero{
		ctx:      ctx,
		cancel:   cancel,
		botReady: make(chan string),
		b:        nil,
		token:    token,
		botName:  botName,
		botID:    botID,
		re429:    regexp.MustCompile(`retry after \d+ \(429\)`),
		db:       d,
		t:        t,
	}
}

// SetReadyChannel устанавливает канал для уведомления о готовности бота
func (c *Carpintero) SetReadyChannel(ch chan string) {
	c.botReady = ch
}

func (c *Carpintero) Run() {
	// Запускаю веб-сервер для получения уведомлений
	go deliveryhttp.Run(deliveryhttp.Handlers{
		Available: c.AvailableHandler, GetName: c.GetBotName, Notification: c.Notification,
		Verification: c.Verification, AdminNotification: c.SendAdminNotification,
		Enable: c.startBot, Disable: c.stopBot, Restart: c.reloadBot,
		SetWebhook: c.SetWebhook, Webhook: c.WebhookUpdate,
	})

	// Проверяю что токен бота задан
	if c.token == "" {
		c.botReady <- "Токен бота не задан в конфигурации, Carpintero не запущен"
		return
	}

	// Для production включаю в режиме webhook
	webhookEnabled, err := strconv.ParseBool(os.Getenv("WEBHOOK"))
	if err != nil {
		webhookEnabled = false
	}

	poller := &tele.LongPoller{Timeout: 30 * time.Second}

	bot, err := tele.NewBot(tele.Settings{
		Token:  c.token,
		Poller: poller,
	})
	if err != nil {
		logger.Fatalf("Ошибка запуска Carpintero %v", err)
	}

	if webhookEnabled {
		if err := configureWebhook(bot, c.token); err != nil {
			logger.Fatalf("ошибка установки webhook: %w", err)
		}
		bot.Poller = &tele.Webhook{}
	} else {
		if err := bot.RemoveWebhook(true); err != nil {
			logger.Fatalf("ошибка удаления webhook: %v", err)
		}
	}

	c.b = bot
	// Добавляем обработчик для команды /start сразу шлём ID пользователя
	c.b.Handle("/start", func(ctx tele.Context) error {
		_, err = c.SendUserID(c.ctx, ctx.Message())
		return err
	})

	logger.Debug("Имя бота: %s, ID бота: %s", c.botName, c.botID)

	c.botReady <- fmt.Sprintf("Бот %s для уведомлений запущен!", c.b.Me.Username)

	// Запускаю слушателя сообщений для отправки уведомлений
	go c.Listener()

	c.b.Start()
}

func configureWebhook(bot *tele.Bot, token string) error {
	webhookURL := fmt.Sprintf("%s/open/tgbot/webhook/%s", mode.GetRealHost(), token)
	return bot.SetWebhook(&tele.Webhook{
		Endpoint: &tele.WebhookEndpoint{PublicURL: webhookURL},
	})
}

func (c *Carpintero) Listener() {
	for {
		select {
		case <-c.ctx.Done():
			logger.Info("Carpintero listener stopped due to context cancellation")
			return
		case msg, ok := <-domain.CarpinteroCh:
			if !ok {
				logger.Error("CarpinteroCh closed")
				return
			}
			go c.SendNotification(c.ctx, msg.TelegaID, msg.Message)
		}
	}
}

func (c *Carpintero) SendNotification(ctx context.Context, tgUserId int64, message string) {
	select {
	case <-ctx.Done():
		logger.Info("SendNotification cancelled due to context")
		return
	default:
	}

	_, err := c.SendMsg(ctx, tgUserId, message)
	if err != nil {
		logger.Error("Ошибка отправки сообщения в Telegram: %v", tgUserId, err)
		return
	}
}

func (c *Carpintero) SendMsg(ctx context.Context, recipient int64, messageBody string) (int, error) {
	startedAt := time.Now()
	if recipient == 0 {
		metrics.ObserveTgBotSend(0, "invalid_recipient")
		metrics.ObserveTgBotSendDuration(0, startedAt)
		logger.Error("Recipient ID is zero, message not sent")
		return -1, nil
	}

	for attempts := 1; attempts <= 3; attempts++ { // Повторяем попытку отправки сообщения 3 раза
		select {
		case <-ctx.Done():
			metrics.ObserveTgBotSend(0, "context_cancelled")
			metrics.ObserveTgBotSendDuration(0, startedAt)
			logger.Info("SendMsg cancelled due to context after %d attempts", attempts-1)
			return -1, ctx.Err()
		default:
		}

		msg, err := c.b.Send(tele.ChatID(recipient),
			messageBody,
			&tele.SendOptions{
				ParseMode:           tele.ModeHTML,
				DisableNotification: true,
			})

		if err != nil {
			if attempts < 3 && c.re429.MatchString(err.Error()) {
				logger.Info("Failed to send message to Telegram: %s, retrying in %d seconds...", err, 2*attempts)

				// Используем контекст для таймаута ожидания
				sleepCtx, sleepCancel := context.WithTimeout(ctx, time.Duration(2*attempts)*time.Second)
				select {
				case <-sleepCtx.Done():
					sleepCancel()
					if ctx.Err() != nil {
						return -1, ctx.Err()
					}
				case <-time.After(time.Duration(2*attempts) * time.Second):
				}
				sleepCancel()
				continue
			}
			logger.Warn("Failed to send message to Telegram after %d attempts: %s", attempts, err)
			logger.Warn("Message: %s", messageBody)
			logger.Warn("Recipient: %d", recipient)
		} else {
			metrics.ObserveTgBotSend(0, "success")
			metrics.ObserveTgBotSendDuration(0, startedAt)
			return msg.ID, nil
		}

		// Используем контекст для таймаута ожидания между попытками
		sleepCtx, sleepCancel := context.WithTimeout(ctx, time.Duration(1*attempts)*time.Second)
		select {
		case <-sleepCtx.Done():
			sleepCancel()
			if ctx.Err() != nil {
				return -1, ctx.Err()
			}
		case <-time.After(time.Duration(1*attempts) * time.Second):
		}
		sleepCancel()
	}
	metrics.ObserveTgBotSend(0, "failed")
	metrics.ObserveTgBotSendDuration(0, startedAt)
	return -1, nil
}

func (c *Carpintero) SendUserID(ctx context.Context, message *tele.Message) (int, error) {
	if message == nil || message.Sender == nil {
		return -1, nil
	}

	select {
	case <-ctx.Done():
		logger.Info("SendUserID cancelled due to context")
		return -1, ctx.Err()
	default:
	}

	userId := message.Sender.ID
	messageBody := "Telegram ID: " + strconv.FormatInt(userId, 10)

	return c.SendMsg(ctx, userId, messageBody)
}

// Stop gracefully stops the Carpintero bot
func (c *Carpintero) Stop() {
	logger.Info("Stopping Carpintero...")
	c.cancel()
	if c.b != nil {
		c.b.Stop()
	}
}
