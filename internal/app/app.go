package app

import (
	"air_tgbot/internal/db"
	"air_tgbot/internal/domain"
	"air_tgbot/internal/telegram"
	"context"
	"time"

	"github.com/ikermy/air_common/pkg/com"
	"github.com/ikermy/air_common/pkg/crm"
	"github.com/ikermy/air_common/pkg/endpoint"
	"github.com/ikermy/air_common/pkg/model"
	"github.com/ikermy/air_common/pkg/model/google"
	"github.com/ikermy/air_common/pkg/model/mistral"
	"github.com/ikermy/air_common/pkg/model/openai"
	"github.com/ikermy/air_common/pkg/operator"
	"github.com/ikermy/air_common/pkg/rpc"
	"github.com/ikermy/air_common/pkg/startpoint"
	"github.com/ikermy/air_logger/v2/pkg/logger"
	"github.com/redis/go-redis/v9"
)

type Mod interface {
	CleanUp()
	Shutdown(shutCh chan<- com.LogMsg)
}

type Start interface {
	StarterListener(start model.StartCh, errCh chan<- error)
	Shutdown(shutCh chan<- com.LogMsg)
}

type Carp interface {
	Run()
}

type End interface {
	Shutdown(shutCh chan<- com.LogMsg)
	NotificationListener(notifCh chan<- com.LogMsg)
}

type Telega interface {
	StartBots() error
	StopBot()
}

type CRM interface {
	Shutdown(shutCh chan<- com.LogMsg)
}

type DB interface {
	HandlerClose()
}

type App struct {
	ctx    context.Context
	cancel context.CancelFunc
	Start  Start
	Mod    Mod
	Carp   Carp
	End    End
	CRM    CRM
	DB     DB
	Telega Telega
}

func New(parent context.Context) *App {
	// Локальный дочерний контекст для уровня app
	ctx, cancel := context.WithCancel(parent)

	d, err := db.New(ctx)
	if err != nil {
		logger.Fatal("Ошибка инициализации базы данных: %v", err)
	}

	rpcClient, err := rpc.New()
	if err != nil {
		logger.Fatalf("ошибка создания rpc клиента: %v", err)
	}

	m := model.NewModelRouter(ctx, d,
		model.WithMasterKeyProvider(rpcClient), // первым!
		openai.NewAsRouterOption(),
		mistral.NewAsRouterOption(),
		google.NewAsRouterOption(),
	)

	// Инжектируем resolver в comdb.DB
	//    Каждый раз когда DB-методу нужен MasterKey — он делает gRPC-запрос к AiR_ORCHESTRATOR
	d.SetMasterKeyResolver(func(userId uint32) ([32]byte, bool) {
		mk, err := rpcClient.GetUserMasterKey(context.Background(), userId)
		if err != nil {
			// codes.Unavailable — пользователь не логинился после рестарта AiR_ORCHESTRATOR
			// codes.Unauthenticated / PermissionDenied — неверный SERVICE_KEY
			return [32]byte{}, false
		}
		return mk, true
	})

	var redisClient redis.UniversalClient
	if domain.RedisAddr != "" {
		redisClient = redis.NewClient(&redis.Options{
			Addr:     domain.RedisAddr,
			Password: domain.RedisPassword,
			DB:       domain.RedisDB,
		})

		if err := redisClient.Ping(ctx).Err(); err != nil {
			logger.Warn("Redis: недоступен, firstInteraction будет работать без восстановления после рестарта: %v", err)
			_ = redisClient.Close()
			redisClient = nil
		} else {
			logger.Info("Redis: клиент firstInteraction инициализирован")
		}
	}

	e := endpoint.New(ctx, d)
	cr := crm.New(ctx, crm.WithAltContactChannel(crm.ChannelTelegram))
	t := telegram.New(ctx, d, m, e, cr, rpcClient, redisClient)
	o := operator.New(ctx)

	// Получаем конфигурацию Telegram-бота через универсальный orc клиент
	botConfig, err := rpcClient.GetBotConfig(ctx)
	if err != nil {
		logger.Error("Ошибка получения конфигурации бота через gRPC: %v, используем пустые значения", err)
		botConfig = nil
	}
	// Создаём Carpintero бота с конфигурацией из gRPC если она получена
	c := telegram.NewCarpintero(ctx, botConfig, d, t)

	s := startpoint.New(ctx, m, e, t, o)

	t.SetDeltaProcessor(s) // Внедряем ProcessStreamDelta из startpoint.Start
	t.SetOperator(o)
	return &App{
		ctx:    ctx,
		cancel: cancel,

		Start:  s,
		Mod:    m,
		Carp:   c,
		End:    e,
		CRM:    cr,
		DB:     d,
		Telega: t,
	}
}

func (a *App) Run() {
	readyCh := make(chan string)
	// Сначала запускаю бота Carpintero
	carpintero := a.Carp.(*telegram.Carpintero)
	carpintero.SetReadyChannel(readyCh)

	// Создаю шину для логирования сообщений от модулей
	bus := com.NewBus(10)

	// Слушаем StartCh
	go a.Starter()

	// Запускаю очистку устаревших пользовательских моделей
	go a.Mod.CleanUp()

	// Запускаю бота Carpintero
	go a.Carp.Run()

	// читатель
	go uReader(bus.MsgCh)
	// Запускаю обработчик закрытия БД
	go a.DB.HandlerClose()
	// Запускаю слушателя уведомлений
	bus.Add(func(ch chan<- com.LogMsg) { a.End.NotificationListener(ch) })

	logger.Infoln(<-readyCh)
	close(readyCh) // Закрываю канал после получения сигнала готовности

	// Запускаю ботов Telega
	logger.Info("Запускаю пользовательских ботов...")

	go func() {
		err := a.Telega.StartBots()
		if err != nil {
			logger.Fatal(err)
		}
	}()

	// Обработка сигнала завершения
	go func() {
		<-a.ctx.Done()
		// Аварийный тайм-аут на случай, если что-то пойдет не так с завершением
		go func() {
			ticker := time.NewTicker(5 * time.Second)
			<-ticker.C
			close(domain.UsersDB)
		}()

		logger.Info("App: получен сигнал завершения, начинаю shutdown")

		// Останавливаем ботов Telegram чтобы не принимать новые запросы
		a.Telega.StopBot()

		bus.Add(func(ch chan<- com.LogMsg) { a.Start.Shutdown(ch) })
		bus.Add(func(ch chan<- com.LogMsg) { a.CRM.Shutdown(ch) })
		bus.Add(func(ch chan<- com.LogMsg) { a.Mod.Shutdown(ch) })
		bus.Add(func(ch chan<- com.LogMsg) { a.End.Shutdown(ch) })

		logger.Info("App: все модули завершены, отправляю сигнал завершения БД")
		// ждём всех producers и закрываем канал
		bus.WaitAndClose()
		// Отправляем сигнал о завершении работы с БД
		close(domain.UsersDB)
	}()
}

func (a *App) Starter() {
	// Создаем канал для ошибок
	errCh := make(chan error, 10)

	// Обработчик ошибок в отдельной горутине
	go func() {
		for err := range errCh {
			if err != nil {
				logger.Error("Ошибка в StarterListener: %v", err)
			}
		}

		close(errCh)
	}()

	// Простой цикл чтения из канала
	for start := range telegram.StartCh {
		// Запускаю слушателя с пользовательскими данными
		go func(startData model.StartCh) {
			a.Start.StarterListener(startData, errCh)
		}(start)
	}

	logger.Infoln("StartCh closed") // Невозможное сообщение, так как канал не закрывается
}

func uReader(readCh <-chan com.LogMsg) {
	for info := range readCh {
		switch info.Log {
		case 0: // Info
			logger.Info("%s: %v", info.Mod, info.Msg, info.UID)
		case 1: // Info
			logger.Error("%s: %v", info.Mod, info.Msg, info.UID)
		case 2: // Info
			logger.Warn("%s: %v", info.Mod, info.Msg, info.UID)
		case 3: // Info
			logger.Debug("%s: %v", info.Mod, info.Msg, info.UID)
		}
	}
}
