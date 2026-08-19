package main

import (
	"air_tgbot/internal/app"
	"air_tgbot/internal/domain"
	"context"
	"os"
	"os/signal"
	"strconv"
	"strings"
	"syscall"

	"github.com/ikermy/air_common/pkg/com"
	"github.com/ikermy/air_common/pkg/mode"
	"github.com/ikermy/air_logger/v2/pkg/logger"
)

func main() {
	logger.Debug(com.GetVersionInfo())

	// Инициализируем инфраструктурные переменные из env vars (порты, домен, TTL, логи).
	// Все значения имеют разумные дефолты; некорректные критичные — fatal.
	mode.InitFromEnv(logger.Fatalf)

	// mode.RealHost всегда должен быть в https режиме для корректной работы Webhook
	mode.SetRealHost("https://" + strings.TrimPrefix(strings.TrimPrefix(mode.GetRealHost(), "https://"), "http://"))
	logger.Infoln("USE REAL_URL:", mode.GetRealHost())

	mode.SetTextMode(true)
	mode.SetAudioMode(true)

	// Логгер: режим os.Stdout для Docker
	logSetup := logger.StdOut()
	logSetup.WithLogLevel(logSetup.FromString(mode.GetLogLevel()))
	logSetup.Apply()

	// ── Redis ───────────────────────────────────────────────────────────────────
	domain.RedisAddr = os.Getenv("REDIS_ADDR")
	domain.RedisPassword = os.Getenv("REDIS_PASSWORD")
	dbStr := os.Getenv("REDIS_DB")
	if dbStr != "" {
		if n, err := strconv.Atoi(dbStr); err == nil {
			domain.RedisDB = n
		}
	}
	if domain.RedisAddr != "" {
		logger.Info("Redis: адрес=%s, db=%d", domain.RedisAddr, domain.RedisDB)
	} else {
		logger.Info("Redis: не настроен (REDIS_ADDR пуст)")
	}

	// Корневой контекст процесса, отменяется по сигналам ОС
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM, syscall.SIGINT)
	defer stop()

	a := app.New(ctx)
	a.Run()

	// Ожидание завершения работы
	<-domain.Exit

	logger.Infoln("Приложение air_tgbot завершено")
}
