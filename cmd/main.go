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

	"github.com/ikermy/air-common/pkg/com"
	"github.com/ikermy/air-common/pkg/mode"
	"github.com/ikermy/air-logger/v2/pkg/logger"
)

func main() {
	logger.Debug(com.GetVersionInfo())

	// Инициализируем инфраструктурные переменные из env vars (порты, домен, TTL, логи).
	// Все значения имеют разумные дефолты; некорректные критичные — fatal.
	mode.InitFromEnv(logger.Fatalf)

	// Для webhook используем публичный адрес из REAL_URL.
	// mode.InitFromEnv уже загружает его, но читаем переменную явно, чтобы
	// источник адреса был очевиден в production-конфигурации.
	realHost := os.Getenv("REAL_URL")
	if realHost == "" {
		realHost = mode.GetRealHost()
	}
	realHost = strings.TrimPrefix(strings.TrimPrefix(realHost, "https://"), "http://")
	mode.SetRealHost("https://" + realHost)
	logger.Infoln("USE REAL_URL:", mode.GetRealHost())

	mode.SetTextMode(true)
	mode.SetAudioMode(true)

	// Логгер: режим os.Stdout для Docker
	logSetup := logger.StdOut()
	logSetup.WithLogLevel(logSetup.FromString(mode.GetLogLevel()))
	logSetup.Apply()

	// ── Redis ───────────────────────────────────────────────────────────────────
	redisCfg := domain.Redis{
		RedisAddr:     os.Getenv("REDIS_ADDR"),
		RedisPassword: os.Getenv("REDIS_PASSWORD"),
		RedisDB:       0,
	}
	dbStr := os.Getenv("REDIS_DB")
	if dbStr != "" {
		if rdb, err := strconv.Atoi(dbStr); err == nil {
			redisCfg.RedisDB = rdb
		}
	}
	if redisCfg.RedisAddr != "" {
		logger.Info("Redis: адрес=%s, db=%d", redisCfg.RedisAddr, redisCfg.RedisDB)
	} else {
		logger.Info("Redis: не настроен (REDIS_ADDR пуст)")
	}

	// Корневой контекст процесса, отменяется по сигналам ОС
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM, syscall.SIGINT)
	defer stop()

	a := app.New(ctx, redisCfg)
	a.Run()

	// Ожидание завершения работы
	<-a.ExitCh()

	logger.Infoln("Приложение air_tgbot завершено")
}
