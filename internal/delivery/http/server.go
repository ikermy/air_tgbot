package http

import (
	metrics2 "air_tgbot/internal/metrics"
	stdhttp "net/http"

	"github.com/ikermy/air_logger/v2/pkg/logger"
)

// Handlers contains the HTTP endpoints exposed by the notification server.
type Handlers struct {
	Available, GetName, Notification, Verification, AdminNotification stdhttp.HandlerFunc
	Enable, Disable, Restart                                          stdhttp.HandlerFunc
	SetWebhook, Webhook                                               stdhttp.HandlerFunc
}

// Run starts the HTTP server for Telegram notifications.
func Run(h Handlers) {
	logger.Info("Запуск сервера уведомлений Carpintero")
	mux := stdhttp.NewServeMux()
	mux.Handle("/metrics", metrics2.Handler())

	mux.Handle("/tgbot/available", metrics2.HTTPMiddleware("/available", enableCORS(h.Available)))
	mux.Handle("/tgbot/getname", metrics2.HTTPMiddleware("/getname", enableCORS(h.GetName)))
	mux.Handle("/tgbot/notification", metrics2.HTTPMiddleware("/notification", enableCORS(h.Notification)))
	mux.Handle("/tgbot/verification", metrics2.HTTPMiddleware("/verification", enableCORS(h.Verification)))
	mux.Handle("/tgbot/adnot", metrics2.HTTPMiddleware("/adnot", enableCORS(h.AdminNotification)))
	mux.Handle("/tgbot/enable", metrics2.HTTPMiddleware("/enable", enableCORS(h.Enable)))
	mux.Handle("/tgbot/disable", metrics2.HTTPMiddleware("/disable", enableCORS(h.Disable)))
	mux.Handle("/tgbot/restart", metrics2.HTTPMiddleware("/restart", enableCORS(h.Restart)))
	mux.Handle("/open/tgbot/setwebhook", metrics2.HTTPMiddleware("/setwebhook", enableCORS(h.SetWebhook)))
	mux.Handle("POST /open/tgbot/webhook/{token}", metrics2.HTTPMiddleware("/webhook", h.Webhook))

	if err := stdhttp.ListenAndServe(":8080", mux); err != nil {
		logger.Fatalf("ошибка запуска сервера уведомлений: %v", err)
	}
}

func enableCORS(next stdhttp.HandlerFunc) stdhttp.HandlerFunc {
	return func(w stdhttp.ResponseWriter, r *stdhttp.Request) {
		if origin := r.Header.Get("Origin"); origin != "" {
			w.Header().Set("Access-Control-Allow-Origin", origin)
		}
		w.Header().Set("Access-Control-Allow-Credentials", "true")
		w.Header().Set("Access-Control-Allow-Methods", "POST, GET, OPTIONS")
		w.Header().Set("Access-Control-Allow-Headers", "Content-Type, Authorization")
		if r.Method == "OPTIONS" {
			w.WriteHeader(stdhttp.StatusNoContent)
			return
		}
		next(w, r)
	}
}
