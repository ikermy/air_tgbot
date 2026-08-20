package telegram

import (
	"air_tgbot/internal/domain"
	"encoding/json"
	"io"
	"net/http"
	"strconv"
	"time"

	"github.com/ikermy/air_common/pkg/endpoint"
	"github.com/ikermy/air_logger/v2/pkg/logger"
)

func (c *Carpintero) AvailableHandler(w http.ResponseWriter, _ *http.Request) {
	w.WriteHeader(http.StatusOK)
}

func (c *Carpintero) GetBotName(w http.ResponseWriter, r *http.Request) {
	if r.Method == http.MethodOptions {
		w.WriteHeader(http.StatusOK)
		return
	}

	if r.Method != http.MethodGet {
		http.Error(w, "Метод не разрешен", http.StatusMethodNotAllowed)
		return
	}

	userId, ok := readUserId(w, r)
	if !ok {
		return
	}

	botName := c.t.GetBotUsername(userId)
	response := map[string]string{"bot_name": botName}
	w.Header().Set("Content-Type", "application/json")
	if err := json.NewEncoder(w).Encode(response); err != nil {
		logger.Error("'GetBotName' Ошибка при кодировании JSON: %v", err)
		w.WriteHeader(http.StatusInternalServerError)
		return
	}
}

func (c *Carpintero) Verification(w http.ResponseWriter, r *http.Request) {
	if r.Method == http.MethodOptions {
		w.WriteHeader(http.StatusOK)
		return
	}

	if r.Method != http.MethodPost {
		http.Error(w, "Метод не разрешен", http.StatusMethodNotAllowed)
		return
	}

	var requestData struct {
		TelegaId uint64 `json:"id"`
		Message  string `json:"msg"`
	}

	decoder := json.NewDecoder(r.Body)
	if err := decoder.Decode(&requestData); err != nil {
		logger.Error("'Verification' Ошибка парсинга JSON: %v", err)
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusBadRequest)
		_ = json.NewEncoder(w).Encode(map[string]string{"error": err.Error()})
		return
	}

	msg, err := c.SendMsg(c.ctx, int64(requestData.TelegaId), requestData.Message)
	if err != nil {
		// Используем корректный формат для ошибок и ставим статус
		logger.Error("Ошибка отправки верификационного сообщения для %d: %v", requestData.TelegaId, err)
		w.WriteHeader(http.StatusInternalServerError)
		return
	}

	logger.Debug("Верификационное сообщение отправлено получателю: %d, ID сообщения: %d", requestData.TelegaId, msg)
}

// Notification обрабатывает входящие HTTP POST запросы для отправки уведомлений
func (c *Carpintero) Notification(w http.ResponseWriter, r *http.Request) {
	if r.Method == http.MethodOptions {
		w.WriteHeader(http.StatusOK)
		return
	}

	if r.Method != http.MethodPost {
		http.Error(w, "Метод не разрешен", http.StatusMethodNotAllowed)
		return
	}

	var requestData struct {
		Lang       string `json:"lang"`
		TelegramId int64  `json:"tid"`
		Event      string `json:"event"`
		UserName   string `json:"user"`
		AssistName string `json:"assist"`
		Target     string `json:"target"`
	}

	decoder := json.NewDecoder(r.Body)
	if err := decoder.Decode(&requestData); err != nil {
		logger.Error("'Notification' Ошибка парсинга JSON: %v", err)
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusBadRequest)
		_ = json.NewEncoder(w).Encode(map[string]string{"error": err.Error()})
		return
	}

	if requestData.TelegramId == 0 {
		// х.з., почему так происходит в первый раз...
		return
	}

	msg, err := endpoint.CreateMessageFromEvent(requestData.Lang, requestData.Event, requestData.UserName, requestData.AssistName, requestData.Target)
	if err != nil {
		logger.Error("'Notification' Ошибка при создании сообщения: %v", err)
		w.WriteHeader(http.StatusInternalServerError)
		_ = json.NewEncoder(w).Encode(map[string]string{"error": "Failed to create message"})
		return
	}

	message := domain.CarpCh{
		TelegaID: requestData.TelegramId,
		Message:  msg,
	}

	select {
	case domain.CarpinteroCh <- message:
		w.WriteHeader(http.StatusOK)
		err := json.NewEncoder(w).Encode(map[string]any{})
		if err != nil {
			return
		}
	default:
		logger.Warn("CarpinteroCh переполнен, сообщение не отправлено")
		w.WriteHeader(http.StatusServiceUnavailable)
	}
}

// SendAdminNotification обрабатывает уведомлений для администратора
func (c *Carpintero) SendAdminNotification(w http.ResponseWriter, r *http.Request) {
	if r.Method == http.MethodOptions {
		w.WriteHeader(http.StatusOK)
		return
	}

	if r.Method != http.MethodPost {
		http.Error(w, "Метод не разрешен", http.StatusMethodNotAllowed)
		return
	}

	var requestData struct {
		TelegramId int64  `json:"tid"`
		Message    string `json:"msg"`
	}

	decoder := json.NewDecoder(r.Body)
	if err := decoder.Decode(&requestData); err != nil {
		logger.Error("'SendAdminNotification' Ошибка парсинга JSON: %v", err)
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusBadRequest)
		_ = json.NewEncoder(w).Encode(map[string]string{"error": err.Error()})
		return
	}

	message := domain.CarpCh{
		TelegaID: requestData.TelegramId,
		Message:  requestData.Message,
	}

	select {
	case domain.CarpinteroCh <- message:
		w.WriteHeader(http.StatusOK)
		err := json.NewEncoder(w).Encode(map[string]any{})
		if err != nil {
			return
		}
	default:
		logger.Warn("CarpinteroCh переполнен, сообщение не отправлено")
		w.WriteHeader(http.StatusServiceUnavailable)
	}
}

func (c *Carpintero) startBot(w http.ResponseWriter, r *http.Request) {
	if r.Method == http.MethodOptions {
		w.WriteHeader(http.StatusOK)
		return
	}

	if r.Method != http.MethodGet {
		http.Error(w, "Метод не разрешен", http.StatusMethodNotAllowed)
		return
	}

	userId, ok := readUserId(w, r)
	if !ok {
		return
	}

	err := c.t.StartUserBot(userId)
	if err != nil {
		logger.Error("'startBot' Ошибка запуска бота для uid %d: %v", userId, err)
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusInternalServerError)
		_ = json.NewEncoder(w).Encode(map[string]string{"error": "failed to stop bot"})
		return
	}

	// Успешный ответ после запуска бота
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	_ = json.NewEncoder(w).Encode(map[string]string{"status": "bot stopped successfully"})
}

func (c *Carpintero) stopBot(w http.ResponseWriter, r *http.Request) {
	if r.Method == http.MethodOptions {
		w.WriteHeader(http.StatusOK)
		return
	}

	if r.Method != http.MethodGet {
		http.Error(w, "Метод не разрешен", http.StatusMethodNotAllowed)
		return
	}

	userId, ok := readUserId(w, r)
	if !ok {
		return
	}

	err := c.t.StopUserBot(userId)
	if err != nil {
		logger.Error("'stopBot' Ошибка остановки бота для uid %d: %v", userId, err)
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusInternalServerError)
		_ = json.NewEncoder(w).Encode(map[string]string{"error": "failed to stop bot"})
		return
	}

	// Успешный ответ после остановки бота
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	_ = json.NewEncoder(w).Encode(map[string]string{"status": "bot stopped successfully"})
}

func (c *Carpintero) reloadBot(w http.ResponseWriter, r *http.Request) {
	uid := r.URL.Query().Get("uid")

	if uid == "" {
		http.Error(w, "id parameter is required", http.StatusBadRequest)
		return
	}

	// Проверка, что uid является числом
	uidUint, err := strconv.ParseUint(uid, 10, 32)
	if err != nil {
		http.Error(w, "id parameter must be a valid number", http.StatusBadRequest)
		return
	}

	userId := uint32(uidUint)

	// канал для результата
	resultCh := make(chan error, 1)

	// запускаем в горутине
	go func() {
		resultCh <- c.t.RestartUserBot(userId)
	}()

	select {
	case err = <-resultCh:
		if err != nil {
			logger.Error("Ошибка перезагрузки бота: %v", err, userId)
			http.Error(w, "Failed to reload bot", http.StatusInternalServerError)
			return
		}
		// успех
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusOK)
		_ = json.NewEncoder(w).Encode(map[string]string{"status": "bot reloaded successfully"})
	case <-time.After(5 * time.Second):
		// если за 5 секунд не пришёл результат — считаем успехом
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusOK)
		_ = json.NewEncoder(w).Encode(map[string]string{"status": "bot reload in progress"})
	}
}

func (c *Carpintero) WebhookUpdate(w http.ResponseWriter, r *http.Request) {
	token := r.PathValue("token")
	suffix := token
	if len(token) > 8 {
		suffix = "..." + token[len(token)-8:]
	}
	logger.Debug("WebhookUpdate: метод=%s токен=%s", r.Method, suffix)

	if token == "" {
		http.Error(w, "token is required", http.StatusBadRequest)
		return
	}

	body, err := io.ReadAll(r.Body)
	if err != nil {
		logger.Error("Ошибка чтения тела Webhook: %v", err)
		w.WriteHeader(http.StatusInternalServerError)
		return
	}

	if err := c.t.ProcessWebhookUpdate(token, body); err != nil {
		prefix := token
		if len(token) > 12 {
			prefix = token[:12]
		}
		logger.Error("Ошибка обработки Webhook для токена %s: %v", prefix, err)
		w.WriteHeader(http.StatusNotFound)
		_ = json.NewEncoder(w).Encode(map[string]string{"error": err.Error()})
		return
	}

	w.WriteHeader(http.StatusOK)
}

func (c *Carpintero) SetWebhook(w http.ResponseWriter, r *http.Request) {
	if r.Method == http.MethodOptions {
		w.WriteHeader(http.StatusOK)
		return
	}

	userId, ok := readUserId(w, r)
	if !ok {
		return
	}

	err := c.t.SetWebhookUserBot(userId)
	if err != nil {
		logger.Error("'SetWebhook' Ошибка для uid %d: %v", userId, err)
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusInternalServerError)
		_ = json.NewEncoder(w).Encode(map[string]string{"error": err.Error()})
		return
	}

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	_ = json.NewEncoder(w).Encode(map[string]string{"status": "webhook set successfully"})
}

func readUserId(w http.ResponseWriter, r *http.Request) (uint32, bool) {
	// Читаем uid из query-параметров: /get-bot-name?uid=123
	uidStr := r.URL.Query().Get("uid")
	if uidStr == "" {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusBadRequest)
		_ = json.NewEncoder(w).Encode(map[string]string{"error": "uid is required"})
		return 0, false
	}

	uid, err := strconv.ParseUint(uidStr, 10, 32)
	if err != nil {
		logger.Error("'stopBot' Некорректный uid: %v", err)
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusBadRequest)
		_ = json.NewEncoder(w).Encode(map[string]string{"error": "invalid uid"})
		return 0, false
	}

	return uint32(uid), true
}
