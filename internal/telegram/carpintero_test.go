package telegram

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	tele "gopkg.in/telebot.v4"
)

func TestConfigureWebhookForWebhookMode(t *testing.T) {
	const token = "123456:TEST_TOKEN"
	var receivedURL string

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/bot"+token+"/setWebhook" {
			t.Errorf("unexpected Telegram API path: %s", r.URL.Path)
		}
		var request struct {
			URL string `json:"url"`
		}
		if err := json.NewDecoder(r.Body).Decode(&request); err != nil {
			t.Errorf("decode request: %v", err)
		}
		receivedURL = request.URL
		_ = json.NewEncoder(w).Encode(map[string]any{"ok": true, "result": true})
	}))
	defer server.Close()

	bot, err := tele.NewBot(tele.Settings{
		Token:   token,
		URL:     server.URL,
		Offline: true,
	})
	if err != nil {
		t.Fatalf("create bot: %v", err)
	}

	if err := configureWebhook(bot, token); err != nil {
		t.Fatalf("configure webhook: %v", err)
	}
	bot.Poller = &tele.Webhook{}

	if _, ok := bot.Poller.(*tele.Webhook); !ok {
		t.Fatalf("WEBHOOK=true must switch poller to tele.Webhook, got %T", bot.Poller)
	}
	if !strings.HasSuffix(receivedURL, "/open/tgbot/webhook/"+token) {
		t.Errorf("unexpected webhook URL: %q", receivedURL)
	}
}
