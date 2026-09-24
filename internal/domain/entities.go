package domain

import (
	"github.com/ikermy/air-common/pkg/comdom"
)

// UserDetails представляет информацию о пользователе, включая настройки телеграм-бота
type UserDetails struct {
	UserId       int64               // Идентификатор пользователя
	TgBot        string              // Токен телеграм-бота
	TgBotEnabled bool                // Флаг включения бота
	AssistName   string              // Имя ассистента
	AssistantId  string              // Идентификатор ассистента
	Provider     comdom.ProviderType // Тип провайдера: 1=OpenAI, 2=Mistral
	MetaAction   string              // Поле MetaAction из модели ассистента
	Triggers     []string            // Список триггеров из модели ассистента
	Espero       uint8               // Значение Espero
	AskLimit     uint32              // Лимит запросов
	Ignore       bool                // Игнорировать сообщения до ответа ассистента
	Events       Notifications       // При каких событиях присылать уведомления
}

// Notifications события уведомлений
type Notifications struct {
	Start  bool
	End    bool
	Target bool
}

// Redis — параметры подключения (заполняются в main.go из env).
type Redis struct {
	RedisAddr     string // REDIS_ADDR (default: "" — Redis отключён)
	RedisPassword string // REDIS_PASSWORD
	RedisDB       int    // REDIS_DB (default: 0)
}

// CarpCh - канал для передачи уведомлений
type CarpCh struct {
	TelegaID int64
	Message  string
}
