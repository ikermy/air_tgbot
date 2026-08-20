package domain

// CarpCh - канал для передачи уведомлений
type CarpCh struct {
	TelegaID int64
	Message  string
}

var (
	CarpinteroCh = make(chan CarpCh, 1) // Канал для передачи уведомлений
	UsersDB      = make(chan struct{})  // Канал уведомления о завершении операций пользователями ДБ
	Exit         = make(chan struct{})  // Канал завершения работы приложения

	// RedisAddr Redis — параметры подключения (заполняются в main.go из env).
	RedisAddr     string // REDIS_ADDR (default: "" — Redis отключён)
	RedisPassword string // REDIS_PASSWORD
	RedisDB       int    // REDIS_DB (default: 0)
)
