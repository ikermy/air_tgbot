package db

import (
	"air_tgbot/internal/domain"
	"air_tgbot/internal/repository"
	"air_tgbot/internal/repository/mysql"
	"context"

	"github.com/ikermy/air_common/pkg/comdb"
	"github.com/ikermy/air_logger/v2/pkg/logger"
)

// DB обёртка соединения с базой данных и репозиториями
type DB struct {
	*comdb.DB
	repo repository.Repository
}

// New создаёт подключение к БД и инициализирует репозитории
func New(parent context.Context) (*DB, error) {
	base, err := comdb.New(parent)
	if err != nil {
		return nil, err
	}
	repo, err := mysql.New(base)
	if err != nil {
		return nil, err
	}
	return &DB{
		DB:   base,
		repo: repo,
	}, nil
}

// Repo возвращает набор репозиториев
func (d *DB) Repo() repository.Repository {
	return d.repo
}

// GetTgBotUsers получает всех пользователей с включённым ботом
func (d *DB) GetTgBotUsers(ctx context.Context) ([]domain.UserDetails, error) {
	return d.repo.Internal.GetTgBotUsers(ctx)
}

// GetTgBotUser получает данные конкретного пользователя
func (d *DB) GetTgBotUser(ctx context.Context, userId uint32) (*domain.UserDetails, error) {
	return d.repo.Internal.GetTgBotUser(ctx, userId)
}

// HandlerClose ожидает завершения всех операций с БД и закрывает соединение
func (d *DB) HandlerClose() {
	go func() {
		<-d.MainCTX().Done()
		logger.Info("DB: контекст отменен, ожидаю завершения всех операций...")
		<-domain.UsersDB
		logger.Info("DB: все модули завершили работу, закрываю соединение...")
		if err := d.Close(); err != nil {
			logger.Error("DB: ошибка при закрытии: %v", err)
		}
		close(domain.Exit)
	}()
}
