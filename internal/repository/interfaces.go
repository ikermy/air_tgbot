package repository

import (
	"air_tgbot/internal/domain"
	"context"

	"github.com/ikermy/air_common/pkg/comdb"
)

// InternalRepository внутренние методы работы с БД
type InternalRepository interface {
	GetTgBotUsers(ctx context.Context) ([]domain.UserDetails, error)
	GetTgBotUser(ctx context.Context, userId uint32) (*domain.UserDetails, error)
}

// ExternalDBRepository интерфейс для внешних методов БД (из AiR_Common)
type ExternalDBRepository interface {
	comdb.Exterior
}

// Repository объединяет все репозитории
type Repository struct {
	Internal InternalRepository
	External ExternalDBRepository
}
