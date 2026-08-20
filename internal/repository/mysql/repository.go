package mysql

import (
	"air_tgbot/internal/domain"
	"air_tgbot/internal/repository"
	"context"
	"database/sql"
	"database/sql/driver"
	"errors"
	"fmt"

	"github.com/ikermy/air_common/pkg/comdb"
	"github.com/ikermy/air_common/pkg/mode"
	"github.com/ikermy/air_common/pkg/model/commdom"
	"github.com/ikermy/air_common/pkg/model/create"
	"github.com/ikermy/air_logger/v2/pkg/logger"
)

//// Обязательные методы ////

// NullBytes промежуточный тип для загрузки массива байт из базы
type NullBytes struct {
	Bytes []byte
	Valid bool // Valid = true, если Bytes не NULL
}

// Scan реализует интерфейс [sql.Scanner](database/sql/sql.go:447) для чтения BLOB/TEXT значений.
func (nb *NullBytes) Scan(value any) error {
	if value == nil {
		nb.Bytes = nil
		nb.Valid = false
		return nil
	}

	switch v := value.(type) {
	case []byte:
		nb.Bytes = append(nb.Bytes[:0], v...)
		nb.Valid = true
		return nil
	case string:
		nb.Bytes = append(nb.Bytes[:0], v...)
		nb.Valid = true
		return nil
	default:
		return fmt.Errorf("cannot scan type %T into NullBytes", value)
	}
}

// Value реализует интерфейс [driver.Valuer](database/sql/driver/types.go:35).
func (nb NullBytes) Value() (driver.Value, error) {
	if !nb.Valid {
		return nil, nil
	}

	return nb.Bytes, nil
}

/////////////////////////////

// Implementation реализация интерфейса Implementation для MySQL
type Implementation struct {
	db *comdb.DB
}

// New создаёт новый MySQL репозиторий пользователей
func New(db *comdb.DB) (repository.Repository, error) {
	if db == nil {
		return repository.Repository{}, fmt.Errorf("database connection is nil")
	}

	userRepo := &Implementation{
		db: db,
	}

	return repository.Repository{
		Internal: userRepo,
		External: db,
	}, nil
}

// populateUserDetails заполняет структуру UserDetails из NULL-безопасных значений базы данных
func populateUserDetails(user *domain.UserDetails, Name, assistantId sql.NullString, provider sql.NullByte, data NullBytes, start, end, target sql.NullBool) {
	// Обработка NULL значений
	if assistantId.Valid {
		user.AssistantId = assistantId.String
	}

	if Name.Valid {
		user.AssistName = Name.String
	}

	// Обработка информации о провайдере
	if provider.Valid {
		user.Provider = commdom.ProviderType(provider.Byte)
	} else {
		user.Provider = commdom.ProviderOpenAI // 1 = OpenAI по умолчанию
	}

	// Распаковываем и обрабатываем data если она существует
	if data.Valid {
		modelData, err := create.DecompressModelData(data.Bytes)
		if err == nil {
			user.MetaAction = modelData.MetaAction
			user.Triggers = modelData.Triggers
			user.AskLimit = uint32(modelData.Espero.Limit)
			user.Espero = modelData.Espero.Wait
			user.Ignore = modelData.Espero.Ignore
		}
	}

	// Обработка полей уведомлений
	if start.Valid {
		user.Events.Start = start.Bool
	}
	if end.Valid {
		user.Events.End = end.Bool
	}
	if target.Valid {
		user.Events.Target = target.Bool
	}
}

// GetTgBotUsers получает все пользователей с включённым телеграм-ботом
func (r *Implementation) GetTgBotUsers(ctx context.Context) ([]domain.UserDetails, error) {
	// Дочерний контекст с тайм-аутом на операцию
	ctxTimeout, cancel := context.WithTimeout(ctx, mode.GetSQLTimeToCancel())
	defer cancel()

	// Всегда возвращаем только записи с TgBot_enabled = 1
	query := `
    SELECT
     c.UserId,
     c.TgBot,
     c.TgBot_enabled,
     u_gpt.Name,
     u_gpt.AssistantId,
     u_gpt.Data,
     um.Provider,
     n.Start,
     n.End,
     n.Target
    FROM
     channels AS c
    LEFT JOIN
     user_models AS um ON c.UserId = um.UserId AND um.IsActive = 1
    LEFT JOIN
     user_gpt AS u_gpt ON um.ModelId = u_gpt.Id
    LEFT JOIN
     notifications AS n ON c.UserId = n.UserId
    WHERE
     c.TgBot IS NOT NULL AND c.TgBot_enabled = 1`

	rows, err := r.db.Conn().QueryContext(ctxTimeout, query)
	if err != nil {
		switch {
		case errors.Is(err, context.DeadlineExceeded):
			return nil, fmt.Errorf("тайм-аут (%d с) при получении пользователей бота: %w", mode.GetSQLTimeToCancel(), err)
		case errors.Is(err, context.Canceled):
			return nil, fmt.Errorf("операция отменена при получении пользователей бота: %w", err)
		default:
			return nil, fmt.Errorf("failed to execute query: %w", err)
		}
	}
	defer func(rows *sql.Rows) {
		err := rows.Close()
		if err != nil {
			logger.Info("ошибка закрытия rows: %v", err)
		}
	}(rows)

	// Собираем результаты
	var users []domain.UserDetails
	for rows.Next() {
		var user domain.UserDetails
		var Name, assistantId sql.NullString // На случай NULL значений
		var provider sql.NullByte            // Провайдер из БД (TINYINT)
		var data NullBytes                   // Для поля Data из BLOB
		var start, end, target sql.NullBool  // Для полей уведомлений

		err := rows.Scan(
			&user.UserId,
			&user.TgBot,
			&user.TgBotEnabled,
			&Name,
			&assistantId,
			&data,
			&provider,
			&start,
			&end,
			&target,
		)
		if err != nil {
			return nil, fmt.Errorf("failed to scan row: %w", err)
		}

		// Заполняем данные пользователя
		populateUserDetails(&user, Name, assistantId, provider, data, start, end, target)

		users = append(users, user)
	}

	// Проверяем ошибки после обработки результатов
	if err = rows.Err(); err != nil {
		switch {
		case errors.Is(err, context.DeadlineExceeded):
			return nil, fmt.Errorf("тайм-аут (%d с) при обработке результатов пользователей бота: %w", mode.GetSQLTimeToCancel(), err)
		case errors.Is(err, context.Canceled):
			return nil, fmt.Errorf("операция отменена при обработке результатов пользователей бота: %w", err)
		default:
			return nil, fmt.Errorf("error iterating rows: %w", err)
		}
	}

	return users, nil
}

// GetTgBotUser получает данные конкретного пользователя из БД по userId
func (r *Implementation) GetTgBotUser(ctx context.Context, userId uint32) (*domain.UserDetails, error) {
	// Дочерний контекст с тайм-аутом на операцию
	ctxTimeout, cancel := context.WithTimeout(ctx, mode.GetSQLTimeToCancel())
	defer cancel()

	query := `
    SELECT
     c.UserId,
     c.TgBot,
     c.TgBot_enabled,
     u_gpt.Name,
     u_gpt.AssistantId,
     u_gpt.Data,
     um.Provider,
     n.Start,
     n.End,
     n.Target
    FROM
     channels AS c
    JOIN
     users AS u ON c.UserId = u.Id
    LEFT JOIN
     user_models AS um ON u.Id = um.UserId AND um.IsActive = 1
    LEFT JOIN
     user_gpt AS u_gpt ON um.ModelId = u_gpt.Id
    LEFT JOIN
     notifications AS n ON u.Id = n.UserId
    WHERE
     c.UserId = ? AND c.TgBot IS NOT NULL AND c.TgBot_enabled = 1`

	var user domain.UserDetails
	var Name, assistantId sql.NullString
	var provider sql.NullByte
	var data NullBytes
	var start, end, target sql.NullBool

	err := r.db.Conn().QueryRowContext(ctxTimeout, query, userId).Scan(
		&user.UserId,
		&user.TgBot,
		&user.TgBotEnabled,
		&Name,
		&assistantId,
		&data,
		&provider,
		&start,
		&end,
		&target,
	)

	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return nil, nil // Пользователь не найден или отключен
		}
		switch {
		case errors.Is(err, context.DeadlineExceeded):
			return nil, fmt.Errorf("тайм-аут (%d с) при получении пользователя %d: %w", mode.GetSQLTimeToCancel(), userId, err)
		case errors.Is(err, context.Canceled):
			return nil, fmt.Errorf("операция отменена при получении пользователя %d: %w", userId, err)
		default:
			return nil, fmt.Errorf("ошибка получения пользователя %d: %w", userId, err)
		}
	}

	// Заполняем данные пользователя
	populateUserDetails(&user, Name, assistantId, provider, data, start, end, target)

	return &user, nil
}
