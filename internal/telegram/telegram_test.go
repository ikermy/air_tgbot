package telegram

import (
	"air_tgbot/internal/domain"
	"context"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/ikermy/air_common/pkg/model"
	"github.com/ikermy/air_common/pkg/model/commdom"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	tele "gopkg.in/telebot.v4"
)

type mockFirstInteractionCache struct {
	hasResult   bool
	hasErr      error
	loadUserIDs []int64
	loadErr     error
	setErr      error
	setCalls    []int64
}

func (m *mockFirstInteractionCache) Has(_ context.Context, _ uint32, senderID int64) (bool, error) {
	_ = senderID
	return m.hasResult, m.hasErr
}

func (m *mockFirstInteractionCache) Set(_ context.Context, _ uint32, senderID int64) error {
	m.setCalls = append(m.setCalls, senderID)
	return m.setErr
}

func (m *mockFirstInteractionCache) LoadUser(_ context.Context, _ uint32) ([]int64, error) {
	return m.loadUserIDs, m.loadErr
}

// ============================================================================
// ВСПОМОГАТЕЛЬНЫЕ ФУНКЦИИ
// ============================================================================

// createMockUser создаёт простой тестовый экземпляр User для тестирования
func createMockUser() *User {
	ctx := context.Background()
	user := &User{
		ctx:    ctx,
		cancel: func() {},
		bot:    make(map[uint32]*Bot),
	}
	return user
}

// ============================================================================
// БАЗОВОЕ ТЕСТИРОВАНИЕ
// ============================================================================

// TestUser_CreateInstance проверяет создание экземпляра User
func TestUser_CreateInstance(t *testing.T) {
	user := createMockUser()

	assert.NotNil(t, user)
	assert.NotNil(t, user.ctx)
	assert.NotNil(t, user.bot)
	assert.Equal(t, 0, len(user.bot))
}

// TestUser_IsOperatorMode проверяет проверку режима оператора
func TestUser_IsOperatorMode(t *testing.T) {
	user := createMockUser()
	dialogId := uint64(12345)

	// Изначально режим оператора выключен
	isOperator := user.IsOperatorMode(dialogId)
	assert.False(t, isOperator, "Режим оператора должен быть выключен по умолчанию")

	// Включаем режим оператора
	user.SetOperatorMode(dialogId, true)
	isOperator = user.IsOperatorMode(dialogId)
	assert.True(t, isOperator, "Режим оператора должен быть включен")

	// Выключаем режим оператора
	user.SetOperatorMode(dialogId, false)
	isOperator = user.IsOperatorMode(dialogId)
	assert.False(t, isOperator, "Режим оператора должен быть выключен")
}

// TestUser_SetOperatorMode проверяет установку режима оператора
func TestUser_SetOperatorMode(t *testing.T) {
	user := createMockUser()
	dialogId := uint64(12345)

	// Включаем режим оператора
	user.SetOperatorMode(dialogId, true)
	value, exists := user.operatorModeByDialog.Load(dialogId)
	assert.True(t, exists, "Запись должна существовать в карте")
	assert.True(t, value.(bool), "Режим оператора должен быть true")

	// Выключаем режим оператора (запись удаляется из карты)
	user.SetOperatorMode(dialogId, false)
	_, exists = user.operatorModeByDialog.Load(dialogId)
	assert.False(t, exists, "Запись должна быть удалена из карты при отключении режима")
}

// ============================================================================
// ТЕСТИРОВАНИЕ ОПЕРАТОРСКОГО РЕЖИМА
// ============================================================================

// TestOperatorMode_BasicSwitching проверяет базовое переключение режима оператора
func TestOperatorMode_BasicSwitching(t *testing.T) {
	user := createMockUser()
	dialogId := uint64(5001)

	// Проверяем изначальное состояние
	assert.False(t, user.IsOperatorMode(dialogId), "Режим оператора должен быть выключен по умолчанию")

	// Включаем режим оператора
	user.SetOperatorMode(dialogId, true)
	assert.True(t, user.IsOperatorMode(dialogId), "Режим оператора должен быть включен")

	// Выключаем режим оператора
	user.SetOperatorMode(dialogId, false)
	assert.False(t, user.IsOperatorMode(dialogId), "Режим оператора должен быть выключен")
}

// TestOperatorMode_MultipleDialogs проверяет независимость режима для разных диалогов
func TestOperatorMode_MultipleDialogs(t *testing.T) {
	user := createMockUser()

	dialog1 := uint64(5002)
	dialog2 := uint64(5003)
	dialog3 := uint64(5004)

	// Включаем режим оператора для dialog1
	user.SetOperatorMode(dialog1, true)
	assert.True(t, user.IsOperatorMode(dialog1), "Dialog1: режим оператора должен быть включен")
	assert.False(t, user.IsOperatorMode(dialog2), "Dialog2: режим оператора должен быть выключен")
	assert.False(t, user.IsOperatorMode(dialog3), "Dialog3: режим оператора должен быть выключен")

	// Включаем режим оператора для dialog2
	user.SetOperatorMode(dialog2, true)
	assert.True(t, user.IsOperatorMode(dialog1), "Dialog1: режим оператора должен остаться включенным")
	assert.True(t, user.IsOperatorMode(dialog2), "Dialog2: режим оператора должен быть включен")
	assert.False(t, user.IsOperatorMode(dialog3), "Dialog3: режим оператора должен быть выключен")

	// Выключаем режим оператора для dialog1
	user.SetOperatorMode(dialog1, false)
	assert.False(t, user.IsOperatorMode(dialog1), "Dialog1: режим оператора должен быть выключен")
	assert.True(t, user.IsOperatorMode(dialog2), "Dialog2: режим оператора должен остаться включенным")
	assert.False(t, user.IsOperatorMode(dialog3), "Dialog3: режим оператора должен быть выключен")
}

// TestOperatorMode_RapidSwitching проверяет быстрое переключение режима
func TestOperatorMode_RapidSwitching(t *testing.T) {
	user := createMockUser()
	dialogId := uint64(5005)

	// Быстро переключаем режим несколько раз
	for i := 0; i < 10; i++ {
		user.SetOperatorMode(dialogId, true)
		assert.True(t, user.IsOperatorMode(dialogId), fmt.Sprintf("Итерация %d: включение", i))

		user.SetOperatorMode(dialogId, false)
		assert.False(t, user.IsOperatorMode(dialogId), fmt.Sprintf("Итерация %d: выключение", i))
	}
}

// ============================================================================
// НАГРУЗОЧНОЕ ТЕСТИРОВАНИЕ
// ============================================================================

// TestLoad_ConcurrentOperatorSwitches проверяет одновременные переключения режима оператора
func TestLoad_ConcurrentOperatorSwitches(t *testing.T) {
	if testing.Short() {
		t.Skip("Пропуск нагрузочного теста в кратком режиме")
	}

	user := createMockUser()

	// Настройки
	dialogsCount := 20
	switchesPerDialog := 10
	startDialogId := uint64(9000)

	var wg sync.WaitGroup
	startTime := time.Now()
	errorCount := 0
	var mu sync.Mutex

	// Запускаем горутины для каждого диалога
	for i := 0; i < dialogsCount; i++ {
		wg.Add(1)
		go func(dialogIndex int) {
			defer wg.Done()

			dialogId := startDialogId + uint64(dialogIndex)

			// Переключаем режим оператора несколько раз
			for j := 0; j < switchesPerDialog; j++ {
				user.SetOperatorMode(dialogId, true)
				time.Sleep(5 * time.Millisecond)

				if !user.IsOperatorMode(dialogId) {
					mu.Lock()
					errorCount++
					mu.Unlock()
				}

				user.SetOperatorMode(dialogId, false)
				time.Sleep(5 * time.Millisecond)

				if user.IsOperatorMode(dialogId) {
					mu.Lock()
					errorCount++
					mu.Unlock()
				}
			}
		}(i)
	}

	// Ждём завершения всех горутин
	wg.Wait()

	duration := time.Since(startTime)
	totalSwitches := dialogsCount * switchesPerDialog * 2

	// Выводим статистику
	t.Logf("\n=== СТАТИСТИКА ТЕСТА ПЕРЕКЛЮЧЕНИЙ ===")
	t.Logf("Диалогов: %d", dialogsCount)
	t.Logf("Переключений на диалог: %d", switchesPerDialog)
	t.Logf("Всего переключений: %d", totalSwitches)
	t.Logf("Ошибок: %d", errorCount)
	t.Logf("Длительность теста: %v", duration)
	t.Logf("Скорость переключений: %.2f переключений/сек", float64(totalSwitches)/duration.Seconds())

	// Проверяем, что ошибок не было
	assert.Equal(t, 0, errorCount, "Не должно быть ошибок при конкурентных переключениях")
}

// TestLoad_ManyDialogsSimultaneous проверяет работу с множеством диалогов одновременно
func TestLoad_ManyDialogsSimultaneous(t *testing.T) {
	if testing.Short() {
		t.Skip("Пропуск нагрузочного теста в кратком режиме")
	}

	user := createMockUser()

	// Настройки
	dialogsCount := 100
	startDialogId := uint64(10000)

	var wg sync.WaitGroup
	startTime := time.Now()

	// Включаем режим оператора для всех диалогов одновременно
	for i := 0; i < dialogsCount; i++ {
		wg.Add(1)
		go func(dialogIndex int) {
			defer wg.Done()
			dialogId := startDialogId + uint64(dialogIndex)
			user.SetOperatorMode(dialogId, true)
		}(i)
	}

	wg.Wait()

	// Проверяем, что все диалоги включены
	for i := 0; i < dialogsCount; i++ {
		dialogId := startDialogId + uint64(i)
		assert.True(t, user.IsOperatorMode(dialogId), fmt.Sprintf("Dialog %d должен быть в режиме оператора", i))
	}

	// Выключаем режим оператора для всех диалогов
	for i := 0; i < dialogsCount; i++ {
		wg.Add(1)
		go func(dialogIndex int) {
			defer wg.Done()
			dialogId := startDialogId + uint64(dialogIndex)
			user.SetOperatorMode(dialogId, false)
		}(i)
	}

	wg.Wait()

	// Проверяем, что все диалоги выключены
	for i := 0; i < dialogsCount; i++ {
		dialogId := startDialogId + uint64(i)
		assert.False(t, user.IsOperatorMode(dialogId), fmt.Sprintf("Dialog %d должен быть НЕ в режиме оператора", i))
	}

	duration := time.Since(startTime)

	// Выводим статистику
	t.Logf("\n=== СТАТИСТИКА ТЕСТА МНОЖЕСТВЕННЫХ ДИАЛОГОВ ===")
	t.Logf("Диалогов: %d", dialogsCount)
	t.Logf("Длительность теста: %v", duration)
	t.Logf("Операций: %d", dialogsCount*2)
}

// ============================================================================
// ТЕСТИРОВАНИЕ СТРЕССОВЫХ СЦЕНАРИЕВ
// ============================================================================

// TestStress_RapidOperatorToggling проверяет очень быстрое переключение режима
func TestStress_RapidOperatorToggling(t *testing.T) {
	if testing.Short() {
		t.Skip("Пропуск стрессового теста в кратком режиме")
	}

	user := createMockUser()
	dialogId := uint64(20000)
	iterations := 1000

	startTime := time.Now()

	// Быстро переключаем режим много раз
	for i := 0; i < iterations; i++ {
		user.SetOperatorMode(dialogId, true)
		user.SetOperatorMode(dialogId, false)
	}

	duration := time.Since(startTime)

	// Проверяем финальное состояние
	assert.False(t, user.IsOperatorMode(dialogId), "Режим оператора должен быть выключен")

	// Выводим статистику
	t.Logf("\n=== СТАТИСТИКА СТРЕСС-ТЕСТА ===")
	t.Logf("Итераций: %d", iterations)
	t.Logf("Длительность: %v", duration)
	t.Logf("Операций/сек: %.2f", float64(iterations*2)/duration.Seconds())
}

// TestStress_ManyDialogsRapidChanges проверяет быстрые изменения для множества диалогов
func TestStress_ManyDialogsRapidChanges(t *testing.T) {
	if testing.Short() {
		t.Skip("Пропуск стрессового теста в кратком режиме")
	}

	user := createMockUser()
	dialogsCount := 50
	changesPerDialog := 20
	startDialogId := uint64(30000)

	var wg sync.WaitGroup
	startTime := time.Now()

	for i := 0; i < dialogsCount; i++ {
		wg.Add(1)
		go func(dialogIndex int) {
			defer wg.Done()
			dialogId := startDialogId + uint64(dialogIndex)

			for j := 0; j < changesPerDialog; j++ {
				user.SetOperatorMode(dialogId, j%2 == 0)
			}
		}(i)
	}

	wg.Wait()
	duration := time.Since(startTime)
	totalOperations := dialogsCount * changesPerDialog

	// Выводим статистику
	t.Logf("\n=== СТАТИСТИКА СТРЕСС-ТЕСТА МНОЖЕСТВЕННЫХ ДИАЛОГОВ ===")
	t.Logf("Диалогов: %d", dialogsCount)
	t.Logf("Изменений на диалог: %d", changesPerDialog)
	t.Logf("Всего операций: %d", totalOperations)
	t.Logf("Длительность: %v", duration)
	t.Logf("Операций/сек: %.2f", float64(totalOperations)/duration.Seconds())
}

// ============================================================================
// BENCHMARK ТЕСТЫ
// ============================================================================

// BenchmarkSetOperatorMode измеряет производительность установки режима оператора
func BenchmarkSetOperatorMode(b *testing.B) {
	user := createMockUser()
	dialogId := uint64(10000)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		user.SetOperatorMode(dialogId, true)
		user.SetOperatorMode(dialogId, false)
	}
}

// BenchmarkIsOperatorMode измеряет производительность проверки режима оператора
func BenchmarkIsOperatorMode(b *testing.B) {
	user := createMockUser()
	dialogId := uint64(10001)
	user.SetOperatorMode(dialogId, true)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = user.IsOperatorMode(dialogId)
	}
}

// BenchmarkConcurrentSetOperatorMode измеряет производительность конкурентной установки режима
func BenchmarkConcurrentSetOperatorMode(b *testing.B) {
	user := createMockUser()

	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		dialogId := uint64(10002)
		for pb.Next() {
			user.SetOperatorMode(dialogId, true)
			user.SetOperatorMode(dialogId, false)
		}
	})
}

// BenchmarkConcurrentIsOperatorMode измеряет производительность конкурентной проверки режима
func BenchmarkConcurrentIsOperatorMode(b *testing.B) {
	user := createMockUser()
	dialogId := uint64(10003)
	user.SetOperatorMode(dialogId, true)

	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			_ = user.IsOperatorMode(dialogId)
		}
	})
}

// ============================================================================
// ТЕСТЫ МОКОВ БД (DB MOCKS)
// ============================================================================

// MockDBForTesting - мок для интерфейса IntDB
type MockDBForTesting struct {
	mock.Mock
}

func (m *MockDBForTesting) GetTgBotUsers(ctx context.Context) ([]domain.UserDetails, error) {
	args := m.Called(ctx)
	if args.Get(0) == nil {
		return nil, args.Error(1)
	}
	return args.Get(0).([]domain.UserDetails), args.Error(1)
}

func (m *MockDBForTesting) GetTgBotUser(ctx context.Context, userId uint32) (*domain.UserDetails, error) {
	args := m.Called(ctx, userId)
	if args.Get(0) == nil {
		return nil, args.Error(1)
	}
	return args.Get(0).(*domain.UserDetails), args.Error(1)
}

// TestDBMock_GetTgBotUsers тест получения всех пользователей
func TestDBMock_GetTgBotUsers(t *testing.T) {
	mockDB := new(MockDBForTesting)

	users := []domain.UserDetails{
		{
			UserId:       123,
			TgBot:        "test_token",
			TgBotEnabled: true,
			AssistName:   "TestAssist",
			AssistantId:  "assist_123",
			Provider:     commdom.ProviderOpenAI,
		},
	}

	mockDB.On("GetTgBotUsers", mock.Anything).Return(users, nil)

	result, err := mockDB.GetTgBotUsers(context.Background())

	assert.NoError(t, err)
	assert.Len(t, result, 1)
	assert.Equal(t, int64(123), result[0].UserId)
	assert.Equal(t, "test_token", result[0].TgBot)
	mockDB.AssertExpectations(t)
}

// TestDBMock_GetTgBotUser тест получения конкретного пользователя
func TestDBMock_GetTgBotUser(t *testing.T) {
	mockDB := new(MockDBForTesting)

	user := &domain.UserDetails{
		UserId:       456,
		TgBot:        "user_token",
		TgBotEnabled: true,
		AssistName:   "Assistant",
		AssistantId:  "assist_456",
		Provider:     commdom.ProviderMistral,
	}

	mockDB.On("GetTgBotUser", mock.Anything, uint32(456)).Return(user, nil)

	result, err := mockDB.GetTgBotUser(context.Background(), 456)

	assert.NoError(t, err)
	assert.NotNil(t, result)
	assert.Equal(t, int64(456), result.UserId)
	assert.Equal(t, commdom.ProviderMistral, result.Provider)
	mockDB.AssertExpectations(t)
}

// TestDBMock_GetTgBotUserNotFound тест когда пользователь не найден
func TestDBMock_GetTgBotUserNotFound(t *testing.T) {
	mockDB := new(MockDBForTesting)

	mockDB.On("GetTgBotUser", mock.Anything, uint32(999)).Return(nil, nil)

	result, err := mockDB.GetTgBotUser(context.Background(), 999)

	assert.NoError(t, err)
	assert.Nil(t, result)
	mockDB.AssertExpectations(t)
}

func TestBot_PreloadFirstInteraction_LoadsFalseFlags(t *testing.T) {
	cache := &mockFirstInteractionCache{loadUserIDs: []int64{1001, 1002}}
	b := &Bot{ctx: context.Background(), userId: 77, firstCache: cache}

	b.preloadFirstInteraction()

	v1, ok1 := b.firstInteraction.Load(int64(1001))
	v2, ok2 := b.firstInteraction.Load(int64(1002))
	assert.True(t, ok1)
	assert.True(t, ok2)
	assert.False(t, v1.(bool))
	assert.False(t, v2.(bool))
}

func TestHandleFirstInteraction_ReturnsFalseForRestoredKey(t *testing.T) {
	b := &Bot{}
	b.firstInteraction.Store(int64(123), false)
	b.assist = &model.Assistant{}

	first := b.handleFirstInteraction(&tele.User{ID: 123}, "restored")

	assert.False(t, first)
	_, exists := b.firstInteraction.Load(int64(123))
	assert.False(t, exists)
}

func TestInitializeResponderSession_UsesRedisMarkerAsFalse(t *testing.T) {
	cache := &mockFirstInteractionCache{hasResult: true}
	b := &Bot{ctx: context.Background(), userId: 5, firstCache: cache}
	b.firstInteraction.Store(int64(333), true)

	b.firstInteraction.Delete(int64(333))
	first := true
	if b.firstCache != nil {
		exists, err := b.firstCache.Has(b.ctx, b.userId, 333)
		if err == nil && exists {
			first = false
		}
	}
	b.firstInteraction.Store(int64(333), first)

	v, ok := b.firstInteraction.Load(int64(333))
	assert.True(t, ok)
	assert.False(t, v.(bool))
	assert.Empty(t, cache.setCalls)
}

func TestDetectAndPrepareText_UnescapesJSONAndConvertsMarkdown(t *testing.T) {
	input := `Ну что ж\n\n1. **Месси и кот на поле.**\n   — Кот: \"мяу\"`

	prepared, opts := detectAndPrepareText(input)

	assert.Equal(t, `Ну что ж

1\. *Месси и кот на поле\.*
   — Кот: "мяу"`, prepared)
	if assert.NotNil(t, opts) {
		assert.Equal(t, "MarkdownV2", opts.ParseMode)
	}
}

func TestDetectAndPrepareText_UnquotesJSONString(t *testing.T) {
	input := `"Привет\n**мир**"`

	prepared, opts := detectAndPrepareText(input)

	assert.Equal(t, "Привет\n*мир*", prepared)
	if assert.NotNil(t, opts) {
		assert.Equal(t, "MarkdownV2", opts.ParseMode)
	}
}
