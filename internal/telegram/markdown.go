package telegram

import (
	"encoding/json"
	"regexp"
	"strings"

	"github.com/ikermy/air_logger/v2/pkg/logger"
	tele "gopkg.in/telebot.v4"
)

// detectAndPrepareText определяет формат текста и подготавливает его для отправки
// Выполняет все стадии подготовки:
// 1. Удаление markdown code blocks (```json ... ```)
// 2. Нормализация переносов строк и табуляций
// 3. Определение формата (HTML/MarkdownV2/Markdown/Plain)
// 4. Экранирование специальных символов при необходимости
// Возвращает подготовленный текст и режим отправки
func detectAndPrepareText(text string) (string, *tele.SendOptions) {
	// Стадия 1: Удаляем лишние пробелы
	text = strings.TrimSpace(text)

	if text == "" {
		return text, nil
	}

	// Стадия 2: Удаляем markdown code blocks (```json ... ```, ```text ... ```)
	// Регулярка для поиска блоков типа ```язык\nкод\n```
	codeBlockPattern := regexp.MustCompile("(?s)```[a-z]*\\s*\\n(.+?)\\n```")
	if codeBlockPattern.MatchString(text) {
		// Извлекаем содержимое из code block
		matches := codeBlockPattern.FindStringSubmatch(text)
		if len(matches) > 1 {
			text = strings.TrimSpace(matches[1])
			logger.Debug("detectAndPrepareText: удалён markdown code block")
		}
	}

	if unquoted, ok := tryUnquoteJSONString(text); ok {
		text = unquoted
	}

	// Стадия 3: Нормализация переносов строк и табуляций
	// Заменяем \r\n на \n
	text = strings.ReplaceAll(text, "\r\n", "\n")
	text = strings.ReplaceAll(text, `\n`, "\n")
	text = strings.ReplaceAll(text, `\t`, "\t")
	text = strings.ReplaceAll(text, `\"`, `"`)
	// Удаляем лишние табуляции в начале строк
	text = regexp.MustCompile(`(?m)^\t+`).ReplaceAllString(text, "")

	// Стадия 4: Проверяем, не является ли текст JSON
	trimmed := strings.TrimSpace(text)
	if (strings.HasPrefix(trimmed, "{") && strings.HasSuffix(trimmed, "}")) ||
		(strings.HasPrefix(trimmed, "[") && strings.HasSuffix(trimmed, "]")) {
		logger.Debug("detectAndPrepareText: обнаружен JSON, отправляем как plain text")
		return text, nil
	}

	// Стадия 5: Проверяем наличие HTML тегов
	hasHTML := strings.Contains(text, "<b>") ||
		strings.Contains(text, "<i>") ||
		strings.Contains(text, "<code>") ||
		strings.Contains(text, "<pre>") ||
		strings.Contains(text, "<a href")

	if hasHTML {
		logger.Debug("detectAndPrepareText: обнаружен HTML формат")
		return text, &tele.SendOptions{ParseMode: tele.ModeHTML}
	}

	// Стадия 6: Нормализуем CommonMark/Markdown в Telegram MarkdownV2
	normalizedText := normalizeTelegramMarkdown(text)
	hasMarkdownV2 := strings.Contains(normalizedText, "*") ||
		strings.Contains(normalizedText, "__") ||
		strings.Contains(normalizedText, "~") ||
		strings.Contains(normalizedText, "||") ||
		strings.Contains(normalizedText, "`") ||
		(strings.Contains(normalizedText, "[") && strings.Contains(normalizedText, "]("))

	if hasMarkdownV2 {
		if isValidTelegramMarkdownV2(normalizedText) {
			escaped := escapeTelegramMarkdownV2(normalizedText)
			logger.Debug("detectAndPrepareText: используем MarkdownV2 с экранированием. Исходная длина: %d, обработанная: %d", len(text), len(escaped))
			return escaped, &tele.SendOptions{ParseMode: tele.ModeMarkdownV2}
		}
		escaped := escapeMarkdownV2Full(normalizedText)
		logger.Debug("detectAndPrepareText: некорректный MarkdownV2, экранируем все символы. Исходная длина: %d, обработанная: %d", len(text), len(escaped))
		return escaped, &tele.SendOptions{ParseMode: tele.ModeMarkdownV2}
	}

	// Стадия 7: Проверяем старый Markdown (одинарные * и _)
	hasMarkdown := (strings.Contains(text, "*") && !strings.Contains(text, "**")) ||
		(strings.Contains(text, "_") && !strings.Contains(text, "__"))

	if hasMarkdown {
		logger.Debug("detectAndPrepareText: используем Markdown (legacy)")
		return text, &tele.SendOptions{ParseMode: tele.ModeMarkdown}
	}

	// Стадия 8: По умолчанию - обычный текст (без экранирования)
	return text, nil
}

func tryUnquoteJSONString(text string) (string, bool) {
	trimmed := strings.TrimSpace(text)
	if len(trimmed) < 2 || trimmed[0] != '"' || trimmed[len(trimmed)-1] != '"' {
		return "", false
	}

	var unquoted string
	if err := json.Unmarshal([]byte(trimmed), &unquoted); err != nil {
		return "", false
	}

	return unquoted, true
}

func normalizeTelegramMarkdown(text string) string {
	reBold := regexp.MustCompile(`\*\*(.+?)\*\*`)
	reStrike := regexp.MustCompile(`~~(.+?)~~`)

	text = reBold.ReplaceAllString(text, `*$1*`)
	text = reStrike.ReplaceAllString(text, `~$1~`)

	return text
}

// isValidTelegramMarkdownV2 проверяет корректность Telegram MarkdownV2 форматирования
func isValidTelegramMarkdownV2(text string) bool {
	markers := []string{"__", "||", "`"}

	for _, marker := range markers {
		count := strings.Count(text, marker)
		if count%2 != 0 {
			return false // Непарные маркеры
		}
	}

	// Проверяем корректность ссылок [text](url)
	linkPattern := `\[([^\]]+)\]\(([^)]+)\)`
	matched, _ := regexp.MatchString(linkPattern, text)
	if strings.Contains(text, "[") && strings.Contains(text, "](") && !matched {
		return false
	}

	return true
}

// escapeMarkdownV2Full полностью экранирует ВСЕ специальные символы для MarkdownV2
// Используется когда нужно отправить текст БЕЗ форматирования
func escapeMarkdownV2Full(text string) string {
	specialChars := []string{
		"_", "*", "[", "]", "(", ")", "~", "`",
		">", "#", "+", "-", "=", "|", "{", "}", ".", "!",
	}
	escaped := text
	for _, char := range specialChars {
		escaped = strings.ReplaceAll(escaped, char, "\\"+char)
	}
	return escaped
}

func escapeTelegramMarkdownV2(text string) string {
	var result strings.Builder
	runes := []rune(text)

	for i := 0; i < len(runes); i++ {
		char := runes[i]

		switch char {
		case '*', '_', '~', '`':
			result.WriteRune(char)
		case '|':
			if i+1 < len(runes) && runes[i+1] == '|' {
				result.WriteString("||")
				i++
			} else {
				result.WriteString("\\|")
			}
		case '[', ']', '(', ')':
			result.WriteRune(char)
		case '\\':
			result.WriteString("\\\\")
		case '.', '!', '-', '#', '+', '=', '{', '}', '>':
			result.WriteRune('\\')
			result.WriteRune(char)
		default:
			result.WriteRune(char)
		}
	}

	return result.String()
}
