# AiR_Tgbot

![air_tgbot](air_tgbot_logo.png)

[🇬🇧 English version](README.md)

![Версия Go](https://img.shields.io/badge/Go-1.25.8-00ADD8?logo=go)
![Лицензия](https://img.shields.io/badge/license-MIT-blue)
[![Telegram](https://img.shields.io/badge/Telegram-Join%20Chat-blue?logo=telegram)](https://t.me/marusia_dev)

`air_tgbot` — микросервис AiR-платформы для запуска и обслуживания Telegram-ботов с поддержкой AI-ассистентов.

## Защита пользовательских данных

Все пользовательские данные шифруются индивидуальным `MasterKey`. Данный ключ доступен только по паролю пользователя и шифрует API-токен Telegram бота, историю диалогов, индивидуальные настройки бота. Расшифровка этих данных возможна только после авторизации пользователя в системе индивидуальным паролем пользователя. Даже в случае компрометации или утечки базы данных, все пользовательские данные останутся недоступны как для злоумышленников, так и для администрации сервиса. 

## Возможности

- динамический запуск, остановка и перезапуск пользовательских Telegram-ботов;
- обработка текста, голосовых сообщений, изображений и документов;
- работа с AI-провайдерами OpenAI, Mistral и Google;
- потоковая обработка ответов ассистента;
- интеграция с CRM и переключение диалога на оператора;
- поддержка long polling и Telegram webhook;
- шифрованное хранение настроек в MySQL и Redis;
- получение конфигурации и master keys через gRPC от `AiR_ORCHESTRATOR`;
- HTTP endpoint’ы для управления ботами и Prometheus-метрики;
- graceful shutdown при остановке приложения или контейнера.

## Архитектура

```text
Telegram
   |
   v
air_tgbot ---- MySQL
   |  \------- Redis
   |  \------- AiR_ORCHESTRATOR (gRPC)
   |  \------- AI Router (OpenAI / Mistral / Google)
   \---------- CRM / Operator / Prometheus
```

Основная логика размещена в `internal/telegram`. Композиция компонентов выполняется в `internal/app`, работа с данными — через `internal/repository/mysql`, HTTP-обработчики — в `internal/delivery/http`.

## Технологии

Go, MySQL, Redis, gRPC, Telegram Bot API, OpenAI, Mistral, Google AI, Prometheus, Docker и Docker Compose.

## Запуск в development

Для запуска используется [`dev.yml`](dev.yml). Сервис подключается к внешним Docker-сетям `air_shared` и `monitoring_shared`, а также к контейнерам:

- `air_db:3306` — MySQL;
- `air_redis:6379` — Redis;
- `airorc:50051` — конфигурационный gRPC-сервис.

```bash
docker compose -f dev.yml up --build
```

В development включено debug-логирование, публичный адрес задан как `localhost`, webhook отключён. Сервисный ключ монтируется из `.service_key` в `/run/secrets/service_key` в режиме read-only.

## Конфигурация

Основные переменные окружения:

```text
DB_HOST
DB_NAME
DB_USER
DB_PASSWORD
REDIS_ADDR
REDIS_PASSWORD
REDIS_DB
GRPC_CONFIG_HOST
SERVICE_KEY_FILE
REAL_URL
LOG_LEVEL
WEBHOOK
GLOB_USER_MODEL_TTL
```

Пример сервисного ключа находится в [`.service_key.example`](.service_key.example). Секретный файл не следует добавлять в репозиторий.

## HTTP и мониторинг

HTTP-сервер слушает порт `8080`.

Основные маршруты:

```text
GET  /metrics
GET  /tgbot/available
GET  /tgbot/getname
POST /tgbot/notification
POST /tgbot/verification
POST /tgbot/adnot
POST /tgbot/enable
POST /tgbot/disable
POST /tgbot/restart
POST /open/tgbot/setwebhook
POST /open/tgbot/webhook/{token}
```

Метрики включают количество и длительность обработки сообщений, отправку сообщений в Telegram, запросы к CRM, жизненный цикл ботов, активные диалоги, операторский режим и HTTP-запросы.

Полный описательный контракт находится в [`doc/openapi.yaml`](doc/openapi.yaml).

## Production

Production-конфигурация находится в [`prod.yml`](prod.yml). В ней включены:

- webhook-режим;
- `LOG_LEVEL=info` - чувствительные пользовательские данные не логируются;
- домен из переменной `DOMAIN`;
- автоматический restart контейнера;
- Docker-логи с ограниченной ротацией;
- подключение к общим сетям проекта AiR.

Dockerfile собирает статический Go-бинарник и помещает его в минимальный runtime-образ на базе `scratch`.

## Связанные сервисы
- [air_common](https://github.com/ikermy/air_common) — Общая библиотека для AI‑микросервисов
- [air_orchestrator](https://github.com/ikermy/air_orchestrator) — Главный сервис оркестратор
- [air_operator](https://github.com/ikermy/air_operator) — Сервис переадресации ответов на операторов от пользователей, поддерживает все типы ботов
- [marusia_crm](https://github.com/ikermy/marusia_crm) — Сервис интеграции с внешними CRM системами
- [air_logger](https://github.com/ikermy/air_logger) — Вспомогательный сервис логирования событий с поддержкой многопользовательского режима и поддержкой сборщика логов loki

## Лицензия

Проект распространяется по лицензии [MIT](LICENSE). Она разрешает свободно использовать, копировать, изменять и распространять программное обеспечение при сохранении текста лицензии и уведомления об авторских правах.

Полный текст лицензии доступен в файле [`LICENSE`](LICENSE).

## Контакты
[![Telegram](https://img.shields.io/badge/Telegram-Contact-blue?logo=telegram)](https://t.me/ikermy)

