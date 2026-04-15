# Полное руководство по интеграции спортивного API SofaScore

Данное руководство объединяет официальную документацию открытого API SofaScore, зеркального SportAPI (из RapidAPI) и глубокий анализ структуры JSON-ответов. Архитектура построена на использовании переменных пути (Path Variables), что позволяет выстроить строгую родословную данных (Data Lineage) от глобального расписания до атомарных X/Y координат действий игрока на поле.

---

## 1. Базовые URL и Авторизация

API предоставляет доступ к данным в реальном времени и исторической статистике (более 24 видов спорта). Все запросы начинаются с пути `/api/v1/`.

*   **Открытый API (SofaScore):** `https://www.sofascore.com`.
*   **RapidAPI Зеркало (SportAPI):** `https://sportapi7.p.rapidapi.com`. Требует заголовки `x-rapidapi-host` и `x-rapidapi-key`.

---

## 2. Иерархия Данных (Data Lineage)

Для достижения "дна" иерархии (сырых данных для визуализации) парсер должен собрать цепочку ключей:
**`{sport}` -> `{category_id}` -> `{unique_tournament_id}` -> `{season_id}` -> `{event_id}` -> `{player_id}`**.

Ловушки и исключения базы данных:
*   **Игнорируемые данные:** Во всех ответах присутствует объект `fieldTranslations` с переводами имен (`nameTranslation`, `shortNameTranslation` на `ar`, `hi`, `ru`, `bn` и т.д.). Эти узлы следует игнорировать для экономии места.
*   **Кандидаты для JSONB:** Сложные и динамические объекты, такие как `statistics` (включающие десятки меняющихся метрик: `expectedGoals`, `accuratePass` и т.д.) и цвета команд `teamColors`, рекомендуется хранить целиком в формате JSONB.
*   **Время:** Все временные метки отдаются в формате UNIX timestamp (например, `startTimestamp`: `1775847600`).

---

## 3. Основные сценарии сбора (Workflows)

### Сценарий 1: Сбор расписания (В ширину)
Идеально подходит для получения всех матчей за день и извлечения из них базовых ID турниров и сезонов.

1.  **Получение всех матчей дня:**
    `GET /api/v1/sport/{sport}/scheduled-events/{date}`
    *Параметры:* `{sport}` (например, `football`), `{date}` в формате `YYYY-MM-DD`.
    *Результат:* Массив `events`, в котором уже лежат `event.id`, `tournament.uniqueTournament.id` и `season.id`.

2.  **Сбор по конкретной категории (Опционально):**
    Сначала получить список активных стран/категорий:
    `GET /api/v1/sport/{sport}/{date}/{timezoneOffset}/categories`.
    Затем получить матчи этой категории:
    `GET /api/v1/category/{id}/scheduled-events/{date}`.

### Сценарий 2: Погружение в матч (В глубину)
Имея `{event_id}`, скрипт собирает детали матча.

*   **Детали матча:** `GET /api/v1/event/{event_id}`.
*   **Инциденты (Голы, карточки, замены, VAR):** `GET /api/v1/event/{event_id}/incidents`.
*   **Составы:** `GET /api/v1/event/{event_id}/lineups`. Возвращает массивы `home.players` и `away.players`. Отсюда извлекается целевой `{player_id}`.
*   **Общая статистика матча:** `GET /api/v1/event/{event_id}/statistics`.
*   **Тренеры:** `GET /api/v1/event/{event_id}/managers`.
*   **Коэффициенты (Odds):** `GET /api/v1/event/{event_id}/odds/1/all` (где `1` — ID провайдера по умолчанию).

### Сценарий 3: Атомарные данные (Дно иерархии)
Используя связку `{event_id}` и `{player_id}`, извлеченную из составов, система собирает X/Y координаты:

1.  **Карта ударов (Shotmap):**
    `GET /api/v1/event/{event_id}/shotmap/player/{player_id}`
    *Результат:* Массив `shotmap`, где лежат `playerCoordinates` (откуда били: `x`, `y`, `z`), `goalMouthCoordinates` (куда летел мяч: `y`, `z`), а также векторы `draw.start` и `draw.end`.

2.  **Векторы действий (Rating Breakdown):**
    `GET /api/v1/event/{event_id}/player/{player_id}/rating-breakdown`
    *Результат:* Массивы `passes`, `defensive`, `ball-carries`. Содержат `playerCoordinates` (точка старта) и `passEndCoordinates` (точка завершения).

3.  **Тепловая карта (Heatmap):**
    `GET /api/v1/event/{event_id}/player/{player_id}/heatmap`.
    *В RapidAPI v2 также есть общий эндпоинт командной тепловой карты:* `/api/v1/events/{id}/heatmap`.

---

## 4. Глобальный справочник эндпоинтов (API Reference)

### Турниры и Сезоны (Tournaments & Seasons)
| Описание | Эндпоинт | Ключевые параметры |
| :--- | :--- | :--- |
| Информация о турнире | `GET /api/v1/tournaments/{id}` | `id` уникального турнира |
| Доступные сезоны | `GET /api/v1/unique-tournament/{id}/seasons` | `id` уникального турнира |
| Турнирная таблица | `GET /api/v1/unique-tournament/{id}/season/{seasonId}/standings/{type}` | `type`: `total`, `home`, `away` |
| Список туров (Rounds) | `GET /api/v1/tournaments/{id}/seasons/{seasonId}/rounds` | `id`, `seasonId` |
| Сетка кубка | `GET /api/v1/tournaments/{id}/seasons/{seasonId}/cuptrees` | `id`, `seasonId` |
| Статистика турнира | `GET /api/v1/tournaments/{id}/seasons/{seasonId}/statistics` | `id`, `seasonId` |

### Команды (Teams)
| Описание | Эндпоинт | Ключевые параметры |
| :--- | :--- | :--- |
| Профиль команды | `GET /api/v1/teams/{id}` | `id` команды |
| Текущий состав | `GET /api/v1/teams/{id}/players` | `id` команды |
| Прошлые матчи команды | `GET /api/v1/teams/{id}/events/last/{page}` | `id`, `page` (начиная с 0) |
| Будущие матчи команды | `GET /api/v1/teams/{id}/events/next/{page}` | `id`, `page` |
| График формы (Performance) | `GET /api/v1/teams/{id}/performance` | `id` команды |

### Игроки и Тренеры (Players & Managers)
| Описание | Эндпоинт | Ключевые параметры |
| :--- | :--- | :--- |
| Профиль игрока | `GET /api/v1/player/{id}` | `id` игрока |
| Характеристики (Сильные/Слабые стороны) | `GET /api/v1/player/{id}/characteristics` | `id` игрока |
| История трансферов | `GET /api/v1/player/{id}/transfer-history` | `id` игрока |
| Статистика по сезонам | `GET /api/v1/player/{id}/statistics/seasons` | `id` игрока |
| Статистика в сборной | `GET /api/v1/player/{id}/national-team-statistics` | `id` игрока |
| Профиль тренера | `GET /api/v1/manager/{id}` | `id` тренера (managerId) |

### Разное (Misc)
| Описание | Эндпоинт | Ключевые параметры |
| :--- | :--- | :--- |
| Глобальный поиск | `GET /api/v1/search?q={query}` | Поиск сущностей (игроков, команд, турниров) |
| Форма перед матчем | `GET /api/v1/event/{id}/pregame-form` | H2H форма перед игрой |
| AI Аналитика (После матча) | `GET /api/v1/event/{id}/ai-insights/{lang}` | `id`, `lang` (например, `ru` или `en`) |
| Трансляции (ТВ-каналы) | `GET /api/v1/event/{id}/tv-channels` | Телеканалы по странам |