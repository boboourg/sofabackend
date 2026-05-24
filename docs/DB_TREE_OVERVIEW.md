# Sofascore Data Tree For PostgreSQL

## 1. Главная логика дерева

Если смотреть не на URL, а на реальные envelope keys из JSON, у тебя вырисовывается такое каноническое дерево:

```text
sport
└── category
    └── uniqueTournament
        ├── seasons
        │   └── season
        │       ├── rounds
        │       ├── events
        │       │   └── event
        │       │       ├── incidents
        │       │       ├── lineups
        │       │       ├── managers
        │       │       ├── statistics
        │       │       ├── odds
        │       │       ├── h2h
        │       │       ├── pregame-form
        │       │       ├── votes
        │       │       └── tv
        │       ├── standings
        │       ├── statistics/info
        │       ├── statistics -> results[]
        │       ├── team-events
        │       ├── topPlayers
        │       ├── topTeams
        │       ├── team-of-the-week
        │       └── venues
        ├── featuredEvents
        └── media

team
├── team
├── pregameForm
├── uniqueTournamentSeasons
├── typesMap
└── statistics

player
├── player
├── uniqueTournamentSeasons
├── typesMap
├── statistics
├── heatmap
├── transferHistory
└── uniqueTournaments
```

Практический вывод:
- матчевая ось данных строится вокруг `event.id`
- турнирно-аналитическая ось строится вокруг пары `uniqueTournament.id + season.id`
- справочники `team`, `player`, `category`, `sport`, `country` переиспользуются во всех ветках

## 2. Что считать ядром модели

Не стоит делать одну таблицу на каждый Markdown-report. Лучше держать 3 слоя.

### Bronze

Сохраняй сырой ответ как есть:

- `api_response_raw`
  - `id`
  - `request_url`
  - `endpoint_pattern`
  - `envelope_keys`
  - `http_status`
  - `fetched_at`
  - `query_params jsonb`
  - `payload jsonb`
  - `payload_hash`

Этот слой нужен, потому что у Sofascore много нестабильных и полудинамических структур:
- `typesMap`
- `statisticsGroups`
- `tournamentTeamEvents`
- `topPlayers.rating[]`, `topPlayers.goals[]`, `topTeams.avgRating[]`
- envelope keys вида `home/away`, `currentRound/rounds`, `vote/bothTeamsToScoreVote/...`

### Silver

Здесь уже нормализованные таблицы.

### Gold

Материализованные представления и денормализованные аналитические таблицы для отчётов.

## 3. Канонические сущности для PostgreSQL

### Справочники

- `sport`
  - key: `id`
  - alt key: `slug`

- `country`
  - key: обычно `slug`
  - если встретится numeric/id, можно хранить отдельно, но `slug` нужен обязательно

- `category`
  - key: `id`
  - fk: `sport_id`, `country_slug`

- `unique_tournament`
  - key: `id`
  - alt key: `slug`
  - fk: `category_id`

- `season`
  - key: `id`
  - fk: `unique_tournament_id`

- `tournament`
  - key: `id`
  - alt key: `slug`
  - это operational tournament instance внутри event/standings
  - fk: `unique_tournament_id`, `category_id`

- `team`
  - key: `id`
  - alt key: `slug`
  - fk: `sport_id`, `category_id`

- `player`
  - key: `id`
  - alt key: `slug`

- `manager`
  - key: `id`

- `venue`
  - key: `id`, если есть
  - иначе суррогатный ключ + raw jsonb

- `odds_provider`
  - key: `id`

### Фактовые сущности

- `event`
  - key: `id`
  - alt keys: `slug`, `custom_id`, `detail_id`
  - fk: `tournament_id`, `unique_tournament_id`, `season_id`, `home_team_id`, `away_team_id`, `venue_id`

- `event_score`
  - one-to-one или embedded columns в `event`

- `event_lineup_player`
  - fk: `event_id`, `team_id`, `player_id`

- `event_incident`
  - fk: `event_id`
  - полезно хранить `side` (`home` / `away`)

- `event_manager`
  - fk: `event_id`, `team_id`, `manager_id`

- `event_odds_market`
  - fk: `event_id`, `provider_id`

- `event_vote`
  - fk: `event_id`
  - тип голоса лучше хранить отдельным полем, а не отдельной таблицей на каждый vote envelope

- `standing`
  - key: `id`
  - fk: `season_id`, `tournament_id`, `unique_tournament_id`
  - variant: `total/home/away`

- `standing_row`
  - key: `id`
  - fk: `standing_id`, `team_id`

- `season_stat_result`
  - суррогатный key
  - fk: `unique_tournament_id`, `season_id`, `player_id`, `team_id`
  - grain: одна строка результата одной statistics-query

- `season_stat_snapshot`
  - для уникализации query-варианта
  - columns:
    - `unique_tournament_id`
    - `season_id`
    - `group_code`
    - `accumulation`
    - `order_code`
    - `offset`
    - `limit`
    - `filters_jsonb`
    - `fields_jsonb`
    - `fetched_at`

- `season_top_player_entry`
  - для `topPlayers.*[]`

- `season_top_team_entry`
  - для `topTeams.*[]`

- `season_team_event_bucket`
  - для `tournamentTeamEvents`
  - здесь объект содержит числовые ключи, поэтому лучше хранить:
    - `level_1_key`
    - `level_2_key`
    - `event_id`
    - `unique_tournament_id`
    - `season_id`
    - `scope` (`home/away/total`)

## 4. Что именно видно из отчетов

Ниже реальные envelope keys, которые уже подтверждены локальными report-файлами.

### Турниры и сезоны

- `/unique-tournament/{id}` -> `uniqueTournament`
- `/unique-tournament/{id}/seasons` -> `seasons`
- `/unique-tournament/{id}/featured-events` -> `featuredEvents`
- `/unique-tournament/{id}/media` -> `media`
- `/unique-tournament/{id}/season/{id}/info` -> `info`
- `/unique-tournament/{id}/season/{id}/rounds` -> `currentRound`, `rounds`
- `/unique-tournament/{id}/season/{id}/events/*` -> `events`
- `/unique-tournament/{id}/season/{id}/standings/*` -> `standings`
- `/unique-tournament/{id}/season/{id}/venues` -> `venues`

### Статистика сезона

- `/unique-tournament/{id}/season/{id}/statistics/info` -> `teams`, `statisticsGroups`, `nationalities`
- `/unique-tournament/{id}/season/{id}/statistics?...` -> `results`
- `/unique-tournament/{id}/season/{id}/team-events/*` -> `tournamentTeamEvents`
- `/unique-tournament/{id}/season/{id}/top-players/overall` -> `topPlayers`, `statisticsType`
- `/unique-tournament/{id}/season/{id}/top-players-per-game/all/overall` -> `topPlayers`, `statisticsType`
- `/unique-tournament/{id}/season/{id}/top-teams/overall` -> `topTeams`, `statisticsType`
- `/unique-tournament/{id}/season/{id}/team-of-the-week/periods` -> `periods`
- `/unique-tournament/{id}/season/{id}/team-of-the-week/{id}` -> `players`

### Команды

- `/team/{id}` -> `team`, `pregameForm`
- `/team/{id}/team-statistics/seasons` -> `uniqueTournamentSeasons`, `typesMap`
- `/team/{id}/unique-tournament/{id}/season/{id}/statistics/overall` -> `statistics`

### Игроки

- `/player/{id}` -> `player`
- `/player/{id}/statistics/seasons` -> `uniqueTournamentSeasons`, `typesMap`
- `/player/{id}/unique-tournaments` -> `uniqueTournaments`
- `/player/{id}/transfer-history` -> `transferHistory`
- `/player/{id}/unique-tournament/{id}/season/{id}/statistics/overall` -> `statistics`, `team`
- `/player/{id}/unique-tournament/{id}/season/{id}/heatmap/overall` -> `points`, `events`

### Матчи

- `/event/{id}` -> `event`
- `/event/{id}/incidents` -> `home`, `away`
- `/event/{id}/lineups` -> `home`, `away`
- `/event/{id}/managers` -> `homeManager`, `awayManager`
- `/event/{id}/statistics` -> `statistics`
- `/event/{id}/h2h` -> `teamDuel`, `managerDuel`
- `/event/{id}/pregame-form` -> `homeTeam`, `awayTeam`
- `/event/{id}/provider/{id}/winning-odds` -> `home`, `away`
- `/event/{id}/odds/{id}/all` -> `markets`
- `/event/{id}/odds/{id}/featured` -> `featured`
- `/event/{id}/votes` -> `vote`, `bothTeamsToScoreVote`, `firstTeamToScoreVote`, `whoShouldHaveWonVote`

## 5. Как грузить в базу по шагам

Рекомендованный порядок загрузки:

1. `sport`, `country`, `category`
2. `unique_tournament`
3. `season`
4. `team`, `player`, `manager`, `venue`
5. `tournament`
6. `event`
7. `standing`, `standing_row`
8. `season_stat_snapshot`
9. `season_stat_result`
10. `season_top_player_entry`, `season_top_team_entry`
11. `event_incident`, `event_lineup_player`, `event_manager`, `event_odds_market`, `event_vote`

Почему так:
- `event` уже содержит ссылки на `tournament`, `uniqueTournament`, `season`, `homeTeam`, `awayTeam`
- `statistics/results` содержат `player` и `team`, значит справочники к этому моменту уже должны быть заполнены
- `topPlayers` и `topTeams` опираются на те же `player`, `team`, `statisticsType`

## 6. Что не стоит делать

- Не делай таблицу на каждый endpoint.
- Не делай таблицу на каждый nested object из report’а вроде `event_tournament_category_fieldtranslation`.
- Не используй URL-name как canonical entity name, если JSON уже дал envelope key.
- Не пытайся уложить все `statistics?...` в одну “широкую” таблицу с сотней nullable-колонок.

## 7. Что стоит делать

- Храни raw JSON в `api_response_raw`.
- Храни canonical справочники отдельно.
- Для `statistics?...` делай snapshot + result rows.
- Для нестабильных объектов с динамическими ключами (`typesMap`, `tournamentTeamEvents`) оставляй `jsonb` и раскладывай только нужные срезы.
- Для `topPlayers` и `topTeams` делай отдельные fact tables, потому что это leaderboard, а не общий player/team master.

## 8. Минимальный набор таблиц, если делать без перегруза

Если хочешь стартовать с минимально жизнеспособной схемы, то достаточно:

- `api_response_raw`
- `sport`
- `country`
- `category`
- `unique_tournament`
- `season`
- `tournament`
- `team`
- `player`
- `manager`
- `venue`
- `event`
- `event_incident`
- `event_lineup_player`
- `event_manager`
- `event_odds_market`
- `event_vote`
- `standing`
- `standing_row`
- `season_stat_snapshot`
- `season_stat_result`
- `season_top_player_entry`
- `season_top_team_entry`

Этого уже хватит, чтобы:
- сохранять календарь
- сохранять матчи
- сохранять команды и игроков
- строить таблицы
- строить лидерборды
- хранить сезонную статистику

## 9. Самая важная мысль

Твоя БД должна быть построена не вокруг URL, а вокруг трех типов объектов:

- `dictionary entities`: `team`, `player`, `unique_tournament`, `season`
- `event facts`: матч и все, что живет на `event_id`
- `season analytics facts`: `results`, `topPlayers`, `topTeams`, `tournamentTeamEvents`

То есть главные бизнес-оси такие:

- `event_id` для матча
- `player_id` для игрока
- `team_id` для команды
- `unique_tournament_id + season_id` для сезонной аналитики

Именно так дерево данных уже выглядит по локальным Sofascore reports.
