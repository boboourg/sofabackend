# Гайд для фронтенда: реал-тайм через WebSocket

> Mirror Sofascore WebSocket. Сервис **зеркалит upstream** Sofascore — формат фреймов и payload идентичны их `wss://ws.sofascore.com:9222`. Если у вас уже есть NATS-парсер для Sofascore — он работает у нас без изменений.

## TL;DR

```ts
const ws = new WebSocket("wss://api.var11.com/ws/v1");

ws.onopen = () => {
  ws.send('CONNECT {"verbose":false}\r\n');
  ws.send("SUB sport.football 1\r\n");      // все футбольные live
  ws.send("SUB event.16167494 2\r\n");      // конкретный матч
};

ws.onmessage = (e) => {
  // фреймы: INFO, MSG, PING — см. ниже
  parseFrames(e.data);
};
```

- **Auth:** нет, как у Sofascore. Подключайся и подписывайся.
- **Health:** `https://api.var11.com/ws/health` → JSON.
- **Latency:** ~100 мс от события в Sofascore до пуша клиенту.
- **Payload:** только дельты (изменённые поля), не полный snapshot.

---

## 1. Endpoint

| URL | Назначение |
|---|---|
| `wss://api.var11.com/ws/v1` | WebSocket — реал-тайм апдейты |
| `https://api.var11.com/ws/health` | HTTP GET → JSON, для uptime/мониторинга |

REST-API (`https://api.var11.com/api/v1/...`) остаётся как был — для **первичной загрузки** state.

---

## 2. Протокол

Текстовые фреймы, разделители `\r\n`. Точная копия NATS-сабсета Sofascore.

### Клиент → Сервер

| Команда | Когда | Пример |
|---|---|---|
| `CONNECT {<json>}` | Сразу после `onopen`. Тело игнорируется, но **обязательно** отправь хотя бы пустое — сервер проигнорирует, но это часть NATS handshake. | `CONNECT {"verbose":false}\r\n` |
| `SUB <subject> <sid>` | Подписаться. `<sid>` — твой числовой ID, любой uint, ты получишь его обратно в MSG'ах. | `SUB sport.football 1\r\n` |
| `UNSUB <sid>` | Отписаться. | `UNSUB 1\r\n` |
| `PING` | Хелсчек (опц.) | `PING\r\n` |
| `PONG` | Ответ на серверный PING | `PONG\r\n` |

### Сервер → Клиент

| Команда | Когда | Пример |
|---|---|---|
| `INFO {...}` | **Сразу при подключении.** Метаданные сервера. | `INFO {"server_id":"sofascore-mirror-7b766615","version":"0.1","proto":1,"max_payload":1048576}\r\n` |
| `MSG <subject> <sid> <bytes>\r\n<payload>\r\n` | На каждое обновление. `<bytes>` — длина payload в байтах. `<payload>` — JSON-объект. | `MSG sport.football 1 27\r\n{"id":1234,"homeScore.current":2}\r\n` |
| `PING` | Каждые 20 секунд — отвечай `PONG`. | `PING\r\n` |
| `PONG` | На твой PING | `PONG\r\n` |
| `-ERR '<reason>'` | Если SUB на невалидный subject | `-ERR 'Permission Violated for Subject foo.bar'\r\n` |

### Парсер фреймов

Простой буфер + поиск `\r\n` + спец-обработка MSG (читать payload по длине). [Референс-парсер](../schema_inspector/ws_nats_parser.py) есть в репо (Python, ~70 строк) — портируется на TS за полчаса.

```ts
function parseFrames(buffer: string): { frames: Frame[]; leftover: string } {
  const frames: Frame[] = [];
  while (true) {
    const eol = buffer.indexOf("\r\n");
    if (eol === -1) return { frames, leftover: buffer };
    const header = buffer.slice(0, eol);

    if (header.startsWith("MSG ")) {
      const [_, subject, sidStr, bytesStr] = header.split(" ");
      const payloadLen = parseInt(bytesStr, 10);
      const payloadStart = eol + 2;
      const payloadEnd = payloadStart + payloadLen;
      if (buffer.length < payloadEnd + 2) return { frames, leftover: buffer };
      const payload = buffer.slice(payloadStart, payloadEnd);
      frames.push({
        kind: "MSG",
        subject,
        sid: parseInt(sidStr, 10),
        payload: JSON.parse(payload),
      });
      buffer = buffer.slice(payloadEnd + 2);
    } else if (header.startsWith("PING")) {
      frames.push({ kind: "PING" });
      buffer = buffer.slice(eol + 2);
    } else if (header.startsWith("INFO ")) {
      frames.push({ kind: "INFO", info: JSON.parse(header.slice(5)) });
      buffer = buffer.slice(eol + 2);
    } else if (header.startsWith("PONG")) {
      frames.push({ kind: "PONG" });
      buffer = buffer.slice(eol + 2);
    } else if (header.startsWith("-ERR")) {
      frames.push({ kind: "ERR", reason: header });
      buffer = buffer.slice(eol + 2);
    } else {
      buffer = buffer.slice(eol + 2); // unknown, skip
    }
  }
}
```

---

## 3. Subjects (что на что подписываться)

| Subject | Когда подписываться | Что приходит |
|---|---|---|
| `sport.football` | Главный live-экран по футболу | Все event-дельты для всех live football-матчей |
| `sport.tennis` / `sport.basketball` / `sport.{slug}` | Аналогично для других видов | То же самое |
| `event.{event_id}` | Открыт Match-экран | event + odds дельты по этому матчу |
| `odds.football.1` | Виджет коэффициентов | Все odds-дельты для football, market_id=1 (Full-time 1X2) |
| `odds.{sport}.1` | Аналогично для других видов спорта | То же |

### Все sport-слаги

```
football, basketball, tennis, table-tennis, volleyball,
handball, ice-hockey, baseball, american-football,
rugby, cricket, futsal, esports
```

### Когда какой subject выбрать

| Экран | Subject | Почему |
|---|---|---|
| `Live/Main` | `sport.football` (+ другие при переключении) | Получаешь все live-матчи спорта одним подписом |
| `Games/Game` (открытый матч) | `event.{event_id}` | Только этот матч; меньше трафика чем `sport.*` |
| `Match → Series/Odds widget` | `odds.{sport}.1` ИЛИ оставайся на `event.{id}` (он получает и odds) | `event.{id}` достаточно — odds приходят туда же |
| `Favorites/Main` (несколько матчей в избранном) | `event.{id1}`, `event.{id2}`, ... несколько SUB на одном сокете | Один WS — несколько подписок |

**Один сокет — много SUB.** Открой одно соединение и подпишись на несколько subjects с разными `sid`. Не открывай новый сокет на каждый виджет.

---

## 4. Формат payload (дельты)

**Это всегда дельта, не полный объект.** Только изменённые поля. Полный state клиент держит у себя (из REST) и применяет дельты сверху.

### Event-type (subject = `sport.{slug}` или `event.{id}`)

Все возможные ключи в payload (40 полей, dot-notation):

```ts
type EventDelta = {
  id: number;                          // event_id — всегда

  // Версионирование
  "changes.changeTimestamp"?: number;  // unix sec; возрастает при каждом изменении

  // Счёт (home/away)
  "homeScore.current"?: number;        // текущий счёт
  "homeScore.display"?: number;        // что показывать (= current для футбола, для тенниса может отличаться)
  "homeScore.period1"?: number;        // по периодам / сетам
  "homeScore.period2"?: number;
  "homeScore.period3"?: number;
  "homeScore.period4"?: number;
  "homeScore.period5"?: number;        // tt/волейбол, у нас не хранится но в WS приходит
  "homeScore.normaltime"?: number;     // основное время (без extra/penalties)
  "homeScore.overtime"?: number;
  "homeScore.penalties"?: number;
  "homeScore.extra1"?: number;
  "homeScore.extra2"?: number;
  "homeScore.aggregated"?: number;     // сумма по 2 матчам в плейофф
  "homeScore.series"?: number;
  "homeScore.point"?: string;          // теннис: "0"/"15"/"30"/"40"/"AD"
  // ...то же самое для awayScore.*
  "awayScore.current"?: number;
  // ...

  // Статус матча
  "status.code"?: number;              // 100=Ended, 6=1st half, 31=Halftime, 7=2nd half...
  "status.description"?: string;       // "1st half", "Ended"
  "status.type"?: string;              // "inprogress" | "finished" | "notstarted" | ...
  statusDescription?: string;          // label вроде "46" для live минуты
  lastPeriod?: string | null;          // "period2", "overtime"
  winnerCode?: number;                 // 1=home, 2=away, 3=draw (после окончания)

  // Тайминг
  "time.played"?: number;              // секунд с начала периода
  "time.playedLastUpdated"?:           // когда tick посчитан
    | { date: string; timezone: string; timezone_type: number }
    | null;
  "time.clockRunning"?: boolean;
  "time.clockRunningLastUpdated"?: {...};
  "time.currentPeriodStartTimestamp"?: number;  // unix sec — старт периода
  "time.initial"?: number;             // длина матча/периода
  "time.max"?: number;
  "time.extra"?: number;               // injury time
  "time.periodLength"?: number;
  "time.totalPeriodCount"?: number;
  "time.overtimeLength"?: number;
  "time.injuryTime1"?: number;
  // ...
  currentPeriodStartTimestamp?: number;  // top-level дубль time.currentPeriodStartTimestamp
  statusTime?: {                        // структурный блок (вместо плоского time.*)
    prefix: string;
    timestamp: number;
    initial: number;
    max: number;
    extra: number;
  };

  // Прочее
  firstToServe?: 1 | 2;                // теннис
  cardsCode?: string;                  // "00", "01", "10" — кол-во красных карт (home|away)
  varInProgress?: {                    // футбол: VAR-проверка идёт
    homeTeam: boolean;
    awayTeam: boolean;
  };
  // (eventState.* приходит, но мы дропаем — derived от varInProgress)
};
```

**Как применять:**

```ts
function applyDelta(state: EventState, delta: EventDelta): EventState {
  const next = { ...state };
  for (const [key, value] of Object.entries(delta)) {
    if (key === "id") continue;
    // dot-path в nested структуру
    if (key.startsWith("homeScore.")) {
      next.homeScore = { ...next.homeScore, [key.slice(10)]: value };
    } else if (key.startsWith("awayScore.")) {
      next.awayScore = { ...next.awayScore, [key.slice(10)]: value };
    } else if (key.startsWith("status.")) {
      next.status = { ...next.status, [key.slice(7)]: value };
    } else if (key.startsWith("time.")) {
      next.time = { ...next.time, [key.slice(5)]: value };
    } else if (key === "statusTime") {
      next.statusTime = value as EventState["statusTime"];
    } else {
      // top-level: winnerCode, lastPeriod, statusDescription, ...
      (next as any)[key] = value;
    }
  }
  return next;
}
```

### Odds-type (subject = `odds.{sport}.1`)

Только market_id=1 = "Full-time" (1X2 для футбола / Home-Away для тенниса/баскетбола).

```ts
type OddsDelta = {
  id: number;                                // event_market.id (offer id)
  "choice1.fractionalValue"?: string;       // "5/6", "13/8" — текущий коэф
  "choice2.fractionalValue"?: string;
  "choice3.fractionalValue"?: string;       // только для 1X2 (footbol, hockey)
  "choice1.initialFractionalValue"?: string; // начальный коэф (для движения линии)
  "choice2.initialFractionalValue"?: string;
  "choice3.initialFractionalValue"?: string;
};
```

**Маппинг choice → name** (через market_group):

| `market_group` (из REST `/event/{id}/odds/1/all`) | choice1 | choice2 | choice3 |
|---|---|---|---|
| `1X2` (football, hockey, handball, rugby, cricket, futsal) | `1` (home) | `X` (draw) | `2` (away) |
| `Home/Away` (tennis, basketball, baseball, table-tennis, volleyball, am.football, esports) | `1` (home) | `2` (away) | — |

Чтобы понять market_group для конкретного offer_id — посмотри в payload REST-запроса `/api/v1/event/{event_id}/odds/1/all`. Mapping offer_id → event_id → choice_name стабилен на время матча.

---

## 5. Жизненный цикл соединения

```
1. WebSocket open
2. Server → INFO {server_id, version, proto, max_payload}     ← парси, можно логировать
3. Client → CONNECT {}                                          ← обязательно отправь
4. Client → SUB <subject> <sid> (одно или несколько)            ← подпишись на нужное
5. Server → MSG ...                                              ← обрабатывай
   Server → PING                                                 ← отвечай PONG
   ...
6. Client → UNSUB <sid> (опц., при смене экрана)
7. WebSocket close (нормально или из-за ошибки сети)
```

### Heartbeat

Сервер шлёт `PING` каждые **20 секунд**. **Отвечай `PONG\r\n`** или соединение будет закрыто через ~30 сек.

Это нужно делать всегда автоматически — не передавай PING в state-машину приложения:

```ts
ws.onmessage = (e) => {
  const { frames } = parseFrames(buffer + e.data);
  for (const f of frames) {
    if (f.kind === "PING") {
      ws.send("PONG\r\n");
      continue;
    }
    if (f.kind === "MSG") {
      // ... передавай в store
    }
  }
};
```

### Reconnect strategy (fire-and-forget)

**Сервер не хранит историю**. Если потерял соединение:

1. Закрой сокет
2. **Перезагрузи свежий state через REST** (`GET /api/v1/event/{id}` или `/api/v1/sport/football/events/live`)
3. Открой новый WS, сделай SUB заново
4. Применяй дельты дальше

Exponential backoff между попытками: `1s, 2s, 5s, 10s, 30s` (не короче 1с, не дольше 30с).

```ts
async function connectWithRetry(): Promise<WebSocket> {
  let delay = 1000;
  while (true) {
    try {
      const ws = await openSocket();
      delay = 1000; // сброс
      return ws;
    } catch (e) {
      await sleep(delay);
      delay = Math.min(delay * 2, 30000);
    }
  }
}
```

---

## 6. Полный пример — React Native + TanStack Query

```ts
// src/shared/realtime/MirrorWS.ts
type Frame =
  | { kind: "INFO"; info: any }
  | { kind: "MSG"; subject: string; sid: number; payload: any }
  | { kind: "PING" }
  | { kind: "PONG" }
  | { kind: "ERR"; reason: string };

export class MirrorWS {
  private ws: WebSocket | null = null;
  private buffer = "";
  private subs = new Map<number, string>();      // sid → subject
  private subjectToSid = new Map<string, number>();
  private nextSid = 1;
  private listeners = new Map<string, Set<(p: any) => void>>();
  private retryDelay = 1000;

  connect() {
    this.ws = new WebSocket("wss://api.var11.com/ws/v1");
    this.ws.onopen = () => {
      this.retryDelay = 1000;
      this.ws!.send('CONNECT {"verbose":false}\r\n');
      // Восстанавливаем подписки после reconnect
      for (const [sid, subject] of this.subs) {
        this.ws!.send(`SUB ${subject} ${sid}\r\n`);
      }
    };
    this.ws.onmessage = (e) => this.onChunk(typeof e.data === "string" ? e.data : "");
    this.ws.onclose = () => this.scheduleReconnect();
    this.ws.onerror = () => this.ws?.close();
  }

  subscribe(subject: string, onMessage: (payload: any) => void): () => void {
    if (!this.listeners.has(subject)) this.listeners.set(subject, new Set());
    this.listeners.get(subject)!.add(onMessage);

    if (!this.subjectToSid.has(subject)) {
      const sid = this.nextSid++;
      this.subs.set(sid, subject);
      this.subjectToSid.set(subject, sid);
      if (this.ws?.readyState === WebSocket.OPEN) {
        this.ws.send(`SUB ${subject} ${sid}\r\n`);
      }
    }

    return () => {
      const set = this.listeners.get(subject);
      set?.delete(onMessage);
      if (set?.size === 0) {
        const sid = this.subjectToSid.get(subject);
        if (sid != null) {
          this.ws?.send(`UNSUB ${sid}\r\n`);
          this.subs.delete(sid);
          this.subjectToSid.delete(subject);
        }
      }
    };
  }

  private onChunk(chunk: string) {
    this.buffer += chunk;
    const { frames, leftover } = parseFrames(this.buffer);
    this.buffer = leftover;
    for (const f of frames) {
      if (f.kind === "PING") {
        this.ws?.send("PONG\r\n");
      } else if (f.kind === "MSG") {
        const set = this.listeners.get(f.subject);
        set?.forEach((cb) => cb(f.payload));
      }
    }
  }

  private scheduleReconnect() {
    setTimeout(() => this.connect(), this.retryDelay);
    this.retryDelay = Math.min(this.retryDelay * 2, 30000);
  }
}

// Singleton — один сокет на всё приложение
export const realtime = new MirrorWS();
realtime.connect();
```

```ts
// src/widgets/games/Match/useLiveMatch.ts
import { useQueryClient } from "@tanstack/react-query";
import { useEffect } from "react";
import { realtime } from "@/shared/realtime/MirrorWS";

export function useLiveMatch(eventId: number) {
  const qc = useQueryClient();

  useEffect(() => {
    const subject = `event.${eventId}`;
    const unsub = realtime.subscribe(subject, (delta) => {
      // Применяем дельту к кэшу TanStack
      qc.setQueryData(["event", eventId], (prev: any) => {
        if (!prev) return prev;  // полный state ещё не загружен из REST
        return { ...prev, event: applyDelta(prev.event, delta) };
      });
    });
    return unsub;
  }, [eventId, qc]);
}
```

```ts
// src/screens/GamesGame/index.tsx (упрощённо)
import { useQuery } from "@tanstack/react-query";
import { useLiveMatch } from "@/widgets/games/Match/useLiveMatch";

function GamesGame({ eventId }: { eventId: number }) {
  // Первичная загрузка — REST
  const { data } = useQuery({
    queryKey: ["event", eventId],
    queryFn: () => fetch(`/api/v1/event/${eventId}`).then((r) => r.json()),
  });

  // Real-time дельты — WS
  useLiveMatch(eventId);

  return <MatchHeader event={data?.event} />;
}
```

---

## 7. Status codes (handy reference)

Самые частые `status.code`:

| Code | Description | Type |
|---|---|---|
| 0 | Not started | notstarted |
| 6 | 1st half (football) | inprogress |
| 7 | 2nd half (football) | inprogress |
| 31 | Halftime | inprogress |
| 8 | 1st set (tennis/volleyball/tt) | inprogress |
| 9 | 2nd set | inprogress |
| 10 | 3rd set | inprogress |
| 11 | 4th set | inprogress |
| 12 | 5th set | inprogress |
| 13 | 1st quarter (basketball) | inprogress |
| 14 | 2nd quarter | inprogress |
| 15 | 3rd quarter | inprogress |
| 16 | 4th quarter | inprogress |
| 20 | Started (generic) | inprogress |
| 30 | Pause | inprogress |
| 60 | Postponed | postponed |
| 70 | Canceled | cancelled |
| 91 | Walkover | finished |
| 100 | Ended | finished |
| 110 | AET (after extra time) | finished |
| 120 | AP (after penalties) | finished |

Полный набор есть в БД (REST `/api/v1/event/{id}` отдаёт `status.type` явно — на нём ориентируйся в UI).

---

## 8. Health endpoint

```bash
curl https://api.var11.com/ws/health
```

```json
{
  "status": "ok",                // "ok" | "degraded"
  "redis": "ok",                 // "ok" | "down"
  "connections": 42,             // live WS-клиентов
  "subscriptions": 87,           // сумма SUB по всем клиентам
  "fanout_channels": [           // что сейчас слушает сервер
    "ws:fanout:sport:football"
  ],
  "uptime_seconds": 12345.6,
  "messages_sent": 9876543,
  "version": "0.1",
  "server_id": "sofascore-mirror-7b766615"
}
```

- HTTP 200 всегда — алерт по полю `status === "degraded"` или `redis === "down"`.
- Нет авторизации, нет rate limit.
- Не используй для healthcheck клиентских девайсов — это server-side метрика.

---

## 9. Известные ограничения

| Что | Поведение | Workaround |
|---|---|---|
| Reconnect | Без буфера — теряешь сообщения за время разрыва | Refetch REST → resubscribe |
| `period5` (5+ сетов в TT/волейбол) | В WS приходит, но не сохраняется у нас в БД | На клиенте применять как обычное поле |
| `time.played` обновляется часто (~1 в сек) | Может спамить UI | Throttle на клиенте до 1 в 5с для timeline-минут |
| Odds приходят только для `marketId=1` | Other markets (Asian handicap, Over/Under) идут только через REST polling | Не SUB на них в WS |
| `eventState.timestamp` / `eventState.statusIndicator` | Приходят, но мы дропаем как derived | Используй `varInProgress` напрямую |
| Slow client | Сервер дропает сообщения если очередь клиента заполнилась (>1024 frames) | На стороне клиента не блокировать `onmessage`, парсить в otherTick |

---

## 10. Чек-лист интеграции

- [ ] Один singleton-сокет на всё приложение (не открывать по WS на каждый виджет)
- [ ] Парсер фреймов с буферизацией partial chunks
- [ ] Автоматический ответ на серверный `PING` → `PONG`
- [ ] Exponential backoff на reconnect (1s → 30s)
- [ ] Re-SUB на все активные subjects после reconnect
- [ ] Применение дельт к локальному state поверх REST snapshot (не вместо)
- [ ] Throttle для `time.played` ticks
- [ ] Логирование `-ERR` ответов (если SUB на левый subject)
- [ ] Healthcheck endpoint в мониторинге (не на стороне клиента)
- [ ] При смене экрана: `UNSUB <sid>` чтобы не получать ненужное

---

## 11. Сравнение с polling

| | Polling (текущий) | WebSocket |
|---|---|---|
| Latency от события до UI | 2-3 секунды (avg poll cycle) | 100-200 мс |
| Bandwidth | Полный payload каждые 5с (даже без изменений) | Только дельты (200-500 байт/msg) |
| Server load | 720K req/min на 12K юзеров | 12K connections, ~10 msg/min push |
| 1:1 sync счёта между экранами | Может быть рассинхрон до 5с | Мгновенно |
| Battery (mobile) | Каждые 5с radio wakeup | Один TCP socket open, push-driven |

**Polling оставляем как fallback** на случай если WS недоступен в сети клиента.

---

## 12. Контакты + ссылки

- Сорсы: `https://github.com/boboourg/sofabackend` (репо `sofascore-prod`)
- Серверный код: [schema_inspector/services/ws_server_service.py](../schema_inspector/services/ws_server_service.py)
- NATS-парсер (Python референс): [schema_inspector/ws_nats_parser.py](../schema_inspector/ws_nats_parser.py)
- Список маппированных полей: [schema_inspector/ws_delta_normalizer.py](../schema_inspector/ws_delta_normalizer.py)
- Тесты с примерами payload: [tests/test_ws_delta_normalizer.py](../tests/test_ws_delta_normalizer.py)

При проблемах: проверь `https://api.var11.com/ws/health` → если `degraded`, проблема на нашей стороне.
