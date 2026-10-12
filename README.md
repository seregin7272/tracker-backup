# Tracker Backup

Бекап Яндекс Трекера: задачи со всеми полями, комментарии, история изменений, связи и вложения,
плюс метаданные — описания глобальных и локальных полей, настройки очередей, справочники.

## Что сохраняется

```
<out>/
  _meta/{fields,statuses,priorities,issuetypes,resolutions,linktypes}.json
  queues.json, state.json, errors.json
  <QUEUE>/
    _queue/{queue,localFields}.json
    <KEY>/
      issue.json         # задача со значениями всех полей, в т.ч. локальных
      comments.json
      changelog.json
      links.json
      attachments.json
      attachments/<id>_<name>
```

## Запуск

```bash
python3 -m venv .venv && .venv/bin/pip install -r requirements.txt

export TRACKER_ORG_ID=1234567          # Яндекс 360 -> X-Org-ID
# или TRACKER_CLOUD_ORG_ID=bpf...       # Cloud Organization -> X-Cloud-Org-ID

export TRACKER_TOKENS=y0_aaa,y0_bbb    # пул токенов
# или --tokens-file tokens.txt (по токену в строке, # — комментарий)
# или один токен: TRACKER_TOKEN=y0_...

.venv/bin/python tracker_backup.py --out ./backup                    # все очереди, инкрементально
.venv/bin/python tracker_backup.py --out ./backup --queues DEV,OPS
.venv/bin/python tracker_backup.py --out ./backup --full --no-files  # заново, без вложений
```

| Параметр | По умолчанию | |
|---|---|---|
| `--rps` / `TRACKER_RPS` | 5 | лимит запросов в секунду на один токен, `0` — без лимита |
| `--workers` | 4 на токен, максимум 32 | число параллельных потоков |
| `--tokens-file` | — | файл с токенами |
| `TRACKER_AUTH` | `OAuth` | схема авторизации по умолчанию; для IAM — `Bearer`. Можно указать в самом токене: `Bearer t1.xxx` |
| `-v` | — | debug-лог, в т.ч. каждый 429 |

## Пул токенов и rate limit

- Каждый запрос получает токен, который освободится раньше остальных. Для каждого токена
  соблюдается свой интервал `1/rps`, так что суммарная скорость ≈ `rps × число токенов`.
- На `429` токен уходит на паузу: `Retry-After` → `X-RateLimit-Reset` → экспоненциальный backoff
  с джиттером (до 60 с). Запрос сразу повторяется другим свободным токеном.
- При `X-RateLimit-Remaining: 0` токен заранее ставится на паузу до `X-RateLimit-Reset`.
- Токен, получивший `401`, исключается из пула. Если отозваны все токены, бекап останавливается.
- `5xx` и сетевые ошибки повторяются с backoff.
- Очередь обрабатывается в два этапа: сначала scroll-поиск быстро проходит все задачи и сохраняет
  `issue.json`, потом комментарии, история, связи и вложения качаются параллельно. Так scroll-контекст
  не истекает, пока идёт медленная выгрузка.
- Scroll ведётся одним токеном. Продолжения scroll не повторяются на таймаутах и 5xx (можно молча
  пропустить страницу), вместо этого поиск начинается заново — до 3 раз. Так же — если токен scroll отозван.
- Пауза токена ограничена 5 минутами, что бы ни прислал сервер; `X-RateLimit-Reset` понимается
  в секундах, unix-времени в секундах или миллисекундах.

Токены должны принадлежать пользователям (лучше роботам) с одинаковым доступом к очередям, иначе
задача, найденная одним токеном, может вернуть 403 на другом.

В конце прогона в лог выводится статистика по токенам: число запросов, 429 и отозван ли токен.

## Инкрементальность

`state.json` хранит для каждой очереди время последнего успешного прогона (минус 5 минут перекрытия).
Следующий запуск берёт только задачи с `updated >=` этого времени. Если в очереди были ошибки, время
не сдвигается и при следующем запуске задачи выгрузятся повторно.

`--full` выгружает выбранные очереди целиком, но не стирает отметки остальных очередей в `state.json`.

Все ошибки за прогон (задачи, поиск, метаданные) пишутся в `errors.json`, в том числе при аварийной
остановке. Код выхода: 0 — без ошибок, 1 — есть ошибки, 2 — остановка (все токены отозваны или не
удалось получить список очередей), 130 — прервано по Ctrl+C.

## Ежедневный запуск по cron

Cron запускает команды с почти пустым окружением, поэтому переменные лучше вынести в отдельный файл,
а токены — в `--tokens-file`. Оба файла должны быть доступны только владельцу (`chmod 600`).

```bash
# /etc/tracker-backup.env
TRACKER_ORG_ID=1234567
TRACKER_RPS=5
```

```bash
# /etc/tracker-backup.tokens — по токену в строке
y0_aaa
y0_bbb
```

`crontab -e` (пример: каждый день в 03:30, проект в `/opt/tracker-backup`):

```cron
30 3 * * * cd /opt/tracker-backup && set -a && . /etc/tracker-backup.env && set +a && flock -n /tmp/tracker-backup.lock .venv/bin/python tracker_backup.py --out /backup/tracker --tokens-file /etc/tracker-backup.tokens >> /var/log/tracker-backup.log 2>&1
```

- `set -a` + `.` экспортирует все переменные из env-файла в окружение скрипта.
- `flock -n` не даёт запустить второй бекап, пока не закончился предыдущий: первый полный прогон
  может идти дольше суток. `flock` есть в Linux (util-linux), в macOS его нужно ставить отдельно
  (`brew install flock`).
- Лог дописывается в `/var/log/tracker-backup.log` — у пользователя cron должны быть права на запись,
  ротацию настройте через `logrotate`.
- Код выхода 1 или 2 означает ошибки (см. «Инкрементальность»). Скрипт пишет лог в stderr при каждом
  прогоне, поэтому для алертов проверяйте непустой `errors.json` или свежесть `state.json` мониторингом.

## Запуск в Docker

```bash
docker build -t tracker-backup .
```

Бекап пишется в `/backup` внутри контейнера, токены передаются файлом только для чтения,
остальные переменные — через `--env-file` (формат тот же, что у `/etc/tracker-backup.env` выше):

```bash
docker run --rm \
  --env-file /etc/tracker-backup.env \
  -v /etc/tracker-backup.tokens:/run/secrets/tokens:ro \
  -v /backup/tracker:/backup \
  tracker-backup --out /backup --tokens-file /run/secrets/tokens
```

Аргументы после имени образа передаются скрипту как есть (`--queues`, `--full`, `--no-files`, `-v` …).

- Процесс в контейнере работает от пользователя с UID 10001: каталог бекапа на хосте должен быть
  доступен ему на запись (`chown 10001 /backup/tracker`), а файл токенов — на чтение.
- `docker stop` останавливает бекап корректно: скрипт обрабатывает SIGTERM так же, как Ctrl+C —
  пишет `errors.json`, не сдвигает `state.json` для прерванной очереди и выходит с кодом 130.
  По умолчанию Docker ждёт 10 секунд; если идёт загрузка крупных вложений, увеличьте `docker stop -t`.
- Время в логах — UTC; для местного времени добавьте `-e TZ=Europe/Moscow`.

Ежедневный запуск через cron на хосте:

```cron
30 3 * * * flock -n /tmp/tracker-backup.lock docker run --rm --env-file /etc/tracker-backup.env -v /etc/tracker-backup.tokens:/run/secrets/tokens:ro -v /backup/tracker:/backup tracker-backup --out /backup --tokens-file /run/secrets/tokens >> /var/log/tracker-backup.log 2>&1
```
