#!/usr/bin/env python3
"""
Бекап Яндекс Трекера: issues (со всеми полями) + comments + changelog + links + attachments
и метаданные: описания глобальных полей, локальные поля и настройки очередей, справочники.

Структура:
  <out>/_meta/{fields,statuses,priorities,issuetypes,resolutions,linktypes}.json
  <out>/<QUEUE>/_queue/{queue,localFields}.json
  <out>/<QUEUE>/<KEY>/{issue,comments,changelog,links,attachments}.json + attachments/
Инкрементально: state.json хранит время последнего успешного прогона очереди,
следующий запуск берёт updated >= last (с небольшим перекрытием).

Очередь обрабатывается в два этапа: сначала scroll-поиск быстро проходит все задачи
и сохраняет issue.json, затем комментарии, историю, связи и вложения качают потоки.
Так scroll-контекст не успевает истечь, пока идёт медленная выгрузка.

Пул токенов: запросы распределяются между токенами, у каждого свой лимит RPS.
На 429 токен уходит в cooldown (Retry-After / X-RateLimit-Reset / экспоненциально),
запрос повторяется другим свободным токеном. Токен с 401 исключается из пула.

  pip install -r requirements.txt
  export TRACKER_TOKENS=y0_aaa,y0_bbb   # или --tokens-file tokens.txt (по одному в строке)
  # один токен: export TRACKER_TOKEN=y0_...
  # IAM: TRACKER_AUTH=Bearer, либо префикс в самом токене: "Bearer t1.xxx"
  export TRACKER_ORG_ID=1234567         # Яндекс 360 -> X-Org-ID
  # или TRACKER_CLOUD_ORG_ID=bpf...      # Cloud Organization -> X-Cloud-Org-ID
  python tracker_backup.py --out ./backup --queues DEV,OPS --rps 5
  python tracker_backup.py --out ./backup --full   # игнорировать state
"""
from __future__ import annotations

import argparse
import json
import logging
import os
import random
import re
import signal
import sys
import threading
import time
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timedelta, timezone
from email.utils import parsedate_to_datetime
from pathlib import Path

import requests

API = os.getenv("TRACKER_API", "https://api.tracker.yandex.net/v3")
RETRY_STATUSES = {500, 502, 503, 504}
STATE_OVERLAP = timedelta(minutes=5)
MAX_PAUSE = 300.0      # верхняя граница паузы токена, что бы ни прислал сервер
SCROLL_RESTARTS = 3    # сколько раз начинать поиск заново, если scroll оборвался

log = logging.getLogger("tracker-backup")


# ---------------------------------------------------------------- token pool

class NoTokensLeft(RuntimeError):
    """Все токены пула отозваны — продолжать бекап нечем."""


class PinnedTokenDead(RuntimeError):
    """Отозван токен, к которому привязан scroll; остальные токены могут быть живы."""


class Token:
    def __init__(self, raw: str, idx: int, default_scheme: str):
        scheme, _, value = raw.strip().partition(" ")
        if not value:
            scheme, value = default_scheme, scheme
        self.auth = f"{scheme} {value.strip()}"
        self.name = f"#{idx}(…{value.strip()[-4:]})"
        self.next_slot = 0.0       # monotonic: раньше этого момента RPS-лимит не пускает
        self.cooldown_until = 0.0  # monotonic: токен на паузе после 429 / исчерпания лимита
        self.strikes = 0           # подряд идущие 429, для экспоненциального backoff
        self.dead = False
        self.requests = 0
        self.throttled = 0

    def ready_at(self) -> float:
        return max(self.next_slot, self.cooldown_until)


def _seconds_header(r: requests.Response, name: str) -> float | None:
    """Retry-After / X-RateLimit-Reset: секунды, unix-время (с или мс) или HTTP-дата."""
    v = r.headers.get(name)
    if not v:
        return None
    try:
        x = float(v)
    except ValueError:
        try:
            x = parsedate_to_datetime(v).timestamp()
        except (TypeError, ValueError):
            return None
    if x > 1e12:
        x /= 1000
    if x > 1e9:
        x -= time.time()
    return max(0.0, x)


def _clamp_pause(pause: float) -> float:
    return min(MAX_PAUSE, max(0.5, pause))


class TokenPool:
    def __init__(self, tokens: list[Token], rps: float):
        if not tokens:
            raise NoTokensLeft("не задано ни одного токена")
        self.tokens = tokens
        self.interval = 1.0 / rps if rps > 0 else 0.0
        self.lock = threading.Lock()

    def acquire(self, pin: Token | None = None) -> Token:
        """Выбирает токен, который освободится раньше всех, резервирует слот и ждёт его."""
        while True:
            with self.lock:
                if pin and pin.dead:
                    raise PinnedTokenDead(f"токен {pin.name} отозван (401)")
                cands = [pin] if pin else [t for t in self.tokens if not t.dead]
                if not cands:
                    raise NoTokensLeft("все токены отозваны (401)")
                now = time.monotonic()
                tok = min(cands, key=lambda t: (t.ready_at(), t.requests))
                start = max(now, tok.ready_at())
                tok.next_slot = start + self.interval
            delay = start - now
            if delay > 5:
                log.info("все токены заняты, ждём %.1fs (%s)", delay, tok.name)
            if delay > 0:
                time.sleep(delay)
            with self.lock:
                # пока спали, токен мог словить 429 или 401 в другом потоке
                if tok.dead or tok.cooldown_until > time.monotonic():
                    continue
                tok.requests += 1
                return tok

    def report(self, tok: Token, r: requests.Response):
        now = time.monotonic()
        with self.lock:
            if r.status_code == 429:
                tok.throttled += 1
                tok.strikes += 1
                pause = _seconds_header(r, "Retry-After")
                if pause is None:
                    pause = _seconds_header(r, "X-RateLimit-Reset")
                if pause is None:
                    pause = 2.0 ** tok.strikes * random.uniform(0.75, 1.25)
                pause = _clamp_pause(pause)
                tok.cooldown_until = max(tok.cooldown_until, now + pause)
                log.debug("429 на токене %s, пауза %.1fs", tok.name, pause)
            elif r.status_code == 401:
                if not tok.dead:
                    log.warning("токен %s получил 401 и исключён из пула", tok.name)
                tok.dead = True
            else:
                tok.strikes = 0
                if r.headers.get("X-RateLimit-Remaining") == "0":
                    pause = _seconds_header(r, "X-RateLimit-Reset")
                    pause = _clamp_pause(1.0 if pause is None else pause)
                    tok.cooldown_until = max(tok.cooldown_until, now + pause)

    def stats(self) -> str:
        return ", ".join(
            f"{t.name}: {t.requests} req, {t.throttled}x429{' DEAD' if t.dead else ''}" for t in self.tokens
        )


# ---------------------------------------------------------------- http client

class Tracker:
    def __init__(self, pool: TokenPool, org_headers: dict, attempts: int = 8, max_throttles: int = 100):
        self.pool = pool
        self.org_headers = org_headers
        self.attempts = attempts
        self.max_throttles = max_throttles
        self._local = threading.local()

    def _session(self) -> requests.Session:
        s = getattr(self._local, "s", None)
        if s is None:
            s = self._local.s = requests.Session()
            s.headers.update(self.org_headers)
        return s

    def request(self, method: str, url: str, pin: Token | None = None, retry: bool = True,
                **kw) -> requests.Response:
        """Запрос через пул. В r.token кладётся токен, которым запрос выполнен.

        retry=False — для неидемпотентных запросов: таймауты и 5xx не повторяются
        (сервер мог успеть обработать запрос), повторяется только 429.
        """
        errors = throttles = 0
        attempts = self.attempts if retry else 1
        while True:
            tok = self.pool.acquire(pin)
            try:
                r = self._session().request(method, url, headers={"Authorization": tok.auth}, timeout=60, **kw)
            except (requests.ConnectionError, requests.Timeout):
                errors += 1
                if errors >= attempts:
                    raise
                time.sleep(min(30.0, 2.0 ** errors))
                continue
            self.pool.report(tok, r)
            r.token = tok
            if r.status_code == 429 and throttles < self.max_throttles:
                throttles += 1
                r.close()
                continue
            if r.status_code == 401 and pin is None:
                r.close()
                continue
            if r.status_code in RETRY_STATUSES and errors + 1 < attempts:
                errors += 1
                r.close()
                time.sleep(min(30.0, 2.0 ** errors))
                continue
            r.raise_for_status()
            return r

    def get(self, url: str, **kw) -> requests.Response:
        return self.request("GET", url, **kw)


def scroll(tr: Tracker, body: dict):
    """Scroll-поиск (обходит лимит 10k). Scroll привязан к токену первого запроса."""
    params = {"scrollType": "sorted", "perScroll": 1000, "scrollTTLMillis": 60000}
    r = tr.request("POST", f"{API}/issues/_search", params=params, json=body)
    pin = r.token
    while True:
        page = r.json()
        yield from page
        sid = r.headers.get("X-Scroll-Id")
        if not page or not sid:
            return
        # повтор продолжения с тем же scrollId может молча пропустить страницу
        r = tr.request("POST", f"{API}/issues/_search", pin=pin, retry=False,
                       params={"scrollId": sid, "scrollTTLMillis": 60000}, json=body)


def search_keys(tr: Tracker, body: dict, on_issue) -> list[str]:
    """Проходит scroll до конца, вызывая on_issue на каждой задаче; при обрыве начинает заново."""
    for attempt in range(SCROLL_RESTARTS + 1):
        keys: dict[str, None] = {}
        try:
            for issue in scroll(tr, body):
                on_issue(issue)
                keys[issue["key"]] = None
            return list(keys)
        except (requests.RequestException, ValueError, PinnedTokenDead) as e:
            if attempt == SCROLL_RESTARTS:
                raise
            log.warning("scroll оборвался после %d задач (%s), поиск заново", len(keys), e)


def paged(tr: Tracker, url: str) -> list:
    """Пагинация comments/changelog через id последнего элемента — до пустой страницы.

    Короткая страница не значит, что она последняя: сервер может урезать perPage.
    Если сервер прислал Link без rel=next — страниц больше нет, лишний запрос не делаем.
    """
    out, last = [], None
    while True:
        params = {"perPage": 100, "expand": "all"}
        if last:
            params["id"] = last
        r = tr.get(url, params=params)
        page = r.json()
        out += page
        if not page or ("Link" in r.headers and "next" not in r.links):
            return out
        last = page[-1]["id"]


def pages(tr: Tracker, url: str, **params) -> list:
    """Постраничная пагинация через page + X-Total-Pages."""
    out, n = [], 1
    while True:
        r = tr.get(url, params={**params, "perPage": 100, "page": n})
        data = r.json()
        out += data
        if not data or n >= int(r.headers.get("X-Total-Pages", 1)):
            return out
        n += 1


# ---------------------------------------------------------------- backup

def dump(p: Path, data):
    tmp = p.with_name(p.name + ".tmp")
    tmp.write_text(json.dumps(data, ensure_ascii=False, indent=2), encoding="utf-8")
    os.replace(tmp, p)


def safe_name(name: str) -> str:
    return re.sub(r'[\\/:*?"<>|\x00-\x1f]', "_", name).strip(" .")[:200] or "file"


def issue_dir(root: Path, key: str) -> Path:
    return root / key.split("-")[0] / key


def save_issue(root: Path, issue: dict):
    d = issue_dir(root, issue["key"])
    d.mkdir(parents=True, exist_ok=True)
    dump(d / "issue.json", issue)


def download(tr: Tracker, url: str, dst: Path, attempts: int = 3):
    part = dst.with_name(dst.name + ".part")
    for i in range(attempts):
        try:
            with tr.get(url, stream=True) as r, open(part, "wb") as fh:
                for chunk in r.iter_content(1 << 20):
                    fh.write(chunk)
            os.replace(part, dst)
            return
        except (requests.ConnectionError, requests.Timeout, requests.exceptions.ChunkedEncodingError):
            if i + 1 == attempts:
                raise
            time.sleep(2 ** i)
        finally:
            part.unlink(missing_ok=True)


def backup_issue(tr: Tracker, key: str, root: Path, with_files: bool):
    """Всё, кроме самой задачи: issue.json уже сохранён на этапе поиска."""
    d = issue_dir(root, key)
    dump(d / "comments.json", paged(tr, f"{API}/issues/{key}/comments"))
    dump(d / "changelog.json", paged(tr, f"{API}/issues/{key}/changelog"))
    dump(d / "links.json", tr.get(f"{API}/issues/{key}/links").json())
    if not with_files:
        return
    atts = tr.get(f"{API}/issues/{key}/attachments").json()
    dump(d / "attachments.json", atts)
    for a in atts:
        f = d / "attachments" / safe_name(f'{a["id"]}_{a["name"]}')
        if f.exists() and f.stat().st_size == a.get("size", -1):
            continue
        f.parent.mkdir(exist_ok=True)
        download(tr, a["content"], f)


def backup_meta(tr: Tracker, root: Path, errors: list):
    """Описания полей и справочники — без них значения полей в issue.json трудно восстановить."""
    d = root / "_meta"
    d.mkdir(parents=True, exist_ok=True)
    for name in ("fields", "statuses", "priorities", "issuetypes", "resolutions", "linktypes"):
        try:
            dump(d / f"{name}.json", tr.get(f"{API}/{name}").json())
        except (requests.RequestException, ValueError) as e:
            log.warning("_meta/%s: %s", name, e)
            errors.append({"key": f"_meta/{name}", "error": repr(e)})


def backup_queue_meta(tr: Tracker, q: str, root: Path, errors: list):
    d = root / q / "_queue"
    d.mkdir(parents=True, exist_ok=True)
    for name, url, params in (("queue", f"{API}/queues/{q}", {"expand": "all"}),
                              ("localFields", f"{API}/queues/{q}/localFields", None)):
        try:
            dump(d / f"{name}.json", tr.get(url, params=params).json())
        except (requests.RequestException, ValueError) as e:
            log.warning("%s/_queue/%s: %s", q, name, e)
            errors.append({"key": f"{q}/_queue/{name}", "error": repr(e)})


class Progress:
    """Счётчики очереди. Ошибки задач пишутся сразу в общий список, чтобы не потеряться при аварии."""

    def __init__(self, q: str, errors: list):
        self.q, self.errors = q, errors
        self.done = self.failed = 0
        self.fatal: BaseException | None = None
        self.lock = threading.Lock()
        self.last_log = time.monotonic()

    def ok(self, key: str):
        with self.lock:
            self.done += 1
            if time.monotonic() - self.last_log > 10:
                self.last_log = time.monotonic()
                log.info("%s: %d issues (последний %s)", self.q, self.done, key)

    def fail(self, key: str, e: BaseException):
        with self.lock:
            self.failed += 1
            self.errors.append({"key": key, "error": repr(e)})
        log.error("%s: %s", key, e)


def backup_queue(tr: Tracker, ex: ThreadPoolExecutor, prog: Progress, since: str | None,
                 root: Path, with_files: bool, inflight: int):
    q = prog.q
    backup_queue_meta(tr, q, root, prog.errors)
    flt = {"queue": q}
    if since:
        flt["updated"] = {"from": since}
    try:
        keys = search_keys(tr, {"filter": flt, "order": "+updated"}, lambda issue: save_issue(root, issue))
    except (requests.RequestException, ValueError, PinnedTokenDead) as e:
        prog.fail(f"{q}/_search", e)
        return
    log.info("%s: найдено %d задач", q, len(keys))

    sem = threading.BoundedSemaphore(inflight)  # ограничиваем число задач в работе

    def run(key):
        try:
            backup_issue(tr, key, root, with_files)
            prog.ok(key)
        except NoTokensLeft as e:
            prog.fatal = e
        except Exception as e:
            prog.fail(key, e)
        finally:
            sem.release()

    for key in keys:
        if prog.fatal:
            break
        sem.acquire()
        ex.submit(run, key)
    for _ in range(inflight):  # ждём, пока доработают все задачи очереди
        sem.acquire()
    if prog.fatal:
        raise prog.fatal


def load_tokens(a) -> list[Token]:
    raw = []
    if a.tokens_file:
        raw += [l for l in Path(a.tokens_file).read_text().splitlines() if l.strip() and not l.lstrip().startswith("#")]
    if os.getenv("TRACKER_TOKENS"):
        raw += [t for t in re.split(r"[,\n]", os.environ["TRACKER_TOKENS"]) if t.strip()]
    if not raw and os.getenv("TRACKER_TOKEN"):
        raw.append(os.environ["TRACKER_TOKEN"])
    scheme = os.getenv("TRACKER_AUTH", "OAuth")
    return [Token(t, i, scheme) for i, t in enumerate(dict.fromkeys(t.strip() for t in raw), 1)]


def org_headers() -> dict:
    if os.getenv("TRACKER_CLOUD_ORG_ID"):
        return {"X-Cloud-Org-ID": os.environ["TRACKER_CLOUD_ORG_ID"]}
    if os.getenv("TRACKER_ORG_ID"):
        return {"X-Org-ID": os.environ["TRACKER_ORG_ID"]}
    sys.exit("нужен TRACKER_ORG_ID или TRACKER_CLOUD_ORG_ID")


def _on_sigterm(signum, frame):
    # docker stop шлёт SIGTERM; у PID 1 без своего обработчика сигнал игнорируется
    raise KeyboardInterrupt


def main():
    signal.signal(signal.SIGTERM, _on_sigterm)
    ap = argparse.ArgumentParser()
    ap.add_argument("--out", default="./tracker-backup")
    ap.add_argument("--queues", help="DEV,OPS; по умолчанию все очереди")
    ap.add_argument("--full", action="store_true", help="игнорировать state.json при выборе задач")
    ap.add_argument("--no-files", action="store_true", help="не скачивать вложения")
    ap.add_argument("--tokens-file", help="файл с токенами, по одному в строке")
    ap.add_argument("--rps", type=float, default=float(os.getenv("TRACKER_RPS", "5")),
                    help="лимит запросов в секунду на один токен (0 — без лимита)")
    ap.add_argument("--workers", type=int, help="число потоков; по умолчанию 4 на токен, максимум 32")
    ap.add_argument("-v", "--verbose", action="store_true")
    a = ap.parse_args()
    logging.basicConfig(level=logging.DEBUG if a.verbose else logging.INFO,
                        format="%(asctime)s %(levelname)s %(message)s", stream=sys.stderr)

    tokens = load_tokens(a)
    if not tokens:
        sys.exit("нужен TRACKER_TOKENS, TRACKER_TOKEN или --tokens-file")
    pool = TokenPool(tokens, a.rps)
    tr = Tracker(pool, org_headers())
    workers = a.workers or min(32, 4 * len(tokens))
    log.info("токенов: %d, rps на токен: %s, потоков: %d", len(tokens), a.rps or "∞", workers)

    root = Path(a.out)
    root.mkdir(parents=True, exist_ok=True)
    state_f = root / "state.json"
    # --full не трогает отметки очередей, которые в этот прогон не входят
    state = json.loads(state_f.read_text()) if state_f.exists() else {}
    started = (datetime.now(timezone.utc) - STATE_OVERLAP).strftime("%Y-%m-%dT%H:%M:%S.000+0000")

    errors: list = []
    progs: list[Progress] = []  # включая прерванную очередь — для итоговой статистики
    code = 0
    ex = ThreadPoolExecutor(workers)
    try:
        backup_meta(tr, root, errors)
        if a.queues:
            queues = list(dict.fromkeys(q.strip() for q in a.queues.split(",") if q.strip()))
        else:
            queues = [q["key"] for q in pages(tr, f"{API}/queues")]
        dump(root / "queues.json", queues)

        for q in queues:
            prog = Progress(q, errors)
            progs.append(prog)
            backup_queue(tr, ex, prog, None if a.full else state.get(q), root, not a.no_files, workers * 4)
            log.info("%s: готово %d, ошибок %d", q, prog.done, prog.failed)
            if not prog.failed:  # с ошибками не сдвигаем state — в следующий раз перекачаем
                state[q] = started
                dump(state_f, state)
    except KeyboardInterrupt:
        log.warning("прервано (Ctrl+C / SIGTERM), state для текущей очереди не сохранён")
        code = 130
    except (NoTokensLeft, requests.RequestException, ValueError) as e:
        log.error("остановка: %s", e)
        code = 2
    finally:
        ex.shutdown(wait=code == 0, cancel_futures=code != 0)
        dump(root / "errors.json", errors)

    total, failed = sum(p.done for p in progs), sum(p.failed for p in progs)
    log.info("done: %d issues, ошибок по задачам %d, всего записей в errors.json %d; %s",
             total, failed, len(errors), pool.stats())
    sys.exit(code or (1 if errors else 0))


if __name__ == "__main__":
    main()
