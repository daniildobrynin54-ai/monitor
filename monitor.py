"""
MangaBuff Alliance Monitor — стабильная версия.

Улучшения по сравнению с оригиналом:
  • Экспоненциальный backoff с джиттером вместо фиксированных пауз
  • Circuit Breaker: при серии сбоев не долбим сервер
  • Детектирование «тихого» выхода из сессии (200 OK, но HTML логин-формы)
  • Автопереавторизация по счётчику подряд-идущих ошибок
  • Превентивная переавторизация каждые REAUTH_INTERVAL_H часов
  • Корректная обработка 429 (Rate-Limit) с Retry-After
  • Трекер здоровья и периодический вывод статистики
  • Flush очереди Telegram при восстановлении
"""

import time
import json
import os
import re
from datetime import datetime, date
from urllib.parse import unquote

import requests
from bs4 import BeautifulSoup

from config import Config
from telegram_bot import TelegramNotifier
from utils import (
    setup_logger,
    calc_backoff,
    sleep_backoff,
    CircuitBreaker,
    HealthTracker,
)


# ---------------------------------------------------------------------------
# Константы
# ---------------------------------------------------------------------------

USER_AGENT = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
    "AppleWebKit/537.36 (KHTML, like Gecko) "
    "Chrome/145.0.0.0 Safari/537.36"
)

BASE_URL        = "https://mangabuff.ru"
REQUEST_TIMEOUT = 15

# Количество подряд-идущих ошибок получения страницы → триггер переавторизации
FAILURES_BEFORE_REAUTH = 5

# Максимум переавторизаций подряд до «фатального» выхода
MAX_REAUTH_ATTEMPTS = 8

# Превентивная переавторизация каждые N часов
REAUTH_INTERVAL_H = 2

# Circuit Breaker: сколько сбоев подряд → открыть (перестать ломиться)
CB_FAILURE_THRESHOLD = 6
CB_RECOVERY_TIMEOUT  = 90    # секунд до перехода в HALF_OPEN

# Базовая пауза основного цикла при ошибке (перед sleep_backoff)
LOOP_BASE_DELAY = 10

# Интервал вывода периодического статуса (в итерациях)
STATUS_LOG_EVERY = 60


# ---------------------------------------------------------------------------
# Вспомогательные функции
# ---------------------------------------------------------------------------

def _get_cookie(jar, name: str) -> str | None:
    for c in jar:
        if c.name == name and c.value:
            return c.value
    return None


def _extract_csrf(html: str) -> str | None:
    soup = BeautifulSoup(html, "html.parser")
    meta = soup.select_one('meta[name="csrf-token"]')
    if meta:
        t = meta.get("content", "").strip()
        if t:
            return t
    inp = soup.find("input", {"name": "_token"})
    if inp:
        t = inp.get("value", "").strip()
        if t:
            return t
    return None


def _apply_ajax_tokens(session: requests.Session):
    xsrf_raw = _get_cookie(session.cookies, "XSRF-TOKEN")
    if xsrf_raw:
        xsrf = unquote(xsrf_raw)
        session.headers.update({"X-CSRF-TOKEN": xsrf, "X-XSRF-TOKEN": xsrf})


def _nav_headers(referer: str = None, fetch_site: str = "none") -> dict:
    h = {
        "Accept": (
            "text/html,application/xhtml+xml,application/xml;q=0.9,"
            "image/avif,image/webp,image/apng,*/*;q=0.8"
        ),
        "Accept-Language":    "ru-RU,ru;q=0.9,en-US;q=0.8,en;q=0.7",
        "Accept-Encoding":    "gzip, deflate, br, zstd",
        "Upgrade-Insecure-Requests": "1",
        "Sec-Ch-Ua":          '"Not:A-Brand";v="99", "Google Chrome";v="145", "Chromium";v="145"',
        "Sec-Ch-Ua-Mobile":   "?0",
        "Sec-Ch-Ua-Platform": '"Windows"',
        "Sec-Fetch-Site":     fetch_site,
        "Sec-Fetch-Mode":     "navigate",
        "Sec-Fetch-User":     "?1",
        "Sec-Fetch-Dest":     "document",
        "Priority":           "u=0, i",
    }
    if referer:
        h["Referer"] = referer
    return h


def _is_auth_html(html: str) -> bool:
    """Проверяет, что HTML содержит признаки авторизованной сессии."""
    if "window.isAuth = 1" in html or "window.isAuth=1" in html:
        return True
    m = re.search(r'window\.user_id\s*=\s*(\d+)', html)
    if m and m.group(1) not in ("0", ""):
        return True
    return False


def _is_login_page(response_url: str, html: str) -> bool:
    """
    Определяет, что сервер вернул страницу логина вместо запрошенной.

    Проверяем в порядке надёжности:
    1. URL после редиректа содержит /login  — самый надёжный признак
    2. HTML не содержит маркеров авторизации И содержит уникальные
       элементы именно страницы /login (заголовок, action формы).

    Намеренно НЕ используем просто 'name="email"' / 'name="password"' —
    Laravel включает скрытый auth-modal на каждой странице сайта.
    """
    # 1. Редирект на /login
    if "/login" in response_url:
        return True

    # 2. Нет маркеров авторизации + есть action="/login" (форма именно логина)
    if not _is_auth_html(html) and 'action="' in html:
        if '/login"' in html or "/login'" in html:
            return True

    return False


def _handle_status_code(status: int, attempt: int, logger, label: str = "") -> bool:
    """
    Обрабатывает не-200 статусы.
    Возвращает True если нужно повторить запрос, False если фатально.
    """
    if status == 429:
        # rate-limit — пауза будет добавлена снаружи через Retry-After или backoff
        logger.warning(f"⚠️ 429 Rate-Limit {label}")
        return True
    if status in (500, 502, 503, 504):
        logger.warning(f"⚠️ {status} сервер {label} (попытка {attempt+1})")
        return True
    if status in (400, 404):
        logger.error(f"❌ {status} на {label} — не повторяем")
        return False
    if status in (401, 403):
        logger.warning(f"⚠️ {status} сессия истекла {label}")
        return False          # сигнал к переавторизации
    logger.warning(f"⚠️ HTTP {status} {label} (попытка {attempt+1})")
    return True


# ---------------------------------------------------------------------------
# Монитор
# ---------------------------------------------------------------------------

class MangaBuffMonitor:
    def __init__(self):
        self.config = Config()
        os.makedirs(self.config.LOG_DIR, exist_ok=True)

        self.logger = setup_logger(
            "mangabuff",
            self.config.LOG_FILE,
        )
        self.telegram = TelegramNotifier(
            self.config.TELEGRAM_BOT_TOKEN,
            self.config.TELEGRAM_CHAT_ID,
            logger=self.logger,
        )

        self.session = requests.Session()
        self.session.headers.update({"User-Agent": USER_AGENT})

        # Состояние альянса
        self.current_manga: str | None = None
        self.current_manga_info: dict | None = None
        self.last_page_data: dict = {}

        # Трекинг опыта за день
        self.today = date.today()
        self.exp_at_day_start: int | None = None
        self.last_known_exp: int | None = None

        # Время последней (успешной) авторизации
        self._last_login_time: float = 0.0

        # Circuit Breaker для запросов к сайту
        self._cb = CircuitBreaker(
            failure_threshold=CB_FAILURE_THRESHOLD,
            recovery_timeout=CB_RECOVERY_TIMEOUT,
            name="MangaBuff",
            logger=self.logger,
        )

        # Здоровье монитора
        self._health = HealthTracker()

        # Счётчик переавторизаций подряд (сбрасывается при успехе)
        self._reauth_streak = 0

    # ------------------------------------------------------------------
    # Логирование (совместимость + делегирование в logger)
    # ------------------------------------------------------------------

    def log(self, message: str, level: str = "info"):
        getattr(self.logger, level)(message)

    # ------------------------------------------------------------------
    # Авторизация
    # ------------------------------------------------------------------

    def login(self, attempt_num: int = 0) -> bool:
        """
        Полная авторизация. attempt_num используется для backoff если
        вызывается в цикле переавторизаций.
        """
        try:
            self.logger.info("🔐 Вход в аккаунт...")

            # 1. Главная страница (получаем куки)
            try:
                r0 = self.session.get(
                    BASE_URL, headers=_nav_headers(), timeout=REQUEST_TIMEOUT
                )
                self.logger.debug(
                    f"   [1] GET / → {r0.status_code}, "
                    f"куки: {[c.name for c in self.session.cookies]}"
                )
            except requests.RequestException as e:
                self.logger.warning(f"   [1] GET / ошибка: {e} (продолжаем)")

            # 2. Страница логина
            try:
                r_get = self.session.get(
                    f"{BASE_URL}/login",
                    headers=_nav_headers(
                        referer=f"{BASE_URL}/", fetch_site="same-origin"
                    ),
                    timeout=REQUEST_TIMEOUT,
                )
            except requests.RequestException as e:
                self.logger.error(f"   [2] GET /login ошибка: {e}")
                return False

            self.logger.debug(f"   [2] GET /login → {r_get.status_code}")
            if r_get.status_code != 200:
                self.logger.error(f"   ❌ /login вернул {r_get.status_code}")
                return False

            csrf = _extract_csrf(r_get.text)
            if not csrf:
                self.logger.error("   ❌ CSRF-токен не найден")
                return False

            self.logger.debug(f"   CSRF: {csrf[:30]}…")
            xsrf_raw = _get_cookie(self.session.cookies, "XSRF-TOKEN")
            xsrf = unquote(xsrf_raw) if xsrf_raw else csrf

            ajax_headers = {
                "Accept":            "application/json, text/plain, */*",
                "Content-Type":      "application/x-www-form-urlencoded; charset=UTF-8",
                "X-Requested-With":  "XMLHttpRequest",
                "X-CSRF-TOKEN":      xsrf,
                "X-XSRF-TOKEN":      xsrf,
                "Origin":            BASE_URL,
                "Referer":           f"{BASE_URL}/login",
                "Sec-Ch-Ua":         '"Not:A-Brand";v="99", "Google Chrome";v="145", "Chromium";v="145"',
                "Sec-Ch-Ua-Mobile":  "?0",
                "Sec-Ch-Ua-Platform":'"Windows"',
                "Sec-Fetch-Site":    "same-origin",
                "Sec-Fetch-Mode":    "cors",
                "Sec-Fetch-Dest":    "empty",
            }

            # 3. POST логина
            try:
                r_post = self.session.post(
                    f"{BASE_URL}/login",
                    data={
                        "email":    self.config.MANGABUFF_EMAIL,
                        "password": self.config.MANGABUFF_PASSWORD,
                        "_token":   csrf,
                    },
                    headers=ajax_headers,
                    allow_redirects=True,
                    timeout=REQUEST_TIMEOUT,
                )
            except requests.RequestException as e:
                self.logger.error(f"   [3] POST /login ошибка: {e}")
                return False

            self.logger.debug(
                f"   [3] POST /login → {r_post.status_code}, URL: {r_post.url}"
            )

            # Проверяем JSON-ответ на ошибку
            ct = r_post.headers.get("content-type", "")
            if "application/json" in ct:
                try:
                    j = r_post.json()
                except Exception:
                    self.logger.error(f"   JSON parse error: {r_post.text[:200]}")
                    return False
                if j.get("errors") or j.get("status") == "error":
                    self.logger.error(f"   ❌ Сервер: {j}")
                    return False

            # 4. Проверка авторизации через главную страницу
            try:
                r_main = self.session.get(BASE_URL, timeout=REQUEST_TIMEOUT)
                is_auth = _is_auth_html(r_main.text)
            except Exception as e:
                self.logger.warning(f"   isAuth check error: {e}")
                is_auth = False

            if not is_auth:
                self.logger.error("   ❌ Не авторизованы после POST /login")
                return False

            _apply_ajax_tokens(self.session)
            new_csrf = _extract_csrf(
                r_post.text if "text/html" in ct else r_main.text
            )
            if new_csrf:
                self.session.headers.update({"X-CSRF-TOKEN": new_csrf})
            self.session.headers.update({"X-Requested-With": "XMLHttpRequest"})

            self._last_login_time = time.monotonic()
            self.logger.info("✅ Успешный вход")
            return True

        except Exception as e:
            self.logger.exception(f"❌ Ошибка при входе: {e}")
            return False

    def _needs_reauth(self) -> bool:
        """Возвращает True если пора сделать превентивную переавторизацию."""
        elapsed = time.monotonic() - self._last_login_time
        return elapsed >= REAUTH_INTERVAL_H * 3600

    def _try_reauth(self) -> bool:
        """
        Цикл переавторизации с экспоненциальным backoff.
        Возвращает True при успехе, False если превышен MAX_REAUTH_ATTEMPTS.
        """
        self._health.reauth()
        self._reauth_streak += 1

        if self._reauth_streak > MAX_REAUTH_ATTEMPTS:
            self.logger.error(
                f"❌ Превышено {MAX_REAUTH_ATTEMPTS} переавторизаций подряд — выход"
            )
            return False

        attempt = self._reauth_streak - 1
        if attempt > 0:
            delay = calc_backoff(attempt, base=15, maximum=300)
            self.logger.warning(
                f"🔐 Переавторизация #{self._reauth_streak}, пауза {delay:.1f}с..."
            )
            time.sleep(delay)
        else:
            self.logger.warning("🔐 Переавторизация...")

        # Сбрасываем сессию перед новым входом
        self.session.cookies.clear()
        if self.login():
            self._reauth_streak = 0
            self._cb.reset()
            self.logger.info("✅ Переавторизация успешна")
            return True
        return False

    # ------------------------------------------------------------------
    # Получение страницы альянса
    # ------------------------------------------------------------------

    def _parse_number(self, text: str) -> int | None:
        if not text:
            return None
        cleaned = re.sub(r"[^\d]", "", text.strip())
        return int(cleaned) if cleaned else None

    def get_alliance_page_data(self) -> dict | None:
        """
        Получает данные страницы альянса.
        Возвращает dict или None (включая случай истёкшей сессии — тогда
        выставляет флаг через исключение SessionExpired).
        """
        max_retries = 4

        for attempt in range(max_retries):

            # Circuit Breaker
            if self._cb.is_open():
                self.logger.warning(
                    f"⛔ Circuit Breaker OPEN — ждём восстановления сервера "
                    f"({CB_RECOVERY_TIMEOUT}с...)"
                )
                time.sleep(CB_RECOVERY_TIMEOUT)
                return None

            try:
                response = self.session.get(
                    self.config.ALLIANCE_URL,
                    timeout=REQUEST_TIMEOUT,
                    allow_redirects=True,
                )

                # ---- Обработка не-200 -----------------------------------
                if response.status_code == 429:
                    retry_after = int(response.headers.get("Retry-After", 30))
                    self.logger.warning(f"⚠️ 429 — ждём {retry_after}с")
                    self._cb.record_failure()
                    time.sleep(retry_after + 1)
                    continue

                if response.status_code in (500, 502, 503, 504):
                    self.logger.warning(
                        f"⚠️ {response.status_code} — попытка {attempt+1}/{max_retries}"
                    )
                    self._cb.record_failure()
                    sleep_backoff(attempt, base=5, maximum=120, logger=self.logger)
                    continue

                if response.status_code in (401, 403):
                    self.logger.warning(
                        f"⚠️ {response.status_code} — сессия истекла"
                    )
                    self._cb.record_failure()
                    raise _SessionExpired()

                if response.status_code != 200:
                    retry = _handle_status_code(
                        response.status_code, attempt, self.logger, "alliance"
                    )
                    self._cb.record_failure()
                    if retry and attempt < max_retries - 1:
                        sleep_backoff(attempt, logger=self.logger)
                        continue
                    return None

                # ---- Проверка «тихого» выхода ---------------------------
                if _is_login_page(response.url, response.text):
                    self.logger.warning(
                        f"⚠️ Сессия истекла (URL: {response.url})"
                    )
                    raise _SessionExpired()

                # ---- Успешный ответ: парсинг ----------------------------
                self._cb.record_success()
                return self._parse_alliance_page(response.text)

            except _SessionExpired:
                raise

            except (requests.exceptions.Timeout, requests.exceptions.ConnectionError) as e:
                self._cb.record_failure()
                self.logger.warning(
                    f"⚠️ Сеть: {e} (попытка {attempt+1}/{max_retries})"
                )
                if attempt < max_retries - 1:
                    sleep_backoff(attempt, base=5, maximum=60, logger=self.logger)
                    continue
                return None

            except Exception as e:
                self._cb.record_failure()
                self.logger.error(f"⚠️ Ошибка парсинга/запроса: {e}")
                if attempt < max_retries - 1:
                    sleep_backoff(attempt, base=5, maximum=60, logger=self.logger)
                    continue
                return None

        self.logger.error(f"❌ Все {max_retries} попытки исчерпаны (alliance)")
        return None

    def _parse_alliance_page(self, html: str) -> dict | None:
        """Парсит HTML страницы альянса в dict."""
        try:
            soup = BeautifulSoup(html, "html.parser")
            result: dict = {}

            # Slug манги
            manga_link = soup.find("a", class_="card-show__placeholder")
            if manga_link:
                href = manga_link.get("href", "")
                if href.startswith("/manga/"):
                    result["slug"] = href.replace("/manga/", "")

            if "slug" not in result:
                poster = soup.find("div", class_="card-show__header")
                if poster:
                    style = poster.get("style", "")
                    if "background-image: url(" in style:
                        try:
                            img_url = style.split("url('")[1].split("'")[0]
                            result["slug"] = img_url.split("/posters/")[-1].replace(".jpg", "")
                        except Exception:
                            pass

            # Уровень
            lv = soup.find("div", class_="alliance__level-value")
            if lv:
                m = re.search(r"\d+", lv.text)
                result["level"] = m.group(0) if m else None

            # Текущий опыт
            exp_elem = soup.find("div", class_="alliance__level-exp")
            if exp_elem:
                result["exp_current"] = self._parse_number(exp_elem.text)

            # Опыт до следующего уровня
            tot_elem = soup.find("div", class_="alliance__level-total-exp")
            if tot_elem:
                result["exp_total"] = self._parse_number(tot_elem.text)

            # Шанс смены манги
            chance_elem = soup.find("span", class_="alliance__chance-change-manga")
            if chance_elem:
                result["chance"] = chance_elem.text.strip()

            return result if result else None

        except Exception as e:
            self.logger.error(f"⚠️ Ошибка парсинга HTML альянса: {e}")
            return None

    # ------------------------------------------------------------------
    # Детали манги
    # ------------------------------------------------------------------

    def get_manga_details(self, manga_slug: str) -> dict | None:
        max_retries = 3

        for attempt in range(max_retries):
            try:
                url = f"{BASE_URL}/manga/{manga_slug}"
                response = self.session.get(url, timeout=REQUEST_TIMEOUT)

                if response.status_code == 429:
                    retry_after = int(response.headers.get("Retry-After", 30))
                    self.logger.warning(f"⚠️ 429 (manga) — ждём {retry_after}с")
                    time.sleep(retry_after + 1)
                    continue

                if response.status_code in (500, 502, 503, 504):
                    self.logger.warning(
                        f"⚠️ {response.status_code} (manga), попытка {attempt+1}/{max_retries}"
                    )
                    sleep_backoff(attempt, logger=self.logger)
                    continue

                if response.status_code != 200:
                    self.logger.error(f"❌ Ошибка страницы манги: {response.status_code}")
                    if attempt < max_retries - 1:
                        sleep_backoff(attempt, logger=self.logger)
                        continue
                    return None

                soup = BeautifulSoup(response.text, "html.parser")

                title = None
                for cls in ("manga-mobile__name", "manga__name"):
                    elem = soup.find("h1", class_=cls)
                    if elem:
                        title = elem.text.strip()
                        break
                if not title:
                    title = manga_slug

                img_src = None
                img_elem = soup.find("img", class_="manga-mobile__image")
                if img_elem:
                    img_src = img_elem.get("src")
                if not img_src:
                    wrapper = soup.find("div", class_="manga__img")
                    if wrapper:
                        img = wrapper.find("img")
                        if img:
                            img_src = img.get("src")
                if img_src and img_src.startswith("/"):
                    img_src = f"{BASE_URL}{img_src}"

                self.logger.info(f"✅ Детали манги: {title}")
                return {
                    "slug":      manga_slug,
                    "title":     title,
                    "image":     img_src,
                    "timestamp": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                }

            except (requests.exceptions.Timeout, requests.exceptions.ConnectionError) as e:
                self.logger.warning(
                    f"⚠️ Сеть (manga): {e} (попытка {attempt+1}/{max_retries})"
                )
                if attempt < max_retries - 1:
                    sleep_backoff(attempt, logger=self.logger)
                    continue
                return None

            except Exception as e:
                self.logger.error(f"❌ Ошибка деталей манги: {e}")
                if attempt < max_retries - 1:
                    sleep_backoff(attempt, logger=self.logger)
                    continue
                return None

        return None

    # ------------------------------------------------------------------
    # Трекинг опыта
    # ------------------------------------------------------------------

    def _update_exp_tracking(self, exp_current: int | None):
        if exp_current is None:
            return
        today = date.today()
        if today != self.today:
            self.today = today
            self.exp_at_day_start = exp_current
            self.logger.info(f"📅 Новый день, сброс прироста. Старт: {exp_current}")
        if self.exp_at_day_start is None:
            self.exp_at_day_start = exp_current
        self.last_known_exp = exp_current

    def get_exp_gain_today(self) -> int | None:
        if self.last_known_exp is None or self.exp_at_day_start is None:
            return None
        gain = self.last_known_exp - self.exp_at_day_start
        return gain if gain >= 0 else None

    # ------------------------------------------------------------------
    # Определение изменений
    # ------------------------------------------------------------------

    def _stats_changed(self, new_data: dict) -> bool:
        if not self.last_page_data:
            return False
        for key in ("exp_current", "exp_total", "chance", "level"):
            if new_data.get(key) != self.last_page_data.get(key):
                return True
        return False

    # ------------------------------------------------------------------
    # История
    # ------------------------------------------------------------------

    def save_history(self, manga_info: dict):
        try:
            try:
                with open(self.config.HISTORY_FILE, "r", encoding="utf-8") as f:
                    history = json.load(f)
            except (FileNotFoundError, json.JSONDecodeError):
                history = []

            history.append(manga_info)
            if len(history) > 100:
                history = history[-100:]

            with open(self.config.HISTORY_FILE, "w", encoding="utf-8") as f:
                json.dump(history, f, ensure_ascii=False, indent=2)

            self.logger.info(f"💾 История: {len(history)} записей")
        except Exception as e:
            self.logger.warning(f"⚠️ Ошибка истории: {e}")

    # ------------------------------------------------------------------
    # Запуск
    # ------------------------------------------------------------------

    def _send_manga_notification(
        self, manga_info: dict, page_data: dict, is_startup: bool
    ):
        """Отправляет фото/текст с информацией о манге."""
        caption = self.telegram.format_manga_caption(
            manga_info, page_data, self.get_exp_gain_today(), is_startup=is_startup
        )
        if manga_info.get("image"):
            self.telegram.send_photo_to_all_topics(manga_info["image"], caption)
        else:
            self.telegram.send_message_to_all_topics(caption)

    def start(self):
        try:
            self.logger.info("🔧 Проверка конфигурации...")
            self.config.validate()
        except ValueError as e:
            self.logger.error(f"❌ Конфигурация: {e}")
            return

        # ----- Первичная авторизация -----
        if not self.login():
            self.logger.error("❌ Не удалось авторизоваться при старте")
            self.telegram.send_message_to_all_topics(
                "❌ Монитор не смог войти в аккаунт при запуске."
            )
            return

        # ----- Начальное состояние -----
        self.logger.info("📚 Получаю начальные данные альянса...")
        try:
            page_data = self.get_alliance_page_data()
        except _SessionExpired:
            # Крайне редко: сессия уже истекла сразу после логина
            self.logger.warning("⚠️ _SessionExpired сразу после логина — повторный вход")
            if not self._try_reauth():
                self.telegram.send_message_to_all_topics("❌ Не удалось авторизоваться при старте.")
                return
            try:
                page_data = self.get_alliance_page_data()
            except _SessionExpired:
                self.logger.error("❌ Повторная авторизация не помогла — выход")
                return

        if page_data and page_data.get("slug"):
            self.current_manga = page_data["slug"]
            self.last_page_data = page_data
            self._update_exp_tracking(page_data.get("exp_current"))

            self.logger.info(
                f"📚 Тайтл: {self.current_manga} | "
                f"Опыт: {page_data.get('exp_current')}/{page_data.get('exp_total')} | "
                f"Шанс: {page_data.get('chance')}%"
            )

            manga_info = self.get_manga_details(self.current_manga)
            self.current_manga_info = manga_info

            if manga_info:
                self._send_manga_notification(manga_info, page_data, is_startup=True)
            else:
                self.telegram.send_message_to_all_topics(
                    f"🚀 Монитор запущен\n📚 <code>{self.current_manga}</code>\n(детали недоступны)"
                )
        else:
            self.logger.warning("⚠️ Не удалось получить тайтл при старте")
            self.telegram.send_message_to_all_topics(
                "🚀 Монитор запущен\n⚠️ Не удалось получить тайтл альянса"
            )

        self.logger.info(
            f"👀 Интервал: {self.config.CHECK_INTERVAL}с | "
            f"Reauth: каждые {REAUTH_INTERVAL_H}ч | Ctrl+C для остановки"
        )

        # ----- Основной цикл -----
        check_count = 0
        consecutive_no_data = 0
        loop_error_attempt = 0   # для backoff при ошибках цикла

        while True:
            try:
                check_count += 1

                # Статус каждые STATUS_LOG_EVERY итераций
                if check_count % STATUS_LOG_EVERY == 0:
                    self.logger.info(
                        f"🔍 #{check_count} тайтл={self.current_manga} | "
                        + self._health.summary()
                    )
                else:
                    print(f"\r🔍 #{check_count}… ", end="", flush=True)

                # Превентивная переавторизация
                if self._needs_reauth():
                    self.logger.info(
                        f"🔐 Плановая переавторизация (>{REAUTH_INTERVAL_H}ч)"
                    )
                    self.session.cookies.clear()
                    if not self.login():
                        self.logger.warning("⚠️ Плановая переавторизация не удалась — продолжаем со старой сессией")

                # Отправляем отложенные Telegram-сообщения
                self.telegram.flush_queue()

                # ----- Запрос страницы -----
                try:
                    page_data = self.get_alliance_page_data()
                except _SessionExpired:
                    print()
                    self.logger.warning("🔐 Сессия истекла — переавторизуемся")
                    self._health.fail()
                    if not self._try_reauth():
                        self.telegram.send_message_to_all_topics(
                            "❌ Монитор остановлен: не удаётся авторизоваться."
                        )
                        break
                    time.sleep(2)
                    continue

                # ----- Нет данных -----
                if page_data is None:
                    self._health.fail()
                    consecutive_no_data += 1
                    loop_error_attempt += 1

                    if consecutive_no_data % 5 == 0:
                        self.logger.warning(
                            f"⚠️ Нет данных ({consecutive_no_data} раз подряд)"
                        )

                    # После N подряд-идущих ошибок — переавторизоваться
                    if consecutive_no_data >= FAILURES_BEFORE_REAUTH:
                        print()
                        self.logger.warning(
                            f"🔐 {consecutive_no_data} ошибок подряд → переавторизация"
                        )
                        if not self._try_reauth():
                            self.telegram.send_message_to_all_topics(
                                "❌ Монитор остановлен: не удаётся авторизоваться."
                            )
                            break
                        consecutive_no_data = 0
                        loop_error_attempt = 0

                    # Адаптивная пауза
                    delay = min(
                        self.config.CHECK_INTERVAL + calc_backoff(loop_error_attempt, base=3, maximum=60),
                        90,
                    )
                    time.sleep(delay)
                    continue

                # ----- Данные получены -----
                self._health.ok()
                consecutive_no_data = 0
                loop_error_attempt = 0
                self._reauth_streak = 0

                self._update_exp_tracking(page_data.get("exp_current"))
                new_slug = page_data.get("slug")

                # --- Смена тайтла ---
                if new_slug and new_slug != self.current_manga:
                    print()
                    self.logger.info(
                        f"🔔 СМЕНА ТАЙТЛА: {self.current_manga} → {new_slug}"
                    )

                    manga_info = self.get_manga_details(new_slug)
                    self.current_manga_info = manga_info

                    if manga_info:
                        self._send_manga_notification(manga_info, page_data, is_startup=False)
                        self.save_history(manga_info)
                    else:
                        self.telegram.send_message_to_all_topics(
                            f"🔔 <b>Смена тайтла!</b>\n\n<code>{new_slug}</code>\n(детали недоступны)"
                        )

                    self.current_manga = new_slug
                    self.last_page_data = page_data
                    self.logger.info("✅ Уведомление отправлено")

                # --- Изменились опыт/шанс → тихое редактирование ---
                elif self._stats_changed(page_data) and self.current_manga_info:
                    self.last_page_data = page_data
                    caption = self.telegram.format_manga_caption(
                        self.current_manga_info,
                        page_data,
                        self.get_exp_gain_today(),
                        is_startup=False,
                    )
                    self.telegram.update_caption_in_all_topics(caption)

                time.sleep(self.config.CHECK_INTERVAL)

            except KeyboardInterrupt:
                print()
                self.logger.info("⏹️ Остановка по Ctrl+C")
                self.telegram.send_message_to_all_topics(
                    f"⏹️ Мониторинг остановлен\n{self._health.summary()}"
                )
                break

            except requests.exceptions.RequestException as e:
                print()
                self.logger.warning(f"⚠️ Сетевая ошибка в цикле: {e}")
                self._health.fail()
                loop_error_attempt += 1
                delay = calc_backoff(loop_error_attempt, base=LOOP_BASE_DELAY, maximum=120)
                self.logger.info(f"   Пауза {delay:.1f}с, затем переавторизация...")
                time.sleep(delay)
                if not self._try_reauth():
                    self.telegram.send_message_to_all_topics(
                        "❌ Мониторинг остановлен: сеть недоступна."
                    )
                    break

            except Exception as e:
                print()
                self.logger.exception(f"⚠️ Непредвиденная ошибка в цикле: {e}")
                self._health.fail()
                loop_error_attempt += 1
                delay = calc_backoff(loop_error_attempt, base=5, maximum=60)
                time.sleep(delay)

        self.logger.info(f"✅ Мониторинг завершён | {self._health.summary()}")


# ---------------------------------------------------------------------------
# Маркер исключения для истёкшей сессии
# ---------------------------------------------------------------------------

class _SessionExpired(Exception):
    """Сигнал: сессия истекла, нужна переавторизация."""
    pass