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


USER_AGENT = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
    "AppleWebKit/537.36 (KHTML, like Gecko) "
    "Chrome/145.0.0.0 Safari/537.36"
)

BASE_URL = "https://mangabuff.ru"
REQUEST_TIMEOUT = 15


# ---------------------------------------------------------------------------
# Вспомогательные функции авторизации
# ---------------------------------------------------------------------------

def _get_cookie(jar, name):
    for cookie in jar:
        if cookie.name == name and cookie.value:
            return cookie.value
    return None


def _extract_csrf(html):
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


def _apply_ajax_tokens(session):
    xsrf_raw = _get_cookie(session.cookies, "XSRF-TOKEN")
    if xsrf_raw:
        xsrf = unquote(xsrf_raw)
        session.headers.update({"X-CSRF-TOKEN": xsrf, "X-XSRF-TOKEN": xsrf})


def _nav_headers(referer=None, fetch_site="none"):
    h = {
        "Accept": (
            "text/html,application/xhtml+xml,application/xml;q=0.9,"
            "image/avif,image/webp,image/apng,*/*;q=0.8,"
            "application/signed-exchange;v=b3;q=0.7"
        ),
        "Accept-Language": "ru-RU,ru;q=0.9,en-US;q=0.8,en;q=0.7",
        "Accept-Encoding": "gzip, deflate, br, zstd",
        "Upgrade-Insecure-Requests": "1",
        "Sec-Ch-Ua": '"Not:A-Brand";v="99", "Google Chrome";v="145", "Chromium";v="145"',
        "Sec-Ch-Ua-Mobile": "?0",
        "Sec-Ch-Ua-Platform": '"Windows"',
        "Sec-Fetch-Site": fetch_site,
        "Sec-Fetch-Mode": "navigate",
        "Sec-Fetch-User": "?1",
        "Sec-Fetch-Dest": "document",
        "Priority": "u=0, i",
    }
    if referer:
        h["Referer"] = referer
    return h


# ---------------------------------------------------------------------------
# Монитор
# ---------------------------------------------------------------------------

class MangaBuffMonitor:
    def __init__(self):
        self.config = Config()
        self.telegram = TelegramNotifier(
            self.config.TELEGRAM_BOT_TOKEN,
            self.config.TELEGRAM_CHAT_ID,
        )
        self.session = requests.Session()
        self.session.headers.update({"User-Agent": USER_AGENT})

        self.current_manga = None       # slug текущей манги
        self.current_manga_info = None  # dict с title/image

        # Последние известные данные альянса для определения изменений
        self.last_page_data: dict = {}

        # Трекинг прироста опыта за день
        self.today = date.today()
        self.exp_at_day_start = None
        self.last_known_exp = None

        os.makedirs(self.config.LOG_DIR, exist_ok=True)

    # ------------------------------------------------------------------
    # Логирование
    # ------------------------------------------------------------------

    def log(self, message, force=False):
        timestamp = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        log_message = f"[{timestamp}] {message}"
        print(log_message)
        if force or any(m in message for m in ['✅', '❌', '🔔', '⚠️', '🔐', '🚀', '⏹️']):
            with open(self.config.LOG_FILE, 'a', encoding='utf-8') as f:
                f.write(log_message + '\n')

    # ------------------------------------------------------------------
    # Авторизация
    # ------------------------------------------------------------------

    def login(self):
        try:
            self.log("🔐 Вход в аккаунт...")

            try:
                r0 = self.session.get(BASE_URL, headers=_nav_headers(), timeout=REQUEST_TIMEOUT)
                self.log(f"   [1] GET / → {r0.status_code}, куки: {[c.name for c in self.session.cookies]}")
            except requests.RequestException as e:
                self.log(f"   [1] GET / → ошибка: {e} (продолжаем)")

            try:
                r_get = self.session.get(
                    f"{BASE_URL}/login",
                    headers=_nav_headers(referer=f"{BASE_URL}/", fetch_site="same-origin"),
                    timeout=REQUEST_TIMEOUT,
                )
            except requests.RequestException as e:
                self.log(f"   [2] GET /login → ошибка: {e}")
                return False

            self.log(f"   [2] GET /login → {r_get.status_code}")
            if r_get.status_code != 200:
                self.log(f"   ❌ Неожиданный статус: {r_get.status_code}")
                return False

            csrf = _extract_csrf(r_get.text)
            if not csrf:
                self.log("   ❌ CSRF-токен не найден")
                return False

            self.log(f"   CSRF: {csrf[:30]}...")
            xsrf_raw = _get_cookie(self.session.cookies, "XSRF-TOKEN")
            xsrf = unquote(xsrf_raw) if xsrf_raw else csrf

            ajax_headers = {
                "Accept": "application/json, text/plain, */*",
                "Content-Type": "application/x-www-form-urlencoded; charset=UTF-8",
                "X-Requested-With": "XMLHttpRequest",
                "X-CSRF-TOKEN": xsrf,
                "X-XSRF-TOKEN": xsrf,
                "Origin": BASE_URL,
                "Referer": f"{BASE_URL}/login",
                "Sec-Ch-Ua": '"Not:A-Brand";v="99", "Google Chrome";v="145", "Chromium";v="145"',
                "Sec-Ch-Ua-Mobile": "?0",
                "Sec-Ch-Ua-Platform": '"Windows"',
                "Sec-Fetch-Site": "same-origin",
                "Sec-Fetch-Mode": "cors",
                "Sec-Fetch-Dest": "empty",
            }

            try:
                r_post = self.session.post(
                    f"{BASE_URL}/login",
                    data={
                        "email": self.config.MANGABUFF_EMAIL,
                        "password": self.config.MANGABUFF_PASSWORD,
                        "_token": csrf,
                    },
                    headers=ajax_headers,
                    allow_redirects=True,
                    timeout=REQUEST_TIMEOUT,
                )
            except requests.RequestException as e:
                self.log(f"   [3] POST /login → ошибка: {e}")
                return False

            self.log(f"   [3] POST /login → {r_post.status_code}, URL: {r_post.url}")
            ct = r_post.headers.get("content-type", "")

            if "application/json" in ct:
                try:
                    j = r_post.json()
                    self.log(f"   JSON: {j}")
                except Exception:
                    self.log(f"   JSON parse error: {r_post.text[:200]}")
                    return False
                if j.get("errors") or j.get("message") == "Unauthenticated." or j.get("status") == "error":
                    self.log(f"   ❌ Сервер: {j}")
                    return False
            else:
                self.log(f"   Не JSON: {r_post.text[:300]}")

            try:
                r_main = self.session.get(BASE_URL, timeout=REQUEST_TIMEOUT)
                if "window.isAuth = 1" in r_main.text or "window.isAuth=1" in r_main.text:
                    self.log("   ✅ window.isAuth=1")
                    is_auth = True
                else:
                    m = re.search(r'window\.user_id\s*=\s*(\d+)', r_main.text)
                    uid = m.group(1) if m else "0"
                    self.log(f"   window.user_id={uid}")
                    is_auth = bool(uid and uid != "0")
            except Exception as e:
                self.log(f"   isAuth check error: {e}")
                is_auth = False

            if not is_auth:
                self.log("   ❌ Не авторизованы после POST /login")
                return False

            _apply_ajax_tokens(self.session)
            new_csrf = _extract_csrf(r_post.text if "text/html" in ct else r_main.text)
            if new_csrf:
                self.session.headers.update({"X-CSRF-TOKEN": new_csrf})
            self.session.headers.update({"X-Requested-With": "XMLHttpRequest"})

            self.log("✅ Успешный вход")
            return True

        except Exception as e:
            self.log(f"❌ Ошибка при входе: {e}")
            return False

    # ------------------------------------------------------------------
    # Парсинг страницы альянса
    # ------------------------------------------------------------------

    def _parse_number(self, text):
        if not text:
            return None
        cleaned = re.sub(r'[^\d]', '', text.strip())
        return int(cleaned) if cleaned else None

    def get_alliance_page_data(self):
        """
        Возвращает dict:
          slug, level, exp_current, exp_total, chance
        """
        max_retries = 3
        retry_delay = 5

        for attempt in range(max_retries):
            try:
                response = self.session.get(self.config.ALLIANCE_URL, timeout=15)

                if response.status_code in (500, 503):
                    self.log(f"⚠️ Ошибка сервера {response.status_code} (попытка {attempt+1}/{max_retries})", force=True)
                    if attempt < max_retries - 1:
                        time.sleep(retry_delay)
                        continue
                    return None

                if response.status_code != 200:
                    self.log(f"⚠️ Статус {response.status_code} (попытка {attempt+1}/{max_retries})", force=True)
                    if attempt < max_retries - 1:
                        time.sleep(retry_delay)
                        continue
                    return None

                soup = BeautifulSoup(response.text, 'html.parser')
                result = {}

                # Slug манги
                manga_link = soup.find('a', class_='card-show__placeholder')
                if manga_link:
                    href = manga_link.get('href', '')
                    if href.startswith('/manga/'):
                        result['slug'] = href.replace('/manga/', '')

                if 'slug' not in result:
                    poster = soup.find('div', class_='card-show__header')
                    if poster:
                        style = poster.get('style', '')
                        if 'background-image: url(' in style:
                            try:
                                img_url = style.split("url('")[1].split("'")[0]
                                result['slug'] = img_url.split('/posters/')[-1].replace('.jpg', '')
                            except Exception:
                                pass

                # Уровень
                lv = soup.find('div', class_='alliance__level-value')
                if lv:
                    m = re.search(r'\d+', lv.text)
                    result['level'] = m.group(0) if m else None

                # Текущий опыт
                exp_elem = soup.find('div', class_='alliance__level-exp')
                if exp_elem:
                    result['exp_current'] = self._parse_number(exp_elem.text)

                # Опыт до следующего уровня
                tot_elem = soup.find('div', class_='alliance__level-total-exp')
                if tot_elem:
                    result['exp_total'] = self._parse_number(tot_elem.text)

                # Шанс смены манги
                chance_elem = soup.find('span', class_='alliance__chance-change-manga')
                if chance_elem:
                    result['chance'] = chance_elem.text.strip()

                return result or None

            except (requests.exceptions.Timeout, requests.exceptions.ConnectionError) as e:
                self.log(f"⚠️ Сеть: {e} (попытка {attempt+1}/{max_retries})", force=True)
                if attempt < max_retries - 1:
                    time.sleep(retry_delay)
                    continue
                return None

            except Exception as e:
                self.log(f"⚠️ Ошибка парсинга: {e}", force=True)
                if attempt < max_retries - 1:
                    time.sleep(retry_delay)
                    continue
                return None

        return None

    # ------------------------------------------------------------------
    # Детали манги
    # ------------------------------------------------------------------

    def get_manga_details(self, manga_slug):
        max_retries = 3
        retry_delay = 5

        for attempt in range(max_retries):
            try:
                url = f"{BASE_URL}/manga/{manga_slug}"
                response = self.session.get(url, timeout=15)

                if response.status_code != 200:
                    self.log(f"❌ Ошибка страницы манги: {response.status_code}", force=True)
                    if attempt < max_retries - 1:
                        time.sleep(retry_delay)
                        continue
                    return None

                soup = BeautifulSoup(response.text, 'html.parser')

                title = None
                for cls in ('manga-mobile__name', 'manga__name'):
                    elem = soup.find('h1', class_=cls)
                    if elem:
                        title = elem.text.strip()
                        break
                if not title:
                    title = manga_slug

                img_src = None
                img_elem = soup.find('img', class_='manga-mobile__image')
                if img_elem:
                    img_src = img_elem.get('src')
                if not img_src:
                    wrapper = soup.find('div', class_='manga__img')
                    if wrapper:
                        img = wrapper.find('img')
                        if img:
                            img_src = img.get('src')
                if img_src and img_src.startswith('/'):
                    img_src = f"{BASE_URL}{img_src}"

                self.log(f"✅ Детали манги: {title}", force=True)
                return {
                    'slug': manga_slug,
                    'title': title,
                    'image': img_src,
                    'timestamp': datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
                }

            except (requests.exceptions.Timeout, requests.exceptions.ConnectionError) as e:
                self.log(f"⚠️ Сеть при получении деталей: {e} (попытка {attempt+1}/{max_retries})", force=True)
                if attempt < max_retries - 1:
                    time.sleep(retry_delay)
                    continue
                return None

            except Exception as e:
                self.log(f"❌ Ошибка деталей: {e}", force=True)
                if attempt < max_retries - 1:
                    time.sleep(retry_delay)
                    continue
                return None

        return None

    # ------------------------------------------------------------------
    # Трекинг опыта
    # ------------------------------------------------------------------

    def _update_exp_tracking(self, exp_current):
        if exp_current is None:
            return
        today = date.today()
        if today != self.today:
            self.today = today
            self.exp_at_day_start = exp_current
            self.log(f"📅 Новый день, сброс прироста. Старт: {exp_current}")
        if self.exp_at_day_start is None:
            self.exp_at_day_start = exp_current
        self.last_known_exp = exp_current

    def get_exp_gain_today(self):
        if self.last_known_exp is None or self.exp_at_day_start is None:
            return None
        gain = self.last_known_exp - self.exp_at_day_start
        return gain if gain >= 0 else None

    # ------------------------------------------------------------------
    # Определение изменений в данных альянса
    # ------------------------------------------------------------------

    def _stats_changed(self, new_data: dict) -> bool:
        """Вернёт True, если опыт или шанс изменились относительно last_page_data."""
        if not self.last_page_data:
            return False
        for key in ('exp_current', 'exp_total', 'chance', 'level'):
            if new_data.get(key) != self.last_page_data.get(key):
                return True
        return False

    # ------------------------------------------------------------------
    # История
    # ------------------------------------------------------------------

    def save_history(self, manga_info):
        try:
            try:
                with open(self.config.HISTORY_FILE, 'r', encoding='utf-8') as f:
                    history = json.load(f)
            except FileNotFoundError:
                history = []
            except json.JSONDecodeError:
                history = []

            history.append(manga_info)
            if len(history) > 100:
                history = history[-100:]

            with open(self.config.HISTORY_FILE, 'w', encoding='utf-8') as f:
                json.dump(history, f, ensure_ascii=False, indent=2)

            self.log(f"💾 История сохранена ({len(history)} записей)")
        except Exception as e:
            self.log(f"⚠️ Ошибка истории: {e}")

    # ------------------------------------------------------------------
    # Основной цикл
    # ------------------------------------------------------------------

    def start(self):
        try:
            self.log("🔧 Проверка конфигурации...")
            self.config.validate()

            if not self.login():
                self.log("❌ Не удалось авторизоваться")
                return

            self.log("📚 Получаю данные альянса...")
            page_data = self.get_alliance_page_data()

            if page_data and page_data.get('slug'):
                self.current_manga = page_data['slug']
                self.last_page_data = page_data
                self._update_exp_tracking(page_data.get('exp_current'))

                self.log(
                    f"📚 Тайтл: {self.current_manga} | "
                    f"Опыт: {page_data.get('exp_current')}/{page_data.get('exp_total')} | "
                    f"Шанс: {page_data.get('chance')}%"
                )

                manga_info = self.get_manga_details(self.current_manga)
                self.current_manga_info = manga_info

                if manga_info:
                    caption = self.telegram.format_manga_caption(
                        manga_info, page_data, self.get_exp_gain_today(), is_startup=True
                    )
                    if manga_info['image']:
                        self.telegram.send_photo_to_all_topics(manga_info['image'], caption)
                    else:
                        self.telegram.send_message_to_all_topics(caption)
            else:
                self.log("⚠️ Не удалось получить тайтл альянса")
                self.telegram.send_message_to_all_topics("⚠️ Не удалось получить тайтл альянса")

            self.log(f"👀 Интервал: {self.config.CHECK_INTERVAL} сек | Ctrl+C для остановки", force=True)

            check_count = 0
            while True:
                try:
                    check_count += 1

                    if check_count % 60 == 0:
                        self.log(f"🔍 #{check_count} тайтл: {self.current_manga}", force=True)
                    else:
                        print(f"\r🔍 Проверка #{check_count}... ", end='', flush=True)

                    page_data = self.get_alliance_page_data()

                    if page_data:
                        self._update_exp_tracking(page_data.get('exp_current'))
                        new_slug = page_data.get('slug')

                        # --- Смена тайтла ---
                        if new_slug and new_slug != self.current_manga:
                            print()
                            self.log(f"🔔 СМЕНА ТАЙТЛА: {self.current_manga} → {new_slug}", force=True)

                            manga_info = self.get_manga_details(new_slug)
                            self.current_manga_info = manga_info

                            if manga_info:
                                caption = self.telegram.format_manga_caption(
                                    manga_info, page_data, self.get_exp_gain_today(), is_startup=False
                                )
                                if manga_info['image']:
                                    self.telegram.send_photo_to_all_topics(manga_info['image'], caption)
                                else:
                                    self.telegram.send_message_to_all_topics(caption)

                                self.save_history(manga_info)
                                self.log("✅ Уведомление отправлено", force=True)
                            else:
                                self.telegram.send_message_to_all_topics(
                                    f"🔔 <b>Смена тайтла!</b>\n\n{new_slug}\n(детали недоступны)"
                                )

                            self.current_manga = new_slug
                            self.last_page_data = page_data

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

                        else:
                            # Ничего не изменилось
                            pass

                    else:
                        if check_count % 60 == 0 or check_count == 1:
                            self.log("⚠️ Нет данных альянса", force=True)

                    time.sleep(self.config.CHECK_INTERVAL)

                except KeyboardInterrupt:
                    print()
                    self.log("⏹️ Остановка...")
                    self.telegram.send_message_to_all_topics("⏹️ Мониторинг остановлен")
                    break

                except requests.exceptions.RequestException as e:
                    self.log(f"⚠️ Ошибка сети: {e}")
                    time.sleep(30)
                    self.log("🔐 Переавторизация...")
                    if not self.login():
                        self.log("❌ Переавторизация не удалась")
                        self.telegram.send_message_to_all_topics("❌ Ошибка сети. Мониторинг остановлен.")
                        break

                except Exception as e:
                    self.log(f"⚠️ Непредвиденная ошибка: {e}")
                    import traceback
                    self.log(traceback.format_exc())
                    time.sleep(5)

        except ValueError as e:
            self.log(f"❌ Конфигурация: {e}")

        except Exception as e:
            self.log(f"❌ Критическая ошибка: {e}")
            import traceback
            self.log(traceback.format_exc())

        finally:
            self.log("✅ Мониторинг завершён")