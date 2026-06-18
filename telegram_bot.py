"""
Telegram-уведомления с retry, обработкой 429 и очередью сообщений.
"""

import time
import logging
from collections import deque
from datetime import datetime
from typing import Optional

import requests

from utils import calc_backoff

# Темы для рассылки: None = General, int = конкретная тема
TOPIC_IDS = [None, 3]

# Максимум попыток для одного Telegram-запроса
TG_MAX_RETRIES = 4

# Максимальный размер очереди отложенных сообщений
MAX_QUEUE_SIZE = 50


class TelegramNotifier:
    def __init__(self, bot_token: str, chat_id: str, logger: logging.Logger = None):
        self.bot_token = bot_token
        self.chat_id = chat_id
        self.api_url = f"https://api.telegram.org/bot{bot_token}"
        self.logger = logger or logging.getLogger(__name__)

        # message_id последнего сообщения по теме (None или int) → message_id
        self.active_message_ids: dict = {}

        # Очередь отложенных сообщений при ошибке сети
        # Элемент: {"type": "photo"|"text", "args": {...}}
        self._queue: deque = deque(maxlen=MAX_QUEUE_SIZE)

    # ------------------------------------------------------------------
    # Внутренние: низкоуровневые запросы к Telegram API
    # ------------------------------------------------------------------

    def _tg_post(self, method: str, data: dict, files=None) -> Optional[dict]:
        """
        POST к Telegram API с retry и обработкой 429 / сетевых ошибок.
        Возвращает result-dict или None при неудаче.
        """
        url = f"{self.api_url}/{method}"

        for attempt in range(TG_MAX_RETRIES):
            try:
                resp = requests.post(url, data=data, files=files, timeout=15)

                # 429 Too Many Requests — уважаем Retry-After
                if resp.status_code == 429:
                    retry_after = int(resp.headers.get("Retry-After", 5))
                    self.logger.warning(
                        f"[TG] 429 Rate limit на {method}, ждём {retry_after}с"
                    )
                    time.sleep(retry_after + 1)
                    continue

                # Временные ошибки сервера
                if resp.status_code in (500, 502, 503, 504):
                    delay = calc_backoff(attempt)
                    self.logger.warning(
                        f"[TG] {resp.status_code} на {method}, попытка {attempt+1}/{TG_MAX_RETRIES}, "
                        f"пауза {delay:.1f}с"
                    )
                    time.sleep(delay)
                    continue

                if resp.status_code != 200:
                    body = resp.json() if resp.content else {}
                    err = body.get("description", resp.text[:120])
                    self.logger.error(f"[TG] {method} → HTTP {resp.status_code}: {err}")
                    return None

                result = resp.json()
                if not result.get("ok"):
                    err = result.get("description", "unknown")
                    # "message is not modified" — не ошибка
                    if "not modified" in err.lower():
                        return result.get("result")
                    self.logger.warning(f"[TG] {method} not ok: {err}")
                    return None

                return result.get("result")

            except requests.exceptions.Timeout:
                delay = calc_backoff(attempt)
                self.logger.warning(
                    f"[TG] Timeout {method}, попытка {attempt+1}/{TG_MAX_RETRIES}, пауза {delay:.1f}с"
                )
                time.sleep(delay)

            except requests.exceptions.ConnectionError as exc:
                delay = calc_backoff(attempt)
                self.logger.warning(
                    f"[TG] ConnectionError {method}: {exc}, попытка {attempt+1}/{TG_MAX_RETRIES}, "
                    f"пауза {delay:.1f}с"
                )
                time.sleep(delay)

            except Exception as exc:
                self.logger.error(f"[TG] Неожиданная ошибка {method}: {exc}")
                return None

        self.logger.error(f"[TG] {method} — исчерпаны все {TG_MAX_RETRIES} попытки")
        return None

    # ------------------------------------------------------------------
    # Внутренние: конкретные методы API
    # ------------------------------------------------------------------

    def _send_photo(
        self,
        photo_url: str,
        caption: str,
        message_thread_id=None,
    ) -> Optional[int]:
        if photo_url and photo_url.startswith("/"):
            photo_url = f"https://mangabuff.ru{photo_url}"

        data = {
            "chat_id": self.chat_id,
            "photo": photo_url,
            "caption": caption,
            "parse_mode": "HTML",
        }
        if message_thread_id is not None:
            data["message_thread_id"] = message_thread_id

        result = self._tg_post("sendPhoto", data)
        if result:
            msg_id = result.get("message_id")
            label = f"тема {message_thread_id}" if message_thread_id is not None else "General"
            self.logger.info(f"[TG] ✅ Фото → {label} (msg_id={msg_id})")
            return msg_id
        return None

    def _send_message(self, text: str, message_thread_id=None) -> Optional[int]:
        data = {
            "chat_id": self.chat_id,
            "text": text,
            "parse_mode": "HTML",
        }
        if message_thread_id is not None:
            data["message_thread_id"] = message_thread_id

        result = self._tg_post("sendMessage", data)
        if result:
            return result.get("message_id")
        return None

    def _edit_caption(self, message_id: int, caption: str) -> bool:
        data = {
            "chat_id": self.chat_id,
            "message_id": message_id,
            "caption": caption,
            "parse_mode": "HTML",
        }
        result = self._tg_post("editMessageCaption", data)
        return result is not None

    # ------------------------------------------------------------------
    # Очередь отложенных сообщений
    # ------------------------------------------------------------------

    def _enqueue(self, item: dict):
        """Добавляет сообщение в очередь для повторной отправки."""
        self._queue.append(item)
        self.logger.warning(f"[TG] Сообщение поставлено в очередь (queue={len(self._queue)})")

    def flush_queue(self):
        """Пытается отправить все отложенные сообщения. Вызывать периодически."""
        if not self._queue:
            return
        self.logger.info(f"[TG] Отправка очереди ({len(self._queue)} шт.)...")
        while self._queue:
            item = self._queue.popleft()
            if item["type"] == "photo":
                self.send_photo_to_all_topics(
                    item["photo_url"], item["caption"], from_queue=True
                )
            elif item["type"] == "text":
                self.send_message_to_all_topics(item["text"], from_queue=True)

    # ------------------------------------------------------------------
    # Публичные методы
    # ------------------------------------------------------------------

    def send_photo_to_all_topics(
        self,
        photo_url: str,
        caption: str,
        from_queue: bool = False,
    ):
        """Отправляет фото во все темы; при неудаче кладёт в очередь."""
        self.active_message_ids.clear()
        failed = False

        for topic_id in TOPIC_IDS:
            msg_id = self._send_photo(photo_url, caption, topic_id)
            if msg_id:
                self.active_message_ids[topic_id] = msg_id
            else:
                failed = True

        if failed and not from_queue:
            self._enqueue({"type": "photo", "photo_url": photo_url, "caption": caption})

    def send_message_to_all_topics(self, text: str, from_queue: bool = False):
        """Отправляет текст во все темы; при неудаче кладёт в очередь."""
        failed = False
        for topic_id in TOPIC_IDS:
            result = self._send_message(text, topic_id)
            if result is None:
                failed = True
        if failed and not from_queue:
            self._enqueue({"type": "text", "text": text})

    def update_caption_in_all_topics(self, caption: str):
        """Тихо редактирует подпись во всех активных сообщениях."""
        if not self.active_message_ids:
            return
        for topic_id, msg_id in self.active_message_ids.items():
            label = f"тема {topic_id}" if topic_id is not None else "General"
            ok = self._edit_caption(msg_id, caption)
            if ok:
                self.logger.debug(f"[TG] 📝 Подпись обновлена ({label})")
            else:
                self.logger.warning(f"[TG] Не удалось обновить подпись ({label})")

    # ------------------------------------------------------------------
    # Форматирование подписи
    # ------------------------------------------------------------------

    def format_manga_caption(
        self,
        manga_info: dict,
        page_data: dict = None,
        exp_gain_today: int = None,
        is_startup: bool = False,
    ) -> str:
        title = manga_info.get("title", "—")
        lines: list[str] = []

        if is_startup:
            lines.append("🚀 <b>Монитор запущен</b>")
            lines.append("")

        lines.append(f"📚 <code>{title}</code>")
        lines.append("")

        if page_data:
            exp_cur = page_data.get("exp_current")
            exp_tot = page_data.get("exp_total")
            chance  = page_data.get("chance")

            if exp_cur is not None and exp_tot is not None:
                cur_fmt = f"{exp_cur:,}".replace(",", " ")
                tot_fmt = f"{exp_tot:,}".replace(",", " ")
                lines.append(f"⭐ Опыт: {cur_fmt} / {tot_fmt}")
            elif exp_cur is not None:
                lines.append(f"⭐ Опыт: {str(exp_cur):,}".replace(",", " "))

            if chance is not None:
                lines.append(f"🎲 Шанс смены: {chance}%")

        lines.append("")
        lines.append(
            '🔗 <a href="https://mangabuff.ru/alliances/10/boost">Перейти к вкладке альянса</a>'
        )
        lines.append("")
        lines.append(f"🕐 {datetime.now().strftime('%d.%m.%Y %H:%M:%S')}")

        if exp_gain_today is not None:
            gain_fmt = f"{exp_gain_today:,}".replace(",", " ")
            lines.append(f"📈 Прирост за сегодня: +{gain_fmt} опыта")
        else:
            lines.append("📈 Прирост за сегодня: —")

        return "\n".join(lines)