#!/usr/bin/env python3
import asyncio
import io
import json
import os
import re
from datetime import datetime, timezone
from time import time
from types import SimpleNamespace
from typing import Dict, List, Optional
from urllib.parse import urljoin

import cloudscraper
from bs4 import BeautifulSoup
from pyrogram.enums import ChatType
from pyrogram.errors import RPCError
from pyrogram.filters import command
from pyrogram.handlers import MessageHandler

from bot import LOGGER, OWNER_ID, bot, config_dict
from bot.helper.telegram_helper.bot_commands import BotCommands
from bot.helper.telegram_helper.filters import CustomFilters
from bot.helper.telegram_helper.message_utils import sendMessage
from bot.modules.mirror_leech import _mirror_leech

_DEFAULT_TBL_FEED = "https://www.1tamilmv.land/"
_FEEDS: List[str] = [url for url in (config_dict.get('AUTO_TBL_FEEDS') or [_DEFAULT_TBL_FEED]) if url]
_MAX_TOPICS = 15
_POLL_DELAY = 900  # seconds
_STATE_FILE = "auto_tbl_state.json"
_STATE_LIMIT = 500
_THUMBNAIL_URL = config_dict.get('AUTO_TBL_THUMBNAIL') or ''
_THUMBNAIL_PATH = "auto_tbl_thumbnail.jpg"

_monitor_lock = asyncio.Lock()
_state_lock = asyncio.Lock()
_monitor_task: asyncio.Task | None = None
_monitor_last_links: set[str] = set()
_monitor_seen_topics: set[str] = set()
_monitor_command_chat: int | str | None = None
_command_chat_cache: Optional[tuple[int | str, SimpleNamespace]] = None
_thumbnail_cached: Optional[str] = None


class _AutoUser:
    def __init__(self, user_id: int, name: str = "TamilAuto") -> None:
        self.id = user_id
        self.first_name = name
        self.username = None
        self.is_bot = False
        self.is_self = False

    def mention(self, style: str = "html") -> str:
        style = (style or "").lower()
        if style in {"html"}:
            return f"<a href='tg://user?id={self.id}'>{self.first_name}</a>"
        if style in {"markdown", "markdownv2", "md", "mdv2"}:
            safe_name = (
                self.first_name.replace("\\", "\\\\")
                .replace("[", "\\[")
                .replace("]", "\\]")
                .replace("(", "\\(")
                .replace(")", "\\)")
            )
            return f"[{safe_name}](tg://user?id={self.id})"
        return self.first_name


class _AutoCommandMessage:
    def __init__(self, chat: SimpleNamespace, link: str, title: str) -> None:
        self.chat = chat
        self.id = int(time() * 1000)
        self.message_id = self.id
        self.text = f"/{BotCommands.QbLeechCommand[0]} {link}"
        self.reply_to_message = None
        self.reply_to_message_id = None
        self.sender_chat = None
        self.link = ""
        self.from_user = _AutoUser(OWNER_ID)
        self.title = title
        self.date = datetime.now(timezone.utc)

    async def reply(self, text: str, **kwargs):
        kwargs.pop('quote', None)
        kwargs.pop('reply_to_message_id', None)
        return await bot.send_message(self.chat.id, text=text, **kwargs)

    async def reply_photo(self, photo, caption: str | None = None, **kwargs):
        kwargs.pop('quote', None)
        kwargs.pop('reply_to_message_id', None)
        return await bot.send_photo(self.chat.id, photo=photo, caption=caption, **kwargs)

    async def unpin(self):
        return


def _load_monitor_state() -> None:
    if not os.path.exists(_STATE_FILE):
        return
    try:
        with open(_STATE_FILE, 'r', encoding='utf-8') as state_file:
            data = json.load(state_file)
        _monitor_last_links.update(data.get('last_links', []))
        _monitor_seen_topics.update(data.get('seen_topics', []))
        LOGGER.info(
            "Restored %d TamilBlasters links and %d topics from %s",
            len(_monitor_last_links),
            len(_monitor_seen_topics),
            _STATE_FILE,
        )
    except Exception as exc:
        LOGGER.warning("Failed to load AUTO_TBL state: %s", exc)


async def _persist_monitor_state() -> None:
    async with _state_lock:
        payload = {
            "last_links": list(_monitor_last_links),
            "seen_topics": list(_monitor_seen_topics),
        }
        tmp_path = f"{_STATE_FILE}.tmp"
        try:
            with open(tmp_path, 'w', encoding='utf-8') as tmp:
                json.dump(payload, tmp)
            os.replace(tmp_path, _STATE_FILE)
        except Exception as exc:
            LOGGER.warning("Failed to persist AUTO_TBL state: %s", exc)


async def _remember_link(link: str) -> None:
    _monitor_last_links.add(link)
    while len(_monitor_last_links) > _STATE_LIMIT:
        try:
            _monitor_last_links.pop()
        except KeyError:
            break
    await _persist_monitor_state()


async def _remember_topic(topic_url: str) -> None:
    if topic_url in _monitor_seen_topics:
        return
    _monitor_seen_topics.add(topic_url)
    await _persist_monitor_state()


def _get_thumbnail_path() -> Optional[str]:
    global _thumbnail_cached
    if not _THUMBNAIL_URL:
        return None
    if _thumbnail_cached and os.path.exists(_thumbnail_cached):
        return _thumbnail_cached
    try:
        scraper = cloudscraper.create_scraper()
        resp = scraper.get(_THUMBNAIL_URL, timeout=10)
        resp.raise_for_status()
        with open(_THUMBNAIL_PATH, 'wb') as thumb_file:
            thumb_file.write(resp.content)
        _thumbnail_cached = _THUMBNAIL_PATH
        LOGGER.info("Downloaded AUTO_TBL thumbnail to %s", _THUMBNAIL_PATH)
    except Exception as exc:
        LOGGER.error("Failed to fetch AUTO_TBL thumbnail: %s", exc)
        _thumbnail_cached = None
    return _thumbnail_cached


_load_monitor_state()


def _extract_size(text: str) -> str:
    match = re.search(r"(\d+(?:\.\d+)?\s*(?:GB|MB|KB))", text, re.IGNORECASE)
    return match.group(1) if match else "Unknown"


def _crawl_tbl_sync() -> List[Dict[str, object]]:
    torrents: List[Dict[str, object]] = []
    for base_url in _FEEDS:
        scraper = cloudscraper.create_scraper()
        try:
            resp = scraper.get(base_url, timeout=10)
            resp.raise_for_status()
        except Exception as exc:
            LOGGER.error("Failed to fetch TBL feed %s: %s", base_url, exc)
            continue

        soup = BeautifulSoup(resp.text, "html.parser")
        topic_links = [
            a["href"] for a in soup.find_all("a", href=re.compile(r"/forums/topic/")) if a.get("href")
        ]

        for rel_url in list(dict.fromkeys(topic_links))[:_MAX_TOPICS]:
            try:
                full_url = rel_url if rel_url.startswith("http") else urljoin(base_url, rel_url)
                dresp = scraper.get(full_url, timeout=10)
                dresp.raise_for_status()
                post_soup = BeautifulSoup(dresp.text, "html.parser")

                torrent_tags = post_soup.find_all("a", attrs={"data-fileext": "torrent"})
                file_links = []
                for tag in torrent_tags:
                    href = tag.get("href")
                    if not href:
                        continue
                    link = href.strip()
                    raw_text = tag.get_text(strip=True)
                    title = (
                        raw_text.replace("https://www.1tamilmv.mba/", "")
                        .rstrip(".torrent")
                        .strip()
                    )
                    size = _extract_size(raw_text)

                    file_links.append(
                        {
                            "type": "torrent",
                            "title": title,
                            "link": link,
                            "size": size,
                        }
                    )

                if file_links:
                    torrents.append(
                        {
                            "topic_url": full_url,
                            "title": file_links[0]["title"],
                            "size": file_links[0]["size"],
                            "links": file_links,
                        }
                    )
            except Exception as post_err:
                LOGGER.error("Failed to parse TBL topic %s from %s: %s", rel_url, base_url, post_err)

    return torrents


def _download_torrent_sync(url: str) -> io.BytesIO | None:
    scraper = cloudscraper.create_scraper()
    try:
        resp = scraper.get(url, timeout=10)
        resp.raise_for_status()
    except Exception as exc:
        LOGGER.error("Error downloading torrent file %s: %s", url, exc)
        return None
    return io.BytesIO(resp.content)


async def _crawl_tbl() -> List[Dict[str, object]]:
    return await asyncio.to_thread(_crawl_tbl_sync)


async def _download_torrent(url: str) -> io.BytesIO | None:
    return await asyncio.to_thread(_download_torrent_sync, url)


def _sanitize_filename(name: str) -> str:
    if not name:
        return "tbl_torrent.torrent"
    safe = re.sub(r"[^0-9A-Za-z._-]+", "_", name).strip("_")
    return f"{safe or 'tbl_torrent'}.torrent"


async def _resolve_command_chat(chat_ref: int | str) -> Optional[SimpleNamespace]:
    global _command_chat_cache
    if _command_chat_cache and _command_chat_cache[0] == chat_ref:
        return _command_chat_cache[1]
    try:
        chat = await bot.get_chat(chat_ref)
    except RPCError as exc:
        LOGGER.error("Failed to resolve AUTO_TBL command chat %s: %s", chat_ref, exc)
        return None
    chat_stub = SimpleNamespace(id=chat.id, type=chat.type if chat.type else ChatType.SUPERGROUP)
    _command_chat_cache = (chat_ref, chat_stub)
    return chat_stub


async def _trigger_qbleech(link: str, title: str) -> None:
    chat_ref = _monitor_command_chat or config_dict.get('AUTO_TBL_COMMAND_CHAT') or config_dict.get('AUTO_TBL_CHANNEL')
    if not chat_ref:
        LOGGER.info("AUTO_TBL_COMMAND_CHAT not configured; skipping qBittorrent trigger for %s", link)
        return
    chat = await _resolve_command_chat(chat_ref)
    if chat is None:
        return
    if chat.type is None:
        chat.type = ChatType.SUPERGROUP if str(chat.id).startswith('-100') else ChatType.PRIVATE
    auto_message = _AutoCommandMessage(chat, link, title or "TamilAuto")
    try:
        _mirror_leech(bot, auto_message, isQbit=True, isLeech=True)
        LOGGER.info("Queued TamilBlasters torrent for qBittorrent: %s", title or link)
    except Exception as exc:
        LOGGER.error("Failed to start qBittorrent download for %s: %s", link, exc)


async def _tamilblasters_monitor(upload_chat: int | str) -> None:
    global _monitor_task
    try:
        while True:
            try:
                torrents = await _crawl_tbl()
            except Exception as exc:
                LOGGER.error("Unexpected error while crawling TBL: %s", exc)
                torrents = []

            for torrent in torrents:
                topic_url = torrent["topic_url"]
                links: List[Dict[str, str]] = torrent.get("links", [])
                new_files = [f for f in links if f["link"] not in _monitor_last_links]
                if topic_url in _monitor_seen_topics and not new_files:
                    continue

                for file_ in new_files:
                    file_bytes = await _download_torrent(file_["link"])
                    if file_bytes is None:
                        continue
                    filename = _sanitize_filename(file_["title"])
                    caption = f"{file_['title']}\n\ud83d\udce6 {file_['size']}\n#tmv torrent file"
                    file_bytes.seek(0)
                    try:
                        thumb_path = _get_thumbnail_path()
                        send_kwargs = {"file_name": filename, "caption": caption}
                        if thumb_path:
                            send_kwargs["thumb"] = thumb_path
                        await bot.send_document(upload_chat, file_bytes, **send_kwargs)
                        await _remember_link(file_["link"])
                        LOGGER.info("Posted TamilBlasters torrent: %s", file_["title"])
                    except RPCError as exc:
                        LOGGER.error("Failed to send torrent file %s: %s", file_["link"], exc)
                        continue

                    await _trigger_qbleech(file_["link"], file_["title"])

                await _remember_topic(topic_url)

            await asyncio.sleep(_POLL_DELAY)
    except asyncio.CancelledError:
        LOGGER.info("TamilBlasters auto-leech monitor stopped")
        raise
    finally:
        async with _monitor_lock:
            _monitor_task = None


async def _start_monitor(message) -> None:
    upload_chat = config_dict.get('AUTO_TBL_CHANNEL')
    if not upload_chat:
        await sendMessage(message, "AUTO_TBL_CHANNEL is not configured. Set it in the environment to enable this feature.")
        return

    async with _monitor_lock:
        global _monitor_task, _monitor_command_chat, _command_chat_cache
        if _monitor_task and not _monitor_task.done():
            await sendMessage(message, "TamilBlasters auto-leech is already running.")
            return
        _monitor_command_chat = config_dict.get('AUTO_TBL_COMMAND_CHAT') or message.chat.id
        _command_chat_cache = None
        _monitor_task = bot.loop.create_task(_tamilblasters_monitor(upload_chat))
    await sendMessage(message, "TamilBlasters auto-leech started. New torrents will be posted automatically.")


async def _stop_monitor(message) -> None:
    async with _monitor_lock:
        global _monitor_task
        if not _monitor_task or _monitor_task.done():
            await sendMessage(message, "TamilBlasters auto-leech is not running.")
            return
        _monitor_task.cancel()
    try:
        await _monitor_task
    except asyncio.CancelledError:
        pass
    await sendMessage(message, "TamilBlasters auto-leech stopped.")


async def tamil_auto_leech_handler(client, message):
    args = message.text.split(maxsplit=1)
    if len(args) > 1 and args[1].strip().lower() == 'stop':
        await _stop_monitor(message)
        return
    await _start_monitor(message)


bot.add_handler(
    MessageHandler(
        tamil_auto_leech_handler,
        filters=command(BotCommands.AutoTamilCommand) & CustomFilters.sudo & ~CustomFilters.blacklisted,
    )
)
