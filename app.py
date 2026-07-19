import asyncio
import base64
import datetime as dt
import html
import json
import logging
import os
import re
import sqlite3
import time
from dataclasses import dataclass
from collections import Counter
from pathlib import Path
from typing import Optional
from zoneinfo import ZoneInfo

import aiohttp
from dotenv import load_dotenv
import qrcode
from pyrogram import Client, filters, raw, types, utils
from pyrogram.errors import SessionPasswordNeeded
from pyrogram.enums import ChatType
from pyrogram.handlers import MessageHandler, RawUpdateHandler
from telethon import TelegramClient
from telethon.errors import PasswordHashInvalidError, SessionPasswordNeededError
from telethon.sessions import StringSession

load_dotenv()
logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger("dialoghub")
PROJECT_COLORS = (0x6FB9F0, 0xFFD67E, 0xCB86DB, 0x8EEE98, 0xFF93B2, 0xFB6F5F)
REPORT_TZ = ZoneInfo("Europe/Moscow")


@dataclass(frozen=True)
class Settings:
    token: str
    api_id: int
    api_hash: str
    admins: set[int]
    sessions_dir: Path
    db_path: Path

    @classmethod
    def from_env(cls):
        required = ("BOT_TOKEN", "API_ID", "API_HASH")
        missing = [name for name in required if not os.getenv(name)]
        if missing:
            raise RuntimeError(f"Missing environment variables: {', '.join(missing)}")
        return cls(
            token=os.environ["BOT_TOKEN"], api_id=int(os.environ["API_ID"]),
            api_hash=os.environ["API_HASH"],
            admins={int(x) for x in os.getenv("ADMIN_IDS", "").split(",") if x.strip()},
            sessions_dir=Path(os.getenv("ACCOUNT_SESSIONS_DIR", "sessions")),
            db_path=Path(os.getenv("DATABASE_PATH", "data/dialoghub.sqlite3")),
        )


class Store:
    def __init__(self, path: Path):
        path.parent.mkdir(parents=True, exist_ok=True)
        self.db = sqlite3.connect(path)
        self.db.row_factory = sqlite3.Row
        self.db.executescript("""
        PRAGMA foreign_keys = ON;
        CREATE TABLE IF NOT EXISTS settings (key TEXT PRIMARY KEY, value TEXT NOT NULL);
        CREATE TABLE IF NOT EXISTS projects (id INTEGER PRIMARY KEY, name TEXT UNIQUE NOT NULL, color INTEGER, hub_chat_id INTEGER);
        CREATE TABLE IF NOT EXISTS hubs (chat_id INTEGER PRIMARY KEY, title TEXT NOT NULL);
        CREATE TABLE IF NOT EXISTS accounts (
          id INTEGER PRIMARY KEY, session_name TEXT UNIQUE NOT NULL, project_id INTEGER,
          title TEXT, enabled INTEGER NOT NULL DEFAULT 1,
          FOREIGN KEY(project_id) REFERENCES projects(id));
        CREATE TABLE IF NOT EXISTS dialogs (
          account_id INTEGER NOT NULL, peer_id INTEGER NOT NULL, topic_id INTEGER NOT NULL,
          peer_name TEXT, imported INTEGER NOT NULL DEFAULT 0, hub_chat_id INTEGER NOT NULL DEFAULT 0, PRIMARY KEY(account_id, peer_id), UNIQUE(hub_chat_id, topic_id),
          FOREIGN KEY(account_id) REFERENCES accounts(id));
        CREATE TABLE IF NOT EXISTS user_states (
          user_id INTEGER PRIMARY KEY, action TEXT NOT NULL, payload TEXT);
        CREATE TABLE IF NOT EXISTS copied_messages (
          account_id INTEGER NOT NULL, peer_id INTEGER NOT NULL, source_message_id INTEGER NOT NULL,
          PRIMARY KEY(account_id, peer_id, source_message_id));
        CREATE TABLE IF NOT EXISTS outreach_messages (
          id INTEGER PRIMARY KEY, account_id INTEGER NOT NULL, peer_id INTEGER NOT NULL,
          source_message_id INTEGER NOT NULL, project_id INTEGER, script_label TEXT NOT NULL,
          sent_at INTEGER NOT NULL, replied_at INTEGER,
          UNIQUE(account_id, peer_id, source_message_id));
        CREATE TABLE IF NOT EXISTS report_topics (
          project_id INTEGER PRIMARY KEY, topic_id INTEGER NOT NULL,
          FOREIGN KEY(project_id) REFERENCES projects(id));
        """)
        columns = {row[1] for row in self.db.execute("PRAGMA table_info(projects)")}
        if "color" not in columns:
            self.db.execute("ALTER TABLE projects ADD COLUMN color INTEGER")
        if "hub_chat_id" not in columns:
            self.db.execute("ALTER TABLE projects ADD COLUMN hub_chat_id INTEGER")
        dialog_columns = {row[1] for row in self.db.execute("PRAGMA table_info(dialogs)")}
        if "hub_chat_id" not in dialog_columns:
            default_hub = int(self.get("hub_chat_id") or 0)
            self.db.executescript("""
            CREATE TABLE dialogs_v2 (
              account_id INTEGER NOT NULL, peer_id INTEGER NOT NULL, topic_id INTEGER NOT NULL,
              peer_name TEXT, imported INTEGER NOT NULL DEFAULT 0, hub_chat_id INTEGER NOT NULL,
              PRIMARY KEY(account_id, peer_id), UNIQUE(hub_chat_id, topic_id),
              FOREIGN KEY(account_id) REFERENCES accounts(id));
            """)
            self.db.execute("INSERT INTO dialogs_v2(account_id,peer_id,topic_id,peer_name,imported,hub_chat_id) SELECT account_id,peer_id,topic_id,peer_name,imported,? FROM dialogs", (default_hub,))
            self.db.execute("DROP TABLE dialogs")
            self.db.execute("ALTER TABLE dialogs_v2 RENAME TO dialogs")
        if "imported" not in dialog_columns:
            self.db.execute("ALTER TABLE dialogs ADD COLUMN imported INTEGER NOT NULL DEFAULT 0")
        default_hub = int(self.get("hub_chat_id") or 0)
        if default_hub:
            self.db.execute("INSERT OR IGNORE INTO hubs(chat_id,title) VALUES(?,?)", (default_hub, "Основная CRM"))
            self.db.execute("UPDATE projects SET hub_chat_id=? WHERE hub_chat_id IS NULL", (default_hub,))
        for project in self.db.execute("SELECT id FROM projects WHERE color IS NULL").fetchall():
            self.db.execute("UPDATE projects SET color=? WHERE id=?", (PROJECT_COLORS[(project["id"] - 1) % len(PROJECT_COLORS)], project["id"]))
        self.db.commit()

    def get(self, key: str) -> Optional[str]:
        row = self.db.execute("SELECT value FROM settings WHERE key=?", (key,)).fetchone()
        return row["value"] if row else None

    def set(self, key: str, value: str):
        self.db.execute("INSERT INTO settings(key,value) VALUES(?,?) ON CONFLICT(key) DO UPDATE SET value=excluded.value", (key, value))
        self.db.commit()

    def add_project(self, name: str):
        self.db.execute("INSERT OR IGNORE INTO projects(name) VALUES(?)", (name,)); self.db.commit()
        row = self.db.execute("SELECT id, color FROM projects WHERE name=?", (name,)).fetchone()
        if row and row["color"] is None:
            self.db.execute("UPDATE projects SET color=? WHERE id=?", (PROJECT_COLORS[(row["id"] - 1) % len(PROJECT_COLORS)], row["id"])); self.db.commit()

    def register_hub(self, chat_id: int, title: str):
        self.db.execute("INSERT INTO hubs(chat_id,title) VALUES(?,?) ON CONFLICT(chat_id) DO UPDATE SET title=excluded.title", (chat_id, title)); self.db.commit()

    def hubs(self):
        return self.db.execute("SELECT * FROM hubs ORDER BY title").fetchall()

    def bind_project_hub(self, project_id: int, chat_id: int):
        self.db.execute("UPDATE projects SET hub_chat_id=? WHERE id=?", (chat_id, project_id)); self.db.commit()

    def project_hub(self, project_id: int) -> int:
        row = self.db.execute("SELECT hub_chat_id FROM projects WHERE id=?", (project_id,)).fetchone()
        return int(row["hub_chat_id"] or self.get("hub_chat_id") or 0) if row else 0

    def projects(self):
        return self.db.execute("SELECT * FROM projects ORDER BY name").fetchall()

    def project_id(self, name: str) -> Optional[int]:
        row = self.db.execute("SELECT id FROM projects WHERE lower(name)=lower(?)", (name,)).fetchone()
        return row["id"] if row else None

    def add_account(self, session_name: str, project_id: int, title: str):
        self.db.execute("INSERT INTO accounts(session_name,project_id,title) VALUES(?,?,?) ON CONFLICT(session_name) DO UPDATE SET project_id=excluded.project_id,title=excluded.title,enabled=1", (session_name, project_id, title))
        self.db.commit()

    def accounts(self):
        return self.db.execute("SELECT a.*, p.name project_name FROM accounts a LEFT JOIN projects p ON p.id=a.project_id ORDER BY a.id").fetchall()

    def account(self, session_name: str):
        return self.db.execute("SELECT * FROM accounts WHERE session_name=? AND enabled=1", (session_name,)).fetchone()

    def dialog(self, account_id: int, peer_id: int):
        return self.db.execute("SELECT * FROM dialogs WHERE account_id=? AND peer_id=?", (account_id, peer_id)).fetchone()

    def by_topic(self, chat_id: int, topic_id: int):
        return self.db.execute("SELECT d.*,a.session_name,a.title,p.name project_name FROM dialogs d JOIN accounts a ON a.id=d.account_id LEFT JOIN projects p ON p.id=a.project_id WHERE d.hub_chat_id=? AND d.topic_id=?", (chat_id, topic_id)).fetchone()

    def dialogs_for_account(self, account_id: int):
        return self.db.execute("SELECT * FROM dialogs WHERE account_id=?", (account_id,)).fetchall()

    def dialogs_for_project(self, project_id: int):
        return self.db.execute("SELECT d.*,a.session_name,a.title,a.project_id FROM dialogs d JOIN accounts a ON a.id=d.account_id WHERE a.project_id=?", (project_id,)).fetchall()

    def delete_dialog(self, account_id: int, peer_id: int):
        self.db.execute("DELETE FROM dialogs WHERE account_id=? AND peer_id=?", (account_id, peer_id)); self.db.commit()

    def move_dialog(self, account_id: int, peer_id: int, topic_id: int, hub_chat_id: int):
        self.db.execute("UPDATE dialogs SET topic_id=?,hub_chat_id=?,imported=0 WHERE account_id=? AND peer_id=?", (topic_id, hub_chat_id, account_id, peer_id)); self.db.execute("DELETE FROM copied_messages WHERE account_id=? AND peer_id=?", (account_id, peer_id)); self.db.commit()

    def account_by_id(self, account_id: int):
        return self.db.execute("SELECT * FROM accounts WHERE id=?", (account_id,)).fetchone()

    def delete_account(self, account_id: int):
        # A topic can already be gone in Telegram; database cleanup must still finish.
        self.db.execute("DELETE FROM dialogs WHERE account_id=?", (account_id,))
        self.db.execute("DELETE FROM copied_messages WHERE account_id=?", (account_id,))
        self.db.execute("DELETE FROM accounts WHERE id=?", (account_id,)); self.db.commit()

    def copied(self, account_id: int, peer_id: int, source_message_id: int) -> bool:
        return self.db.execute("SELECT 1 FROM copied_messages WHERE account_id=? AND peer_id=? AND source_message_id=?", (account_id, peer_id, source_message_id)).fetchone() is not None

    def mark_copied(self, account_id: int, peer_id: int, source_message_id: int):
        self.db.execute("INSERT OR IGNORE INTO copied_messages(account_id,peer_id,source_message_id) VALUES(?,?,?)", (account_id, peer_id, source_message_id)); self.db.commit()

    def track_outreach(self, account, peer_id: int, message):
        text = (message.text or message.caption or "[медиа/файл]").strip()
        label = " ".join(text.split())[:160] or "[медиа/файл]"
        sent_at = int(message.date.timestamp()) if message.date else int(dt.datetime.now(dt.timezone.utc).timestamp())
        self.db.execute("INSERT OR IGNORE INTO outreach_messages(account_id,peer_id,source_message_id,project_id,script_label,sent_at) VALUES(?,?,?,?,?,?)", (account["id"], peer_id, message.id, account["project_id"], label, sent_at)); self.db.commit()

    def mark_reply(self, account_id: int, peer_id: int, replied_at: int):
        row = self.db.execute("SELECT id FROM outreach_messages WHERE account_id=? AND peer_id=? AND replied_at IS NULL ORDER BY sent_at DESC LIMIT 1", (account_id, peer_id)).fetchone()
        if row:
            self.db.execute("UPDATE outreach_messages SET replied_at=? WHERE id=?", (replied_at, row["id"])); self.db.commit()

    def daily_stats(self, start_ts: int, end_ts: int):
        projects = self.db.execute("""
          SELECT COALESCE(p.name, 'Без проекта') name,
                 SUM(CASE WHEN o.sent_at>=? AND o.sent_at<? THEN 1 ELSE 0 END) sent,
                 SUM(CASE WHEN o.replied_at>=? AND o.replied_at<? THEN 1 ELSE 0 END) replied
          FROM outreach_messages o LEFT JOIN projects p ON p.id=o.project_id
          WHERE (o.sent_at>=? AND o.sent_at<?) OR (o.replied_at>=? AND o.replied_at<?)
          GROUP BY COALESCE(p.name, 'Без проекта') ORDER BY name
        """, (start_ts, end_ts, start_ts, end_ts, start_ts, end_ts, start_ts, end_ts)).fetchall()
        scripts = self.db.execute("""
          SELECT p.name project_name, o.script_label, COUNT(*) replies
          FROM outreach_messages o JOIN projects p ON p.id=o.project_id
          WHERE o.replied_at>=? AND o.replied_at<? AND (lower(p.name) LIKE '%тендер%' OR lower(p.name) LIKE '%трейдинг%')
          GROUP BY p.name, o.script_label HAVING COUNT(*)>0 ORDER BY p.name, replies DESC
        """, (start_ts, end_ts)).fetchall()
        return projects, scripts

    def report_topic(self, project_id: int):
        row = self.db.execute("SELECT topic_id FROM report_topics WHERE project_id=?", (project_id,)).fetchone()
        return row["topic_id"] if row else None

    def set_report_topic(self, project_id: int, topic_id: int):
        self.db.execute("INSERT INTO report_topics(project_id,topic_id) VALUES(?,?) ON CONFLICT(project_id) DO UPDATE SET topic_id=excluded.topic_id", (project_id, topic_id)); self.db.commit()

    def clear_report_topic(self, project_id: int):
        self.db.execute("DELETE FROM report_topics WHERE project_id=?", (project_id,)); self.db.commit()

    def add_dialog(self, account_id: int, peer_id: int, topic_id: int, peer_name: str, hub_chat_id: int):
        self.db.execute("INSERT INTO dialogs(account_id,peer_id,topic_id,peer_name,hub_chat_id) VALUES(?,?,?,?,?)", (account_id, peer_id, topic_id, peer_name, hub_chat_id)); self.db.commit()

    def mark_imported(self, account_id: int, peer_id: int):
        self.db.execute("UPDATE dialogs SET imported=1 WHERE account_id=? AND peer_id=?", (account_id, peer_id)); self.db.commit()

    def state(self, user_id: int):
        return self.db.execute("SELECT * FROM user_states WHERE user_id=?", (user_id,)).fetchone()

    def set_state(self, user_id: int, action: str, payload: str = ""):
        self.db.execute("INSERT INTO user_states(user_id,action,payload) VALUES(?,?,?) ON CONFLICT(user_id) DO UPDATE SET action=excluded.action,payload=excluded.payload", (user_id, action, payload)); self.db.commit()

    def clear_state(self, user_id: int):
        self.db.execute("DELETE FROM user_states WHERE user_id=?", (user_id,)); self.db.commit()


class BotAPI:
    def __init__(self, token: str):
        self.base = f"https://api.telegram.org/bot{token}"
        self.http: Optional[aiohttp.ClientSession] = None
        self.topic_lock = asyncio.Lock()
        self.last_topic_created_at = 0.0
        self.topic_interval_seconds = 25

    async def start(self): self.http = aiohttp.ClientSession(timeout=aiohttp.ClientTimeout(total=60))
    async def close(self):
        if self.http and not self.http.closed: await self.http.close()
    async def reconnect(self):
        await self.close()
        await self.start()
    async def call(self, method: str, **payload):
        while True:
            async with self.http.post(f"{self.base}/{method}", json=payload) as response:
                data = await response.json()
            if data.get("ok"): return data["result"]
            retry_after = data.get("parameters", {}).get("retry_after")
            if retry_after is None:
                match = re.search(r"retry after (\d+)", data.get("description", ""), re.I)
                retry_after = int(match.group(1)) if match else None
            if retry_after:
                log.warning("Telegram rate limit for %s; waiting %s seconds", method, retry_after)
                await asyncio.sleep(retry_after + 1)
                continue
            raise RuntimeError(data.get("description", "Telegram API error"))
    async def send(self, chat_id: int, text: str, topic_id: int | None = None, markup: dict | None = None):
        body = {"chat_id": chat_id, "text": text, "parse_mode": "HTML"}
        if topic_id: body["message_thread_id"] = topic_id
        if markup: body["reply_markup"] = markup
        return await self.call("sendMessage", **body)
    async def answer(self, callback_id: str): return await self.call("answerCallbackQuery", callback_query_id=callback_id)
    async def photo(self, chat_id: int, path: Path, caption: str):
        form = aiohttp.FormData()
        form.add_field("chat_id", str(chat_id)); form.add_field("caption", caption, content_type="text/plain")
        form.add_field("photo", path.read_bytes(), filename=path.name, content_type="image/png")
        async with self.http.post(f"{self.base}/sendPhoto", data=form) as response:
            data = await response.json()
        if not data.get("ok"): raise RuntimeError(data.get("description", "Telegram API error"))
        return data["result"]
    async def edit(self, chat_id: int, message_id: int, text: str):
        return await self.call("editMessageText", chat_id=chat_id, message_id=message_id, text=text, parse_mode="HTML")
    async def topic(self, chat_id: int, title: str, color: int | None = None):
        body = {"chat_id": chat_id, "name": title[:128]}
        if color is not None: body["icon_color"] = color
        async with self.topic_lock:
            wait = self.topic_interval_seconds - (time.monotonic() - self.last_topic_created_at)
            if wait > 0: await asyncio.sleep(wait)
            result = await self.call("createForumTopic", **body)
            self.last_topic_created_at = time.monotonic()
            return result
    async def delete_topic(self, chat_id: int, topic_id: int):
        return await self.call("deleteForumTopic", chat_id=chat_id, message_thread_id=topic_id)


class Hub:
    def __init__(self, settings: Settings):
        self.s = settings; self.store = Store(settings.db_path); self.bot = BotAPI(settings.token); self.clients = {}; self.pending_qr = {}; self.pending_auth = {}; self.import_tasks = {}; self.copy_lock = asyncio.Lock(); self.report_lock = asyncio.Lock(); self.archived_peers = {}; self.last_poll_activity = time.monotonic()

    def allowed(self, user_id: int) -> bool:
        stored = {int(x) for x in (self.store.get("admin_ids") or "").split(",") if x}
        allowed = stored or self.s.admins
        return not allowed or user_id in allowed

    async def ensure_topic(self, account, peer_id: int, peer_name: str) -> int:
        existing = self.store.dialog(account["id"], peer_id)
        if existing: return existing["topic_id"]
        chat_id = self.store.project_hub(account["project_id"])
        if not chat_id: raise RuntimeError("CRM group not configured: send /setup in the forum group")
        project = self.store.db.execute("SELECT name, color FROM projects WHERE id=?", (account["project_id"],)).fetchone()
        label = f"{peer_name} · {project['name'] if project else 'Без проекта'}"
        topic = await self.bot.topic(int(chat_id), label, project["color"] if project else None)
        self.store.add_dialog(account["id"], peer_id, topic["message_thread_id"], peer_name, chat_id)
        return topic["message_thread_id"]

    def message_text(self, message, outgoing: bool = False) -> str:
        direction = "📤 <b>Аккаунт</b>" if outgoing else "📩 <b>Клиент</b>"
        body = html.escape(message.text or message.caption or "[медиа/файл]")
        return f"{direction}\n{body}"

    async def copy_message(self, account, peer_id: int, topic_id: int, message):
        """Idempotent bridge: a source Telegram message is copied to CRM only once."""
        async with self.copy_lock:
            if self.store.copied(account["id"], peer_id, message.id): return
            dialog = self.store.dialog(account["id"], peer_id)
            if not dialog: return
            await self.bot.send(dialog["hub_chat_id"], self.message_text(message, message.outgoing), topic_id)
            self.store.mark_copied(account["id"], peer_id, message.id)

    async def routed_message(self, client: Client, message):
        if message.chat.type not in (ChatType.PRIVATE,): return
        account = self.store.account(client.dialoghub_session)
        if not account: return
        peer = message.chat
        if peer.id in self.archived_peers.get(client.dialoghub_session, set()):
            # Old archive chats are never imported in bulk.  But a fresh
            # customer reply means the conversation is active again, even if
            # Telegram keeps the chat in Archive (for example when it is
            # muted).  Remember this exception across service restarts.
            if message.outgoing:
                return
            self.store.set(f"resumed_archive_{account['id']}_{peer.id}", "1")
        peer_name = " ".join(filter(None, [peer.first_name, peer.last_name])) or peer.username or str(peer.id)
        try:
            dialog = self.store.dialog(account["id"], peer.id)
            if message.outgoing and not dialog:
                self.store.track_outreach(account, peer.id, message)
                return
            if not message.outgoing:
                replied_at = int(message.date.timestamp()) if message.date else int(dt.datetime.now(dt.timezone.utc).timestamp())
                self.store.mark_reply(account["id"], peer.id, replied_at)
            if not dialog:
                await self.import_dialog(client, account, peer.id, peer_name)
                return
            topic_id = dialog["topic_id"]
            await self.copy_message(account, peer.id, topic_id, message)
        except Exception:
            log.exception("Could not route message")

    async def folder_dialogs(self, client: Client, folder_id: int, limit: int):
        """Pyrogram's dialog iterator with an explicit Telegram folder (0 = main, 1 = archive)."""
        current = 0; offset_date = 0; offset_id = 0; offset_peer = raw.types.InputPeerEmpty()
        while current < limit:
            result = await client.invoke(raw.functions.messages.GetDialogs(
                offset_date=offset_date, offset_id=offset_id, offset_peer=offset_peer,
                limit=min(100, limit - current), hash=0, folder_id=folder_id), sleep_threshold=60)
            users = {item.id: item for item in result.users}; chats = {item.id: item for item in result.chats}; messages = {}
            for raw_message in result.messages:
                if isinstance(raw_message, raw.types.MessageEmpty): continue
                peer_id = utils.get_peer_id(raw_message.peer_id)
                messages[peer_id] = await types.Message._parse(client, raw_message, users, chats)
            dialogs = [types.Dialog._parse(client, item, messages, users, chats) for item in result.dialogs if isinstance(item, raw.types.Dialog)]
            if not dialogs: return
            last = dialogs[-1]; offset_id = last.top_message.id; offset_date = utils.datetime_to_timestamp(last.top_message.date); offset_peer = await client.resolve_peer(last.chat.id)
            for dialog in dialogs:
                yield dialog; current += 1
                if current >= limit: return

    async def remove_archived_topics(self, client: Client, account):
        archived_ids = self.archived_peers.get(client.dialoghub_session)
        if archived_ids is None:
            archived_ids = {dialog.chat.id async for dialog in self.folder_dialogs(client, 1, 500) if dialog.chat.type == ChatType.PRIVATE}
        self.archived_peers[client.dialoghub_session] = archived_ids
        if not archived_ids: return
        removed = 0
        for dialog in self.store.dialogs_for_account(account["id"]):
            if dialog["peer_id"] not in archived_ids: continue
            if self.store.get(f"resumed_archive_{account['id']}_{dialog['peer_id']}") == "1": continue
            try:
                if dialog["hub_chat_id"]: await self.bot.delete_topic(dialog["hub_chat_id"], dialog["topic_id"])
                self.store.delete_dialog(account["id"], dialog["peer_id"]); removed += 1
                await asyncio.sleep(1)
            except Exception:
                log.exception("Could not delete archived CRM topic")
        if removed: log.info("Removed %s archived CRM topics for %s", removed, client.dialoghub_session)

    async def load_archived_peer_ids(self, client: Client):
        """Read archive before handlers are registered, so old updates cannot create topics."""
        result = await client.invoke(raw.functions.messages.GetDialogs(
            offset_date=0, offset_id=0, offset_peer=raw.types.InputPeerEmpty(),
            limit=100, hash=0, folder_id=1,
        ), sleep_threshold=60)
        return {
            dialog.peer.user_id
            for dialog in result.dialogs
            if isinstance(dialog, raw.types.Dialog) and isinstance(dialog.peer, raw.types.PeerUser)
        }

    async def delete_account_from_hub(self, chat_id: int, account_id: int):
        account = self.store.account_by_id(account_id)
        if not account:
            await self.bot.send(chat_id, "Этот аккаунт уже удалён."); return
        dialogs = self.store.dialogs_for_account(account_id)
        status = await self.bot.send(chat_id, f"🗑 Удаляю аккаунт «{html.escape(account['title'] or account['session_name'])}».\nУдаление диалогов выполняется…")
        client = self.clients.pop(account["session_name"], None)
        if client:
            try: await client.stop()
            except Exception: log.exception("Could not stop account client before deletion")
        deleted = 0; failed = 0
        for dialog in dialogs:
            try:
                if dialog["hub_chat_id"]: await self.bot.delete_topic(dialog["hub_chat_id"], dialog["topic_id"])
                self.store.delete_dialog(account_id, dialog["peer_id"]); deleted += 1
                await asyncio.sleep(1)
            except Exception:
                failed += 1; log.exception("Could not delete account topic %s", dialog["topic_id"])
        for suffix in (".session", ".session-journal"):
            (self.s.sessions_dir / f"{account['session_name']}{suffix}").unlink(missing_ok=True)
        self.store.delete_account(account_id)
        result = "✅ Удаление завершено.\nАккаунт исчез из списка, сессия удалена."
        if failed: result += f"\n⚠️ Не удалось удалить тем: {failed}. Они будут удалены при следующем запуске очистки."
        else: result += "\nВсе связанные темы удалены."
        await self.bot.edit(chat_id, status["message_id"], result)

    async def import_dialog(self, client: Client, account, peer_id: int, peer_name: str, history=None):
        """Create a topic only after a real client reply, then copy its recent context."""
        existing = self.store.dialog(account["id"], peer_id)
        if existing and existing["imported"]: return False
        history = history if history is not None else [m async for m in client.get_chat_history(peer_id, limit=20)]
        if not any(not message.outgoing for message in history): return False
        topic_id = existing["topic_id"] if existing else await self.ensure_topic(account, peer_id, peer_name)
        for message in reversed(history):
            await self.copy_message(account, peer_id, topic_id, message)
            # Leave Telegram capacity for bot buttons and live replies.
            await asyncio.sleep(4.0)
        self.store.mark_imported(account["id"], peer_id)
        return True

    async def import_recent_replied_dialogs(self, client: Client, account):
        """Import up to 50 recent private chats where the other person has replied."""
        imported = 0
        checked = 0
        async for dialog in self.folder_dialogs(client, 0, 250):
            if imported >= 50: break
            chat = dialog.chat
            existing = self.store.dialog(account["id"], chat.id)
            if chat.type != ChatType.PRIVATE or (existing and existing["imported"]):
                continue
            checked += 1
            peer_name = " ".join(filter(None, [chat.first_name, chat.last_name])) or chat.username or str(chat.id)
            try:
                history = [message async for message in client.get_chat_history(chat.id, limit=20)]
                if await self.import_dialog(client, account, chat.id, peer_name, history):
                    imported += 1
                    await asyncio.sleep(5.0)
            except Exception:
                log.exception("Could not import dialog for %s", client.dialoghub_session)
        log.info("Checked %s chats and imported %s replied dialogs for %s", checked, imported, client.dialoghub_session)

    async def import_account_in_background(self, client: Client, account):
        """History import must never hold up the management bot interface."""
        try:
            await self.import_recent_replied_dialogs(client, account)
        except Exception:
            log.exception("Background history import failed for %s", client.dialoghub_session)
        finally:
            self.import_tasks.pop(client.dialoghub_session, None)

    async def move_project_to_hub(self, project_id: int, target_chat_id: int):
        """Recreate a project's CRM topics in another forum and preserve recent context."""
        project = self.store.db.execute("SELECT * FROM projects WHERE id=?", (project_id,)).fetchone()
        if not project: return
        old_project_hub = self.store.project_hub(project_id)
        self.store.bind_project_hub(project_id, target_chat_id)
        old_report = self.store.report_topic(project_id)
        if old_report:
            self.store.clear_report_topic(project_id)
            try:
                if old_project_hub: await self.bot.delete_topic(old_project_hub, old_report)
            except Exception:
                log.exception("Could not delete old report topic for %s", project["name"])
        # Create the report first so the project is immediately visible in its new forum.
        await self.ensure_report_topic(project)
        for dialog in self.store.dialogs_for_project(project_id):
            old_chat_id = dialog["hub_chat_id"]
            if old_chat_id == target_chat_id: continue
            account = self.store.account(dialog["session_name"])
            client = self.clients.get(dialog["session_name"])
            if not account or not client:
                log.warning("Skipping project move for unavailable account %s", dialog["session_name"])
                continue
            label = f"{dialog['peer_name']} · {project['name']}"
            try:
                new_topic = await self.bot.topic(target_chat_id, label, project["color"])
                self.store.move_dialog(account["id"], dialog["peer_id"], new_topic["message_thread_id"], target_chat_id)
                history = [message async for message in client.get_chat_history(dialog["peer_id"], limit=20)]
                for message in reversed(history):
                    await self.copy_message(account, dialog["peer_id"], new_topic["message_thread_id"], message)
                    await asyncio.sleep(4)
                self.store.mark_imported(account["id"], dialog["peer_id"])
                if old_chat_id: await self.bot.delete_topic(old_chat_id, dialog["topic_id"])
            except Exception:
                log.exception("Could not move CRM topic for project %s", project["name"])

    async def resume_project_moves(self):
        """Continue any move interrupted by permissions, a restart, or a rate limit."""
        await asyncio.sleep(8)
        for project in self.store.projects():
            target_chat_id = self.store.project_hub(project["id"])
            if target_chat_id and any(dialog["hub_chat_id"] != target_chat_id for dialog in self.store.dialogs_for_project(project["id"])):
                log.info("Resuming move for project %s", project["name"])
                try:
                    await self.move_project_to_hub(project["id"], target_chat_id)
                except Exception:
                    log.exception("Could not resume move for project %s", project["name"])

    async def start_account(self, session_name: str):
        if session_name in self.clients: return
        account = self.store.account(session_name)
        if not account: return
        session_path = self.s.sessions_dir / f"{session_name}.session"
        if not session_path.exists():
            await self.cleanup_stale_account(account, "session file is missing")
            return
        client = Client(str(self.s.sessions_dir / session_name), api_id=self.s.api_id, api_hash=self.s.api_hash, no_updates=False)
        client.dialoghub_session = session_name
        authorized = await client.connect()
        if not authorized:
            await client.disconnect()
            session_path.unlink(missing_ok=True); (self.s.sessions_dir / f"{session_name}.session-journal").unlink(missing_ok=True)
            await self.cleanup_stale_account(account, "session is not authorized")
            return
        self.archived_peers[session_name] = await self.load_archived_peer_ids(client)
        await self.remove_archived_topics(client, account)
        client.add_handler(MessageHandler(self.routed_message, filters.private & (filters.incoming | filters.outgoing)))
        await client.initialize(); self.clients[session_name] = client
        log.info("Account started: %s", session_name)
        task = self.import_tasks.get(session_name)
        if not task or task.done():
            self.import_tasks[session_name] = asyncio.create_task(self.import_account_in_background(client, account))

    async def cleanup_stale_account(self, account, reason: str):
        log.warning("Removing stale account %s: %s", account["session_name"], reason)
        for dialog in self.store.dialogs_for_account(account["id"]):
            try:
                if dialog["hub_chat_id"]: await self.bot.delete_topic(dialog["hub_chat_id"], dialog["topic_id"])
                self.store.delete_dialog(account["id"], dialog["peer_id"])
                await asyncio.sleep(1)
            except Exception: log.exception("Could not clean stale account topic %s", dialog["topic_id"])
        self.store.delete_account(account["id"])

    def new_login_client(self, user_id: int):
        name = f"dialoghub_{user_id}_{int(time.time())}"
        # QR login needs raw Telegram updates immediately after confirmation.
        return name, Client(name, api_id=self.s.api_id, api_hash=self.s.api_hash, workdir=str(self.s.sessions_dir))

    async def cancel_pending_login(self, user_id: int):
        """A new login attempt replaces an abandoned one without a manual restart."""
        task = self.pending_qr.pop(user_id, None)
        if task and not task.done(): task.cancel()
        auth = self.pending_auth.pop(user_id, None)
        if auth:
            client = auth.get("telethon_client") or auth.get("client")
            try:
                if client and (client.is_connected() if "telethon_client" in auth else client.is_connected): await client.disconnect()
            except Exception:
                log.debug("Could not close abandoned login", exc_info=True)
            session_name = auth.get("session_name")
            if session_name:
                for suffix in (".session", ".session-journal"):
                    (self.s.sessions_dir / f"{session_name}{suffix}").unlink(missing_ok=True)
        self.store.clear_state(user_id)

    async def complete_login(self, chat_id: int, user_id: int, client: Client, project_id: int, session_name: str):
        me = await client.get_me()
        title = " ".join(filter(None, [me.first_name, me.last_name])) or str(me.id)
        if getattr(client, "is_initialized", False): await client.stop()
        elif client.is_connected: await client.disconnect()
        self.pending_auth.pop(user_id, None); self.store.clear_state(user_id)
        self.store.add_account(session_name, project_id, title)
        await self.bot.send(chat_id, f"✅ Аккаунт «{html.escape(title)}» подключён. Импортирую диалоги с ответами клиентов…")
        await self.start_account(session_name)

    async def switch_qr_dc(self, client: Client, dc_id: int):
        if getattr(client, "is_initialized", False): await client.stop()
        elif client.is_connected: await client.disconnect()
        await client.storage.dc_id(dc_id); await client.storage.auth_key(None); await client.connect()

    async def export_qr_token(self, client: Client):
        result = await client.invoke(raw.functions.auth.ExportLoginToken(api_id=self.s.api_id, api_hash=self.s.api_hash, except_ids=[]))
        if isinstance(result, raw.types.auth.LoginTokenMigrateTo):
            await self.switch_qr_dc(client, result.dc_id)
            result = await client.invoke(raw.functions.auth.ImportLoginToken(token=result.token))
        return result

    async def finish_qr_authorization(self, client: Client, result):
        user = result.authorization.user
        await client.storage.user_id(user.id); await client.storage.is_bot(False)
        try: await client.invoke(raw.functions.updates.GetState())
        except Exception: log.debug("Could not get Telegram state after QR authorization", exc_info=True)

    async def begin_phone_login(self, chat_id: int, user_id: int, project_id: int):
        if user_id in self.pending_auth:
            await self.cancel_pending_login(user_id)
        self.store.clear_state(user_id)
        session_name, client = self.new_login_client(user_id)
        try:
            await asyncio.wait_for(client.connect(), timeout=45)
            self.pending_auth[user_id] = {"client": client, "project_id": project_id, "session_name": session_name}
            self.store.set_state(user_id, "auth_phone")
            await self.bot.send(chat_id, "Введите номер телефона в формате <code>+79991234567</code>.")
        except Exception:
            if client.is_connected: await client.disconnect()
            log.exception("Phone login connection failed")
            await self.bot.send(chat_id, "⚠️ Не удалось подключиться к Telegram. Попробуйте QR-код или повторите позже.")

    async def begin_qr_login(self, chat_id: int, user_id: int, project_id: int):
        if user_id in self.pending_auth:
            await self.cancel_pending_login(user_id)
        self.store.clear_state(user_id)
        session_name = f"dialoghub_{user_id}_{int(time.time())}"
        client = TelegramClient(StringSession(), self.s.api_id, self.s.api_hash)
        image_path = self.s.db_path.parent / f"qr_{user_id}.png"
        try:
            await asyncio.wait_for(client.connect(), timeout=45)
            qr_login = await asyncio.wait_for(client.qr_login(), timeout=45)
            self.pending_auth[user_id] = {"telethon_client": client, "project_id": project_id, "session_name": session_name}
            task = asyncio.create_task(self.wait_telethon_qr_login(chat_id, user_id, client, qr_login, image_path))
            self.pending_qr[user_id] = task
        except Exception:
            if client.is_connected(): await client.disconnect()
            log.exception("Could not start QR login")
            await self.bot.send(chat_id, "⚠️ Не удалось создать QR-код. Выберите вход по номеру или попробуйте снова.")

    async def pyrogram_client_from_telethon(self, telethon_client, session_name: str):
        """Persist the successful QR session in Pyrogram's format for the CRM worker."""
        me = await telethon_client.get_me()
        bootstrap = Client(str(self.s.sessions_dir / session_name), api_id=self.s.api_id, api_hash=self.s.api_hash, no_updates=True)
        await bootstrap.connect()
        await bootstrap.storage.dc_id(telethon_client.session.dc_id)
        await bootstrap.storage.auth_key(telethon_client.session.auth_key.key)
        await bootstrap.storage.user_id(me.id)
        await bootstrap.storage.is_bot(False)
        await bootstrap.disconnect()
        client = Client(str(self.s.sessions_dir / session_name), api_id=self.s.api_id, api_hash=self.s.api_hash, no_updates=True)
        await client.connect()
        return client

    async def wait_telethon_qr_login(self, chat_id: int, user_id: int, client, qr_login, image_path: Path):
        keep_client = False
        try:
            qrcode.make(qr_login.url).save(image_path)
            await self.bot.photo(chat_id, image_path, "Откройте Telegram на подключаемом аккаунте: Настройки → Устройства → Подключить устройство. Отсканируйте QR.")
            await asyncio.wait_for(qr_login.wait(), timeout=120)
            data = self.pending_auth.get(user_id)
            if not data: return
            pyrogram_client = await self.pyrogram_client_from_telethon(client, data["session_name"])
            await client.disconnect()
            await self.complete_login(chat_id, user_id, pyrogram_client, data["project_id"], data["session_name"])
        except SessionPasswordNeededError:
            keep_client = True
            self.store.set_state(user_id, "auth_password")
            await self.bot.send(chat_id, "Введите пароль двухфакторной защиты подключаемого аккаунта:")
        except asyncio.TimeoutError:
            await self.bot.send(chat_id, "⌛ QR-код истёк. Выберите QR-код или вход по номеру заново.")
        except Exception:
            log.exception("Telethon QR login failed")
            await self.bot.send(chat_id, "⚠️ Не удалось завершить QR-вход. Попробуйте ещё раз или войдите по номеру.")
        finally:
            self.pending_qr.pop(user_id, None)
            data = self.pending_auth.get(user_id)
            if not keep_client:
                self.pending_auth.pop(user_id, None)
                if data and client.is_connected(): await client.disconnect()
            image_path.unlink(missing_ok=True)

    async def show_qr(self, chat_id: int, token: bytes, path: Path, refreshed: bool = False):
        encoded = base64.urlsafe_b64encode(token).decode().rstrip("=")
        qrcode.make(f"tg://login?token={encoded}").save(path)
        caption = "QR-код обновлён. Отсканируйте его в Telegram." if refreshed else "Откройте Telegram на подключаемом аккаунте: Настройки → Устройства → Подключить устройство. Отсканируйте QR."
        await self.bot.photo(chat_id, path, caption)

    async def wait_qr_login(self, chat_id: int, user_id: int, client: Client, result, event: asyncio.Event, image_path: Path):
        keep_client = False
        try:
            await self.show_qr(chat_id, result.token, image_path)
            deadline = time.monotonic() + 120
            while time.monotonic() < deadline:
                expires = result.expires - int(time.time())
                if expires <= 5:
                    result = await self.export_qr_token(client)
                    if isinstance(result, raw.types.auth.LoginTokenSuccess):
                        await self.finish_qr_authorization(client, result)
                        data = self.pending_auth[user_id]; await self.complete_login(chat_id, user_id, client, data["project_id"], data["session_name"]); return
                    if isinstance(result, raw.types.auth.LoginToken): await self.show_qr(chat_id, result.token, image_path, True)
                try: await asyncio.wait_for(event.wait(), timeout=min(5, max(1, expires)))
                except asyncio.TimeoutError: continue
                event.clear(); result = await self.export_qr_token(client)
                if isinstance(result, raw.types.auth.LoginTokenSuccess):
                    await self.finish_qr_authorization(client, result)
                    data = self.pending_auth[user_id]; await self.complete_login(chat_id, user_id, client, data["project_id"], data["session_name"]); return
            await self.bot.send(chat_id, "⌛ QR-код истёк. Выберите QR-код или вход по номеру заново.")
        except SessionPasswordNeeded:
            # QR was accepted, but this account has Telegram two-step verification.
            # Keep the authenticated temporary client alive for check_password().
            keep_client = True
            self.store.set_state(user_id, "auth_password")
            await self.bot.send(chat_id, "Введите пароль двухфакторной защиты подключаемого аккаунта:")
        except Exception:
            log.exception("QR login failed"); await self.bot.send(chat_id, "⚠️ Не удалось завершить QR-вход. Попробуйте вход по номеру.")
        finally:
            self.pending_qr.pop(user_id, None)
            data = self.pending_auth.get(user_id)
            if not keep_client:
                self.pending_auth.pop(user_id, None)
                if data:
                    if getattr(client, "is_initialized", False): await client.stop()
                    elif client.is_connected: await client.disconnect()
            image_path.unlink(missing_ok=True)

    @staticmethod
    def keyboard(rows):
        return {"inline_keyboard": [[{"text": text, "callback_data": data} for text, data in row] for row in rows]}

    async def main_menu(self, chat_id: int):
        await self.bot.send(chat_id, "<b>DialogHub</b>\nВыберите раздел:", markup=self.keyboard([
            [("📁 Проекты", "menu:projects"), ("👤 Аккаунты", "menu:accounts")],
            [("📊 Статус", "menu:status"), ("ℹ️ Помощь", "menu:help")],
        ]))

    async def handle_state_input(self, message: dict) -> bool:
        sender = message.get("from", {}); user_id = sender.get("id", 0)
        state = self.store.state(user_id); text = (message.get("text") or "").strip()
        if not state or not text or text.startswith("/") or not self.allowed(user_id): return False
        if state["action"] == "new_project":
            self.store.add_project(text[:80])
            project_id = self.store.project_id(text[:80])
            hubs = self.store.hubs()
            if not project_id or not hubs:
                self.store.clear_state(user_id)
                await self.bot.send(message["chat"]["id"], "⚠️ Сначала добавьте бота в форум-беседу и отправьте там /setup.")
                return True
            self.store.set_state(user_id, "new_project_hub", str(project_id))
            await self.bot.send(message["chat"]["id"], f"Проект «{html.escape(text[:80])}» создан. Выберите беседу для диалогов и отчётов:", markup=self.keyboard([[(hub["title"], f"project:hub:{project_id}:{hub['chat_id']}")] for hub in hubs]))
            return True
        if state["action"] == "new_account":
            project_id = int(state["payload"]); session = text.removesuffix(".session").strip()
            if not (self.s.sessions_dir / f"{session}.session").exists():
                await self.bot.send(message["chat"]["id"], "⚠️ Сессия не найдена. Введите имя файла ещё раз.")
                return True
            self.store.add_account(session, project_id, session); self.store.clear_state(user_id)
            await self.bot.send(message["chat"]["id"], "✅ Аккаунт добавлен. Импортирую до 50 диалогов, где клиент уже ответил…")
            asyncio.create_task(self.start_account(session))
            await self.accounts_menu(message["chat"]["id"])
            return True
        auth = self.pending_auth.get(user_id)
        if state["action"] == "auth_phone":
            if not auth:
                self.store.clear_state(user_id); await self.bot.send(message["chat"]["id"], "Сессия входа истекла. Начните заново."); return True
            phone = text.replace(" ", "").replace("-", "")
            if not phone.startswith("+"): phone = "+" + phone
            try:
                sent = await asyncio.wait_for(auth["client"].send_code(phone), timeout=45)
                auth["phone"] = phone; auth["phone_hash"] = sent.phone_code_hash; self.store.set_state(user_id, "auth_code")
                await self.bot.send(message["chat"]["id"], "Код отправлен в Telegram. Введите код:")
            except Exception:
                log.exception("Could not send login code"); await self.bot.send(message["chat"]["id"], "⚠️ Не удалось отправить код. Проверьте номер и попробуйте ещё раз.")
            return True
        if state["action"] == "auth_code":
            if not auth:
                self.store.clear_state(user_id); await self.bot.send(message["chat"]["id"], "Сессия входа истекла. Начните заново."); return True
            try:
                await asyncio.wait_for(auth["client"].sign_in(auth["phone"], auth["phone_hash"], text.replace(" ", "")), timeout=45)
                await self.complete_login(message["chat"]["id"], user_id, auth["client"], auth["project_id"], auth["session_name"])
            except SessionPasswordNeeded:
                self.store.set_state(user_id, "auth_password"); await self.bot.send(message["chat"]["id"], "Введите пароль двухфакторной защиты:")
            except Exception:
                log.exception("Could not sign in by code"); await self.bot.send(message["chat"]["id"], "⚠️ Код не подошёл. Введите код ещё раз.")
            return True
        if state["action"] == "auth_password":
            if not auth:
                self.store.clear_state(user_id); await self.bot.send(message["chat"]["id"], "Сессия входа истекла. Начните заново."); return True
            try:
                if "telethon_client" in auth:
                    await asyncio.wait_for(auth["telethon_client"].sign_in(password=text), timeout=45)
                    client = await self.pyrogram_client_from_telethon(auth["telethon_client"], auth["session_name"])
                    await auth["telethon_client"].disconnect()
                    await self.complete_login(message["chat"]["id"], user_id, client, auth["project_id"], auth["session_name"])
                else:
                    await asyncio.wait_for(auth["client"].check_password(text), timeout=45)
                    await self.complete_login(message["chat"]["id"], user_id, auth["client"], auth["project_id"], auth["session_name"])
            except PasswordHashInvalidError:
                await self.bot.send(message["chat"]["id"], "⚠️ Telegram не принял пароль. Введите его ещё раз.")
            except Exception:
                log.exception("Could not validate 2FA password")
                await self.bot.send(message["chat"]["id"], "⚠️ Не удалось проверить пароль из-за ошибки входа. Нажмите «Войти по QR-коду» и отсканируйте новый QR.")
            return True
        return False

    async def projects_menu(self, chat_id: int):
        projects = self.store.projects()
        text = "<b>Проекты</b>\n" + ("\n".join(f"• {html.escape(p['name'])}" for p in projects) if projects else "Пока нет проектов.")
        await self.bot.send(chat_id, text, markup=self.keyboard([[("➕ Добавить проект", "project:add")], [("⬅️ Назад", "menu:main")]]))

    async def accounts_menu(self, chat_id: int):
        accounts = self.store.accounts()
        text = "<b>Аккаунты</b>\n" + ("\n".join(f"• {html.escape(a['title'] or a['session_name'])} — {html.escape(a['project_name'] or 'Без проекта')}" for a in accounts) if accounts else "Пока нет подключённых аккаунтов.")
        buttons = [[(f"🗑 {a['title'] or a['session_name']}", f"account:delete:{a['id']}")] for a in accounts]
        buttons += [[("➕ Добавить аккаунт", "account:add")], [("⬅️ Назад", "menu:main")]]
        await self.bot.send(chat_id, text, markup=self.keyboard(buttons))

    async def pin_report_topic(self, topic_id: int):
        # Telegram Bot API can create forum topics but cannot pin forum topics.
        # A user-admin session is required for this optional visual action.
        return

    async def ensure_report_topic(self, project):
        async with self.report_lock:
            topic_id = self.store.report_topic(project["id"])
            if topic_id:
                try: await self.pin_report_topic(topic_id)
                except Exception: log.exception("Could not pin report topic")
                return topic_id
            chat_id = self.store.project_hub(project["id"])
            if not chat_id: return None
            topic = await self.bot.topic(chat_id, f"Отчёт · {project['name']}", project["color"])
            topic_id = topic["message_thread_id"]
            self.store.set_report_topic(project["id"], topic_id)
            await self.bot.send(chat_id, f"<b>Отчёты проекта «{html.escape(project['name'])}»</b>\nЕжедневный отчёт приходит сюда в 00:00 МСК.", topic_id)
            await self.pin_report_topic(topic_id)
            return topic_id

    async def ensure_all_report_topics(self):
        for project in self.store.projects():
            try: await self.ensure_report_topic(project)
            except Exception: log.exception("Could not create report topic for %s", project["name"])

    async def send_daily_report(self, report_day: dt.date):
        start = dt.datetime.combine(report_day, dt.time.min, tzinfo=REPORT_TZ)
        end = start + dt.timedelta(days=1)
        projects, scripts = self.store.daily_stats(int(start.timestamp()), int(end.timestamp()))
        project_rows = {row["name"]: row for row in projects}
        scripts_by_project = {}
        for row in scripts: scripts_by_project.setdefault(row["project_name"], []).append(row)
        for project in self.store.projects():
            topic_id = await self.ensure_report_topic(project)
            if not topic_id: continue
            row = project_rows.get(project["name"]); sent = row["sent"] if row else 0; replied = row["replied"] if row else 0
            text = f"<b>Отчёт · {html.escape(project['name'])}</b>\n{report_day.strftime('%d.%m.%Y')}\n\nОтправлено: <b>{sent}</b>\nОтветили: <b>{replied}</b>"
            project_scripts = scripts_by_project.get(project["name"], [])
            if project_scripts:
                text += "\n\n<b>Сработавшие скрипты</b>"
                for script in project_scripts:
                    text += f"\n• {html.escape(script['script_label'])} — <b>{script['replies']}</b>"
            await self.bot.send(self.store.project_hub(project["id"]), text, topic_id)

    async def run_requested_historical_script_analysis(self):
        """One-off analysis requested by an admin; uses live account sessions only."""
        if self.store.get("historical_script_analysis_requested") != "1": return
        await asyncio.sleep(15)
        project_names = ("ГОС ТЕНДЕР", "ТРЕЙДИНГ")
        output = {}
        try:
            for project_name in project_names:
                project_id = self.store.project_id(project_name)
                if not project_id: continue
                totals = Counter()
                for account in (a for a in self.store.accounts() if a["project_id"] == project_id):
                    client = self.clients.get(account["session_name"])
                    if not client: continue
                    seen_peers = set()
                    async for dialog in self.folder_dialogs(client, 0, 5000):
                        chat = dialog.chat
                        if chat.id in seen_peers or chat.type != ChatType.PRIVATE or getattr(chat, "is_bot", False): continue
                        seen_peers.add(chat.id)
                        history = [message async for message in client.get_chat_history(chat.id)]
                        history.reverse()
                        first_outbound = next((i for i, message in enumerate(history) if message.outgoing), None)
                        if first_outbound is None or any(not message.outgoing for message in history[:first_outbound]): continue
                        label = " ".join((history[first_outbound].text or history[first_outbound].caption or "[медиа/файл]").split())[:500]
                        totals[("sent", label)] += 1
                        if any(not message.outgoing for message in history[first_outbound + 1:]): totals[("reply", label)] += 1
                        await asyncio.sleep(0.8)
                rows = []
                for (_, label), sent in totals.items():
                    replies = totals[("reply", label)]
                    rows.append({"script": label, "sent": sent, "replies": replies, "rate": round(replies / sent * 100, 2)})
                output[project_name] = sorted(rows, key=lambda row: (-row["replies"], -row["rate"], -row["sent"]))
            self.store.set("historical_script_analysis_result", json.dumps(output, ensure_ascii=False))
            self.store.set("historical_script_analysis_requested", "done")
            for project_name, rows in output.items():
                project = self.store.db.execute("SELECT * FROM projects WHERE name=?", (project_name,)).fetchone()
                if not project: continue
                total_sent = sum(row["sent"] for row in rows); total_replies = sum(row["replies"] for row in rows)
                text = f"<b>Исторический анализ скриптов · {html.escape(project_name)}</b>\nТолько основная папка аккаунтов.\n\nОтправлено: <b>{total_sent}</b>\nОтветили: <b>{total_replies}</b>"
                for row in rows:
                    text += f"\n• {html.escape(row['script'][:250])} — <b>{row['replies']}</b> из {row['sent']} ({row['rate']}%)"
                    if len(text) > 3500:
                        await self.bot.send(self.store.project_hub(project["id"]), text, await self.ensure_report_topic(project)); text = "<b>Продолжение анализа</b>"
                await self.bot.send(self.store.project_hub(project["id"]), text, await self.ensure_report_topic(project))
        except Exception:
            log.exception("Historical script analysis failed")
            self.store.set("historical_script_analysis_requested", "failed")

    async def report_loop(self):
        while True:
            now = dt.datetime.now(REPORT_TZ)
            next_midnight = dt.datetime.combine(now.date() + dt.timedelta(days=1), dt.time.min, tzinfo=REPORT_TZ)
            await asyncio.sleep(max(1, (next_midnight - now).total_seconds()))
            report_day = next_midnight.date() - dt.timedelta(days=1)
            key = f"report_sent_{report_day.isoformat()}"
            if self.store.get(key): continue
            try:
                await self.send_daily_report(report_day)
                self.store.set(key, "1")
            except Exception: log.exception("Could not send daily report")

    async def callback(self, update: dict):
        query = update.get("callback_query")
        if not query: return False
        await self.bot.answer(query["id"])
        user_id = query.get("from", {}).get("id", 0); chat_id = query.get("message", {}).get("chat", {}).get("id")
        if not chat_id: return True
        data = query.get("data", "")
        if data == "menu:main": await self.main_menu(chat_id)
        elif data == "menu:projects": await self.projects_menu(chat_id)
        elif data == "menu:accounts": await self.accounts_menu(chat_id)
        elif data == "menu:status":
            await self.bot.send(chat_id, f"<b>Статус</b>\nПроектов: {len(self.store.projects())}\nПодключённых аккаунтов: {len(self.store.accounts())}")
        elif data == "menu:help":
            await self.bot.send(chat_id, "<b>Как начать</b>\n1. В CRM-группе: /setup\n2. В меню создайте проект\n3. Добавьте аккаунт в проект")
        elif data.startswith("account:delete:") and self.allowed(user_id):
            await self.delete_account_from_hub(chat_id, int(data.rsplit(":", 1)[1]))
            await self.accounts_menu(chat_id)
        elif data == "project:add" and self.allowed(user_id):
            self.store.set_state(user_id, "new_project"); await self.bot.send(chat_id, "Введите название нового проекта:")
        elif data.startswith("project:hub:") and self.allowed(user_id):
            _, _, project_id, hub_chat_id = data.split(":", 3)
            self.store.bind_project_hub(int(project_id), int(hub_chat_id)); self.store.clear_state(user_id)
            project = self.store.db.execute("SELECT * FROM projects WHERE id=?", (int(project_id),)).fetchone()
            await self.ensure_report_topic(project)
            await self.bot.send(chat_id, f"✅ Проект «{html.escape(project['name'])}» привязан к выбранной беседе. Там же создан топик отчёта.")
            await self.projects_menu(chat_id)
        elif data == "account:add" and self.allowed(user_id):
            projects = self.store.projects()
            if not projects: await self.bot.send(chat_id, "Сначала создайте хотя бы один проект.")
            else: await self.bot.send(chat_id, "Выберите проект:", markup=self.keyboard([[(p["name"], f"account:project:{p['id']}")] for p in projects] + [[("⬅️ Назад", "menu:accounts")]]))
        elif data.startswith("account:project:") and self.allowed(user_id):
            project_id = data.rsplit(":", 1)[1]
            await self.bot.send(chat_id, "Выберите способ подключения:", markup=self.keyboard([
                [("📷 Войти по QR-коду", f"account:qr:{project_id}")],
                [("📱 Войти по номеру", f"account:phone:{project_id}")],
                [("📁 Подключить готовую session", f"account:session:{project_id}")],
                [("⬅️ Назад", "account:add")],
            ]))
        elif data.startswith("account:qr:") and self.allowed(user_id):
            await self.begin_qr_login(chat_id, user_id, int(data.rsplit(":", 1)[1]))
        elif data.startswith("account:phone:") and self.allowed(user_id):
            await self.begin_phone_login(chat_id, user_id, int(data.rsplit(":", 1)[1]))
        elif data.startswith("account:session:") and self.allowed(user_id):
            self.store.set_state(user_id, "new_account", data.rsplit(":", 1)[1]); await self.bot.send(chat_id, "Введите имя файла сессии без <code>.session</code>.")
        return True

    async def reply(self, update: dict):
        message = update.get("message") or update.get("edited_message")
        if not message: return
        if await self.handle_state_input(message): return
        crm_chat_id = message.get("chat", {}).get("id")
        if not any(hub["chat_id"] == crm_chat_id for hub in self.store.hubs()): return
        sender = message.get("from", {})
        if sender.get("is_bot") or not self.allowed(sender.get("id", 0)): return
        text = message.get("text") or message.get("caption")
        topic_id = message.get("message_thread_id")
        if not text or not topic_id: return
        dialog = self.store.by_topic(crm_chat_id, topic_id)
        if not dialog: return
        client = self.clients.get(dialog["session_name"])
        if not client: return
        try:
            await client.send_message(dialog["peer_id"], text)
        except Exception:
            log.exception("Could not send reply")
            await self.bot.send(crm_chat_id, "⚠️ Не удалось отправить ответ с рабочего аккаунта.", topic_id)

    async def command(self, update: dict):
        message = update.get("message")
        if not message or not (text := message.get("text", "")).startswith("/"): return False
        sender = message.get("from", {}); chat = message.get("chat", {}); user_id = sender.get("id", 0)
        command, *parts = text.split(maxsplit=2); command = command.split("@", 1)[0]
        if command == "/start":
            await self.main_menu(chat["id"])
        elif command == "/setup":
            if not self.allowed(user_id) or chat.get("type") not in ("supergroup", "group"):
                return True
            if not self.store.get("admin_ids") and not self.s.admins:
                self.store.set("admin_ids", str(user_id))
            self.store.register_hub(chat["id"], chat.get("title") or f"CRM {chat['id']}")
            if not self.store.get("hub_chat_id"):
                self.store.set("hub_chat_id", str(chat["id"]))
            project_name = " ".join(parts).strip()
            project_id = self.store.project_id(project_name) if project_name else None
            if project_id:
                asyncio.create_task(self.move_project_to_hub(project_id, chat["id"]))
                await self.bot.send(chat["id"], f"✅ Беседа привязана к проекту «{html.escape(project_name)}». Переношу его диалоги и отчёт сюда.")
            else:
                await self.bot.send(chat["id"], "✅ Беседа добавлена. Теперь её можно выбрать для проекта в личном меню бота.")
        elif command == "/project" and self.allowed(user_id):
            if parts:
                name = " ".join(parts); self.store.add_project(name)
                project_id = self.store.project_id(name)
                if project_id: asyncio.create_task(self.ensure_report_topic(self.store.db.execute("SELECT * FROM projects WHERE id=?", (project_id,)).fetchone()))
                await self.bot.send(chat["id"], "✅ Проект добавлен.")
        elif command == "/account" and self.allowed(user_id):
            if len(parts) >= 2:
                project = self.store.project_id(parts[0]); session = parts[1]
                if project is None: await self.bot.send(chat["id"], "⚠️ Сначала создайте проект: /project Название")
                elif not (self.s.sessions_dir / f"{session}.session").exists(): await self.bot.send(chat["id"], "⚠️ Файл сессии не найден.")
                else:
                    self.store.add_account(session, project, session); await self.bot.send(chat["id"], "✅ Аккаунт добавлен. Перезапустите сервис.")
        elif command == "/status" and self.allowed(user_id):
            accounts = self.store.accounts(); projects = self.store.projects()
            await self.bot.send(chat["id"], f"Проектов: {len(projects)}\nПодключённых аккаунтов: {len(accounts)}")
        elif command == "/help":
            await self.bot.send(chat["id"], "<b>DialogHub</b>\n/setup — подключить этот чат\n/project Название\n/account Проект имя_сессии\n/status")
        return True

    async def poll(self):
        offset = None
        while True:
            try:
                self.last_poll_activity = time.monotonic()
                updates = await self.bot.call("getUpdates", offset=offset, timeout=50, allowed_updates=["message", "edited_message", "callback_query"])
                self.last_poll_activity = time.monotonic()
                for update in updates:
                    offset = update["update_id"] + 1
                    if await self.callback(update): continue
                    if not await self.command(update): await self.reply(update)
                    self.last_poll_activity = time.monotonic()
            except asyncio.CancelledError: raise
            except Exception:
                log.exception("Bot polling error"); await asyncio.sleep(3)

    async def poll_watchdog(self):
        """Recover the Bot API long-poll connection if Telegram leaves it hanging."""
        while True:
            await asyncio.sleep(30)
            if time.monotonic() - self.last_poll_activity > 90:
                log.warning("Bot polling stalled; reconnecting Bot API session")
                await self.bot.reconnect()
                self.last_poll_activity = time.monotonic()

    async def run(self):
        await self.bot.start()
        report_task = asyncio.create_task(self.report_loop())
        watchdog_task = asyncio.create_task(self.poll_watchdog())
        move_task = asyncio.create_task(self.resume_project_moves())
        historical_analysis_task = asyncio.create_task(self.run_requested_historical_script_analysis())
        asyncio.create_task(self.ensure_all_report_topics())
        for account in self.store.accounts():
            asyncio.create_task(self.start_account(account["session_name"]))
        try: await self.poll()
        finally:
            report_task.cancel()
            watchdog_task.cancel()
            move_task.cancel()
            historical_analysis_task.cancel()
            for client in self.clients.values(): await client.stop()
            await self.bot.close()


if __name__ == "__main__":
    asyncio.run(Hub(Settings.from_env()).run())
