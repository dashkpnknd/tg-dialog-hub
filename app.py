import asyncio
import html
import logging
import os
import sqlite3
from dataclasses import dataclass
from pathlib import Path
from typing import Optional

import aiohttp
from dotenv import load_dotenv
from pyrogram import Client, filters
from pyrogram.enums import ChatType
from pyrogram.handlers import MessageHandler

load_dotenv()
logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger("dialoghub")


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
        CREATE TABLE IF NOT EXISTS projects (id INTEGER PRIMARY KEY, name TEXT UNIQUE NOT NULL);
        CREATE TABLE IF NOT EXISTS accounts (
          id INTEGER PRIMARY KEY, session_name TEXT UNIQUE NOT NULL, project_id INTEGER,
          title TEXT, enabled INTEGER NOT NULL DEFAULT 1,
          FOREIGN KEY(project_id) REFERENCES projects(id));
        CREATE TABLE IF NOT EXISTS dialogs (
          account_id INTEGER NOT NULL, peer_id INTEGER NOT NULL, topic_id INTEGER NOT NULL,
          peer_name TEXT, PRIMARY KEY(account_id, peer_id), UNIQUE(topic_id),
          FOREIGN KEY(account_id) REFERENCES accounts(id));
        """)
        self.db.commit()

    def get(self, key: str) -> Optional[str]:
        row = self.db.execute("SELECT value FROM settings WHERE key=?", (key,)).fetchone()
        return row["value"] if row else None

    def set(self, key: str, value: str):
        self.db.execute("INSERT INTO settings(key,value) VALUES(?,?) ON CONFLICT(key) DO UPDATE SET value=excluded.value", (key, value))
        self.db.commit()

    def add_project(self, name: str):
        self.db.execute("INSERT OR IGNORE INTO projects(name) VALUES(?)", (name,)); self.db.commit()

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

    def by_topic(self, topic_id: int):
        return self.db.execute("SELECT d.*,a.session_name,a.title,p.name project_name FROM dialogs d JOIN accounts a ON a.id=d.account_id LEFT JOIN projects p ON p.id=a.project_id WHERE d.topic_id=?", (topic_id,)).fetchone()

    def add_dialog(self, account_id: int, peer_id: int, topic_id: int, peer_name: str):
        self.db.execute("INSERT INTO dialogs(account_id,peer_id,topic_id,peer_name) VALUES(?,?,?,?)", (account_id, peer_id, topic_id, peer_name)); self.db.commit()


class BotAPI:
    def __init__(self, token: str):
        self.base = f"https://api.telegram.org/bot{token}"
        self.http: Optional[aiohttp.ClientSession] = None

    async def start(self): self.http = aiohttp.ClientSession(timeout=aiohttp.ClientTimeout(total=60))
    async def close(self): await self.http.close()
    async def call(self, method: str, **payload):
        async with self.http.post(f"{self.base}/{method}", json=payload) as response:
            data = await response.json()
        if not data.get("ok"):
            raise RuntimeError(data.get("description", "Telegram API error"))
        return data["result"]
    async def send(self, chat_id: int, text: str, topic_id: int | None = None):
        body = {"chat_id": chat_id, "text": text, "parse_mode": "HTML"}
        if topic_id: body["message_thread_id"] = topic_id
        return await self.call("sendMessage", **body)
    async def topic(self, chat_id: int, title: str):
        return await self.call("createForumTopic", chat_id=chat_id, name=title[:128])


class Hub:
    def __init__(self, settings: Settings):
        self.s = settings; self.store = Store(settings.db_path); self.bot = BotAPI(settings.token); self.clients = {}

    def allowed(self, user_id: int) -> bool:
        stored = {int(x) for x in (self.store.get("admin_ids") or "").split(",") if x}
        allowed = stored or self.s.admins
        return not allowed or user_id in allowed

    async def ensure_topic(self, account, peer_id: int, peer_name: str) -> int:
        existing = self.store.dialog(account["id"], peer_id)
        if existing: return existing["topic_id"]
        chat_id = self.store.get("hub_chat_id")
        if not chat_id: raise RuntimeError("CRM group not configured: send /setup in the forum group")
        project = self.store.db.execute("SELECT name FROM projects WHERE id=?", (account["project_id"],)).fetchone()
        label = f"{peer_name} · {project['name'] if project else 'Без проекта'}"
        topic = await self.bot.topic(int(chat_id), label)
        self.store.add_dialog(account["id"], peer_id, topic["message_thread_id"], peer_name)
        return topic["message_thread_id"]

    async def incoming(self, client: Client, message):
        if message.chat.type not in (ChatType.PRIVATE,): return
        account = self.store.account(client.dialoghub_session)
        if not account or message.from_user is None: return
        peer_name = " ".join(filter(None, [message.from_user.first_name, message.from_user.last_name])) or str(message.from_user.id)
        try:
            topic_id = await self.ensure_topic(account, message.from_user.id, peer_name)
            project = self.store.db.execute("SELECT name FROM projects WHERE id=?", (account["project_id"],)).fetchone()
            header = f"<b>{html.escape(peer_name)}</b> · {html.escape(project['name'] if project else 'Без проекта')}"
            body = html.escape(message.text or message.caption or "[медиа/файл]")
            await self.bot.send(int(self.store.get("hub_chat_id")), f"{header}\n{body}", topic_id)
        except Exception:
            log.exception("Could not route incoming message")

    async def reply(self, update: dict):
        message = update.get("message") or update.get("edited_message")
        if not message or message.get("chat", {}).get("id") != int(self.store.get("hub_chat_id") or 0): return
        sender = message.get("from", {})
        if sender.get("is_bot") or not self.allowed(sender.get("id", 0)): return
        text = message.get("text") or message.get("caption")
        topic_id = message.get("message_thread_id")
        if not text or not topic_id: return
        dialog = self.store.by_topic(topic_id)
        if not dialog: return
        client = self.clients.get(dialog["session_name"])
        if not client: return
        try:
            await client.send_message(dialog["peer_id"], text)
        except Exception:
            log.exception("Could not send reply")
            await self.bot.send(int(self.store.get("hub_chat_id")), "⚠️ Не удалось отправить ответ с рабочего аккаунта.", topic_id)

    async def command(self, update: dict):
        message = update.get("message")
        if not message or not (text := message.get("text", "")).startswith("/"): return False
        sender = message.get("from", {}); chat = message.get("chat", {}); user_id = sender.get("id", 0)
        command, *parts = text.split(maxsplit=2); command = command.split("@", 1)[0]
        if command == "/setup":
            if not self.allowed(user_id) or chat.get("type") not in ("supergroup", "group"):
                return True
            if not self.store.get("admin_ids") and not self.s.admins:
                self.store.set("admin_ids", str(user_id))
            self.store.set("hub_chat_id", str(chat["id"])); await self.bot.send(chat["id"], "✅ DialogHub подключён к этому чату.")
        elif command == "/project" and self.allowed(user_id):
            if parts: self.store.add_project(" ".join(parts)); await self.bot.send(chat["id"], "✅ Проект добавлен.")
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
                updates = await self.bot.call("getUpdates", offset=offset, timeout=50, allowed_updates=["message", "edited_message"])
                for update in updates:
                    offset = update["update_id"] + 1
                    if not await self.command(update): await self.reply(update)
            except asyncio.CancelledError: raise
            except Exception:
                log.exception("Bot polling error"); await asyncio.sleep(3)

    async def run(self):
        await self.bot.start()
        for account in self.store.accounts():
            name = account["session_name"]
            client = Client(str(self.s.sessions_dir / name), api_id=self.s.api_id, api_hash=self.s.api_hash, no_updates=False)
            client.dialoghub_session = name
            client.add_handler(MessageHandler(self.incoming, filters.incoming & filters.private))
            await client.start(); self.clients[name] = client; log.info("Account started: %s", name)
        try: await self.poll()
        finally:
            for client in self.clients.values(): await client.stop()
            await self.bot.close()


if __name__ == "__main__":
    asyncio.run(Hub(Settings.from_env()).run())
