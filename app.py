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
        CREATE TABLE IF NOT EXISTS user_states (
          user_id INTEGER PRIMARY KEY, action TEXT NOT NULL, payload TEXT);
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

    async def start(self): self.http = aiohttp.ClientSession(timeout=aiohttp.ClientTimeout(total=60))
    async def close(self): await self.http.close()
    async def call(self, method: str, **payload):
        async with self.http.post(f"{self.base}/{method}", json=payload) as response:
            data = await response.json()
        if not data.get("ok"):
            raise RuntimeError(data.get("description", "Telegram API error"))
        return data["result"]
    async def send(self, chat_id: int, text: str, topic_id: int | None = None, markup: dict | None = None):
        body = {"chat_id": chat_id, "text": text, "parse_mode": "HTML"}
        if topic_id: body["message_thread_id"] = topic_id
        if markup: body["reply_markup"] = markup
        return await self.call("sendMessage", **body)
    async def answer(self, callback_id: str): return await self.call("answerCallbackQuery", callback_query_id=callback_id)
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

    def message_text(self, message, outgoing: bool = False) -> str:
        direction = "📤 <b>Аккаунт</b>" if outgoing else "📩 <b>Клиент</b>"
        body = html.escape(message.text or message.caption or "[медиа/файл]")
        return f"{direction}\n{body}"

    async def routed_message(self, client: Client, message):
        if message.chat.type not in (ChatType.PRIVATE,): return
        account = self.store.account(client.dialoghub_session)
        if not account: return
        peer = message.chat
        peer_name = " ".join(filter(None, [peer.first_name, peer.last_name])) or peer.username or str(peer.id)
        try:
            dialog = self.store.dialog(account["id"], peer.id)
            if message.outgoing and not dialog:
                return
            topic_id = dialog["topic_id"] if dialog else await self.ensure_topic(account, peer.id, peer_name)
            await self.bot.send(int(self.store.get("hub_chat_id")), self.message_text(message, message.outgoing), topic_id)
        except Exception:
            log.exception("Could not route incoming message")

    async def import_recent_dialogs(self, client: Client, account):
        """One-time import: 50 latest private chats, 20 recent messages each."""
        imported = 0
        async for dialog in client.get_dialogs(limit=50):
            chat = dialog.chat
            if chat.type != ChatType.PRIVATE or self.store.dialog(account["id"], chat.id):
                continue
            peer_name = " ".join(filter(None, [chat.first_name, chat.last_name])) or chat.username or str(chat.id)
            try:
                topic_id = await self.ensure_topic(account, chat.id, peer_name)
                history = [message async for message in client.get_chat_history(chat.id, limit=20)]
                for message in reversed(history):
                    await self.bot.send(int(self.store.get("hub_chat_id")), self.message_text(message, message.outgoing), topic_id)
                    await asyncio.sleep(0.08)
                imported += 1
                await asyncio.sleep(0.2)
            except Exception:
                log.exception("Could not import dialog for %s", client.dialoghub_session)
        log.info("Imported %s recent dialogs for %s", imported, client.dialoghub_session)

    async def start_account(self, session_name: str):
        if session_name in self.clients: return
        account = self.store.account(session_name)
        if not account: return
        client = Client(str(self.s.sessions_dir / session_name), api_id=self.s.api_id, api_hash=self.s.api_hash, no_updates=False)
        client.dialoghub_session = session_name
        client.add_handler(MessageHandler(self.routed_message, filters.private & (filters.incoming | filters.outgoing)))
        await client.start(); self.clients[session_name] = client
        log.info("Account started: %s", session_name)
        await self.import_recent_dialogs(client, account)

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
            self.store.add_project(text[:80]); self.store.clear_state(user_id)
            await self.bot.send(message["chat"]["id"], f"✅ Проект «{html.escape(text[:80])}» добавлен.")
            await self.projects_menu(message["chat"]["id"])
            return True
        if state["action"] == "new_account":
            project_id = int(state["payload"]); session = text.removesuffix(".session").strip()
            if not (self.s.sessions_dir / f"{session}.session").exists():
                await self.bot.send(message["chat"]["id"], "⚠️ Сессия не найдена. Введите имя файла ещё раз.")
                return True
            self.store.add_account(session, project_id, session); self.store.clear_state(user_id)
            await self.bot.send(message["chat"]["id"], "✅ Аккаунт добавлен. Импортирую последние 50 диалогов…")
            asyncio.create_task(self.start_account(session))
            await self.accounts_menu(message["chat"]["id"])
            return True
        return False

    async def projects_menu(self, chat_id: int):
        projects = self.store.projects()
        text = "<b>Проекты</b>\n" + ("\n".join(f"• {html.escape(p['name'])}" for p in projects) if projects else "Пока нет проектов.")
        await self.bot.send(chat_id, text, markup=self.keyboard([[("➕ Добавить проект", "project:add")], [("⬅️ Назад", "menu:main")]]))

    async def accounts_menu(self, chat_id: int):
        accounts = self.store.accounts()
        text = "<b>Аккаунты</b>\n" + ("\n".join(f"• {html.escape(a['title'] or a['session_name'])} — {html.escape(a['project_name'] or 'Без проекта')}" for a in accounts) if accounts else "Пока нет подключённых аккаунтов.")
        await self.bot.send(chat_id, text, markup=self.keyboard([[("➕ Добавить аккаунт", "account:add")], [("⬅️ Назад", "menu:main")]]))

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
        elif data == "project:add" and self.allowed(user_id):
            self.store.set_state(user_id, "new_project"); await self.bot.send(chat_id, "Введите название нового проекта:")
        elif data == "account:add" and self.allowed(user_id):
            projects = self.store.projects()
            if not projects: await self.bot.send(chat_id, "Сначала создайте хотя бы один проект.")
            else: await self.bot.send(chat_id, "Выберите проект:", markup=self.keyboard([[(p["name"], f"account:project:{p['id']}")] for p in projects] + [[("⬅️ Назад", "menu:accounts")]]))
        elif data.startswith("account:project:") and self.allowed(user_id):
            self.store.set_state(user_id, "new_account", data.rsplit(":", 1)[1]); await self.bot.send(chat_id, "Введите имя файла сессии без <code>.session</code>.")
        return True

    async def reply(self, update: dict):
        message = update.get("message") or update.get("edited_message")
        if not message: return
        if await self.handle_state_input(message): return
        if message.get("chat", {}).get("id") != int(self.store.get("hub_chat_id") or 0): return
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
        if command == "/start":
            await self.main_menu(chat["id"])
        elif command == "/setup":
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
                updates = await self.bot.call("getUpdates", offset=offset, timeout=50, allowed_updates=["message", "edited_message", "callback_query"])
                for update in updates:
                    offset = update["update_id"] + 1
                    if await self.callback(update): continue
                    if not await self.command(update): await self.reply(update)
            except asyncio.CancelledError: raise
            except Exception:
                log.exception("Bot polling error"); await asyncio.sleep(3)

    async def run(self):
        await self.bot.start()
        for account in self.store.accounts():
            await self.start_account(account["session_name"])
        try: await self.poll()
        finally:
            for client in self.clients.values(): await client.stop()
            await self.bot.close()


if __name__ == "__main__":
    asyncio.run(Hub(Settings.from_env()).run())
