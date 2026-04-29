from __future__ import annotations

import json
import sqlite3
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List, Optional

import requests
from werkzeug.security import check_password_hash, generate_password_hash

BASE_DIR = Path(__file__).resolve().parent
DATA_DIR = BASE_DIR / "data"
DATA_DIR.mkdir(exist_ok=True)
DB_PATH = DATA_DIR / "crm.sqlite3"


def load_env_file(path: Optional[Path] = None) -> Dict[str, str]:
    path = path or (BASE_DIR / ".env")
    values: Dict[str, str] = {}
    if not path.exists():
        return values
    for line in path.read_text(encoding="utf-8").splitlines():
        line = line.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        key, value = line.split("=", 1)
        values[key.strip()] = value.strip().strip('"').strip("'")
    return values


ENV = load_env_file()
BOT_TOKEN = ENV.get("BOT_TOKEN", "8665717135:AAECBG4A-EchQHLxKvMLPT4z-pQ8JvpLQnA").strip()
WEBAPP_URL = ENV.get("WEBAPP_URL", "http://127.0.0.1:8000/static/app.html").strip()
CRM_PUBLIC_URL = ENV.get("CRM_PUBLIC_URL", "http://127.0.0.1:8000").strip()
ADMIN_USERNAME = ENV.get("ADMIN_USERNAME", "admin").strip()
ADMIN_PASSWORD = ENV.get("ADMIN_PASSWORD", "admin123").strip()
SECRET_KEY = ENV.get("SECRET_KEY", "trud-exchange-secret").strip()
MANAGER_ALERT_CHAT_ID = ENV.get("MANAGER_ALERT_CHAT_ID", "-1003911423320").strip()


def now_iso() -> str:
    return datetime.utcnow().isoformat(timespec="seconds") + "Z"


def connect_db() -> sqlite3.Connection:
    conn = sqlite3.connect(DB_PATH, check_same_thread=False)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA foreign_keys=ON;")
    conn.execute("PRAGMA journal_mode=WAL;")
    return conn


def init_db() -> None:
    with connect_db() as conn:
        conn.executescript(
            """
            CREATE TABLE IF NOT EXISTS managers (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                name TEXT NOT NULL,
                username TEXT NOT NULL UNIQUE,
                password_hash TEXT NOT NULL,
                role TEXT NOT NULL DEFAULT 'manager',
                active INTEGER NOT NULL DEFAULT 1,
                tg_chat_id TEXT DEFAULT '',
                created_at TEXT NOT NULL
            );

            CREATE TABLE IF NOT EXISTS clients (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                tg_user_id INTEGER NOT NULL UNIQUE,
                username TEXT DEFAULT '',
                first_name TEXT DEFAULT '',
                last_name TEXT DEFAULT '',
                created_at TEXT NOT NULL,
                last_seen_at TEXT NOT NULL
            );

            CREATE TABLE IF NOT EXISTS conversations (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                client_id INTEGER NOT NULL,
                assigned_manager_id INTEGER,
                status TEXT NOT NULL DEFAULT 'new',
                source TEXT NOT NULL DEFAULT 'telegram',
                created_at TEXT NOT NULL,
                updated_at TEXT NOT NULL,
                last_message_at TEXT NOT NULL,
                FOREIGN KEY(client_id) REFERENCES clients(id) ON DELETE CASCADE,
                FOREIGN KEY(assigned_manager_id) REFERENCES managers(id) ON DELETE SET NULL
            );

            CREATE TABLE IF NOT EXISTS messages (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                conversation_id INTEGER NOT NULL,
                sender_type TEXT NOT NULL,
                sender_name TEXT DEFAULT '',
                content TEXT NOT NULL,
                message_type TEXT NOT NULL DEFAULT 'text',
                telegram_message_id INTEGER,
                created_at TEXT NOT NULL,
                FOREIGN KEY(conversation_id) REFERENCES conversations(id) ON DELETE CASCADE
            );

            CREATE INDEX IF NOT EXISTS idx_messages_conversation ON messages(conversation_id);
            CREATE INDEX IF NOT EXISTS idx_conversations_status ON conversations(status);
            CREATE INDEX IF NOT EXISTS idx_conversations_updated ON conversations(updated_at);
            """
        )
        count = conn.execute("SELECT COUNT(*) AS c FROM managers").fetchone()["c"]
        if count == 0:
            conn.execute(
                """
                INSERT INTO managers (name, username, password_hash, role, active, created_at)
                VALUES (?, ?, ?, ?, 1, ?)
                """,
                (
                    "Admin",
                    ADMIN_USERNAME,
                    generate_password_hash(ADMIN_PASSWORD),
                    "admin",
                    now_iso(),
                ),
            )
        conn.commit()


def verify_manager_login(username: str, password: str):
    with connect_db() as conn:
        row = conn.execute(
            "SELECT * FROM managers WHERE username = ? AND active = 1",
            (username.strip(),),
        ).fetchone()
        if row and check_password_hash(row["password_hash"], password):
            return row
    return None


def list_managers(active_only: bool = False) -> List[sqlite3.Row]:
    with connect_db() as conn:
        if active_only:
            rows = conn.execute("SELECT * FROM managers WHERE active = 1 ORDER BY role DESC, id ASC").fetchall()
        else:
            rows = conn.execute("SELECT * FROM managers ORDER BY role DESC, active DESC, id ASC").fetchall()
    return list(rows)


def add_manager(name: str, username: str, password: str, role: str = "manager", tg_chat_id: str = "") -> int:
    with connect_db() as conn:
        cur = conn.execute(
            """
            INSERT INTO managers (name, username, password_hash, role, active, tg_chat_id, created_at)
            VALUES (?, ?, ?, ?, 1, ?, ?)
            """,
            (name.strip(), username.strip(), generate_password_hash(password), role, tg_chat_id.strip(), now_iso()),
        )
        conn.commit()
        return int(cur.lastrowid)


def update_manager(manager_id: int, **fields: Any) -> None:
    allowed = {"name", "username", "password_hash", "role", "active", "tg_chat_id"}
    chunks = []
    vals: List[Any] = []
    for key, val in fields.items():
        if key in allowed:
            chunks.append(f"{key} = ?")
            vals.append(val)
    if not chunks:
        return
    vals.append(manager_id)
    with connect_db() as conn:
        conn.execute(f"UPDATE managers SET {', '.join(chunks)} WHERE id = ?", vals)
        conn.commit()


def get_manager(manager_id: int):
    with connect_db() as conn:
        return conn.execute("SELECT * FROM managers WHERE id = ?", (manager_id,)).fetchone()


def get_manager_by_username(username: str):
    with connect_db() as conn:
        return conn.execute("SELECT * FROM managers WHERE username = ?", (username,),).fetchone()


def upsert_client(tg_user: Dict[str, Any]) -> int:
    tg_user_id = int(tg_user["id"])
    username = tg_user.get("username") or ""
    first_name = tg_user.get("first_name") or ""
    last_name = tg_user.get("last_name") or ""
    current = now_iso()
    with connect_db() as conn:
        row = conn.execute("SELECT id FROM clients WHERE tg_user_id = ?", (tg_user_id,)).fetchone()
        if row:
            conn.execute(
                "UPDATE clients SET username = ?, first_name = ?, last_name = ?, last_seen_at = ? WHERE tg_user_id = ?",
                (username, first_name, last_name, current, tg_user_id),
            )
            conn.commit()
            return int(row["id"])
        cur = conn.execute(
            """
            INSERT INTO clients (tg_user_id, username, first_name, last_name, created_at, last_seen_at)
            VALUES (?, ?, ?, ?, ?, ?)
            """,
            (tg_user_id, username, first_name, last_name, current, current),
        )
        conn.commit()
        return int(cur.lastrowid)


def get_client_by_tg_id(tg_user_id: int):
    with connect_db() as conn:
        return conn.execute("SELECT * FROM clients WHERE tg_user_id = ?", (tg_user_id,)).fetchone()


def get_open_conversation(client_id: int):
    with connect_db() as conn:
        return conn.execute(
            """
            SELECT * FROM conversations
            WHERE client_id = ? AND status != 'closed'
            ORDER BY updated_at DESC, id DESC
            LIMIT 1
            """,
            (client_id,),
        ).fetchone()


def create_conversation(client_id: int, source: str = "telegram") -> int:
    current = now_iso()
    with connect_db() as conn:
        cur = conn.execute(
            """
            INSERT INTO conversations (client_id, status, source, created_at, updated_at, last_message_at)
            VALUES (?, 'new', ?, ?, ?, ?)
            """,
            (client_id, source, current, current, current),
        )
        conn.commit()
        return int(cur.lastrowid)


def get_conversation(conversation_id: int):
    with connect_db() as conn:
        return conn.execute(
            """
            SELECT c.*, cl.tg_user_id, cl.username as client_username, cl.first_name, cl.last_name,
                   m.name as manager_name, m.username as manager_username, m.tg_chat_id as manager_tg_chat_id
            FROM conversations c
            JOIN clients cl ON cl.id = c.client_id
            LEFT JOIN managers m ON m.id = c.assigned_manager_id
            WHERE c.id = ?
            """,
            (conversation_id,),
        ).fetchone()


def list_conversations(status: Optional[str] = None, q: str = "", manager_id: Optional[int] = None,
                       only_assigned_to_me: bool = False, limit: int = 200):
    sql = """
        SELECT c.*, cl.tg_user_id, cl.username as client_username, cl.first_name, cl.last_name,
               m.name as manager_name, m.username as manager_username,
               (
                   SELECT content
                   FROM messages mm
                   WHERE mm.conversation_id = c.id
                   ORDER BY mm.id DESC
                   LIMIT 1
               ) as last_message
        FROM conversations c
        JOIN clients cl ON cl.id = c.client_id
        LEFT JOIN managers m ON m.id = c.assigned_manager_id
        WHERE 1=1
    """
    args: List[Any] = []
    if status and status != "all":
        sql += " AND c.status = ?"
        args.append(status)
    if q:
        sql += " AND (cl.first_name LIKE ? OR cl.last_name LIKE ? OR cl.username LIKE ? OR CAST(cl.tg_user_id AS TEXT) LIKE ? OR CAST(c.id AS TEXT) LIKE ?)"
        like = f"%{q}%"
        args.extend([like, like, like, like, like])
    if manager_id is not None and only_assigned_to_me:
        sql += " AND c.assigned_manager_id = ?"
        args.append(manager_id)
    sql += " ORDER BY c.updated_at DESC, c.id DESC LIMIT ?"
    args.append(limit)
    with connect_db() as conn:
        rows = conn.execute(sql, args).fetchall()
    return list(rows)


def get_messages(conversation_id: int):
    with connect_db() as conn:
        rows = conn.execute(
            "SELECT * FROM messages WHERE conversation_id = ? ORDER BY id ASC",
            (conversation_id,),
        ).fetchall()
    return list(rows)


def add_message(conversation_id: int, sender_type: str, content: str, sender_name: str = "",
                message_type: str = "text", telegram_message_id: Optional[int] = None) -> int:
    current = now_iso()
    with connect_db() as conn:
        cur = conn.execute(
            """
            INSERT INTO messages (conversation_id, sender_type, sender_name, content, message_type, telegram_message_id, created_at)
            VALUES (?, ?, ?, ?, ?, ?, ?)
            """,
            (conversation_id, sender_type, sender_name, content, message_type, telegram_message_id, current),
        )
        conn.execute(
            "UPDATE conversations SET updated_at = ?, last_message_at = ? WHERE id = ?",
            (current, current, conversation_id),
        )
        conn.commit()
        return int(cur.lastrowid)


def set_conversation_status(conversation_id: int, status: str) -> None:
    current = now_iso()
    with connect_db() as conn:
        conn.execute(
            "UPDATE conversations SET status = ?, updated_at = ? WHERE id = ?",
            (status, current, conversation_id),
        )
        conn.commit()


def assign_conversation(conversation_id: int, manager_id: Optional[int]) -> None:
    current = now_iso()
    with connect_db() as conn:
        conn.execute(
            "UPDATE conversations SET assigned_manager_id = ?, updated_at = ? WHERE id = ?",
            (manager_id, current, conversation_id),
        )
        conn.commit()


def choose_manager_for_new_conversation():
    with connect_db() as conn:
        rows = conn.execute(
            """
            SELECT m.*, COUNT(c.id) AS load_count
            FROM managers m
            LEFT JOIN conversations c
              ON c.assigned_manager_id = m.id AND c.status != 'closed'
            WHERE m.active = 1
            GROUP BY m.id
            ORDER BY load_count ASC, m.id ASC
            LIMIT 1
            """
        ).fetchone()
    return rows


def manager_display_name(manager_row) -> str:
    if not manager_row:
        return "Менеджер"
    return manager_row["name"] or manager_row["username"] or "Менеджер"


def extract_short_name(conv_row) -> str:
    parts = [conv_row["first_name"] or "", conv_row["last_name"] or ""]
    name = " ".join([p for p in parts if p]).strip()
    if name:
        return name
    if conv_row.get("client_username"):
        return f"@{conv_row['client_username']}"
    return f"Клиент {conv_row['tg_user_id']}"


def format_dt(iso_value: str) -> str:
    if not iso_value:
        return ""
    try:
        dt = datetime.fromisoformat(iso_value.replace("Z", "+00:00"))
        return dt.strftime("%d.%m.%Y %H:%M")
    except Exception:
        return iso_value


def safe_json_loads(value: str) -> Any:
    try:
        return json.loads(value)
    except Exception:
        return None


def build_summary_from_order(order: Dict[str, Any]) -> str:
    if not isinstance(order, dict):
        return ""
    direction = order.get("direction") or order.get("mode") or ""
    amount = order.get("amount") or order.get("sum") or ""
    source = order.get("from") or order.get("source_currency") or ""
    target = order.get("to") or order.get("target_currency") or ""
    if direction == "give":
        return f"Отдаю: {amount} {source}".strip()
    if direction == "receive":
        return f"Получаю: {amount} {target}".strip()
    return f"{amount} {source} → {target}".strip(" →")


def build_text_keyboard() -> Dict[str, Any]:
    if not WEBAPP_URL:
        return {"inline_keyboard": []}
    return {
        "inline_keyboard": [[{"text": "Открыть форму", "web_app": {"url": WEBAPP_URL}}]],
    }


def send_telegram_message(chat_id: int, text: str, reply_markup: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
    if not BOT_TOKEN:
        raise RuntimeError("BOT_TOKEN is not set")
    payload: Dict[str, Any] = {"chat_id": int(chat_id), "text": text}
    if reply_markup:
        payload["reply_markup"] = reply_markup
    response = requests.post(f"https://api.telegram.org/bot{BOT_TOKEN}/sendMessage", json=payload, timeout=25)
    response.raise_for_status()
    data = response.json()
    if not data.get("ok"):
        raise RuntimeError(data)
    return data


def send_chat_action(chat_id: int, action: str = "typing") -> None:
    if not BOT_TOKEN:
        return
    try:
        requests.post(
            f"https://api.telegram.org/bot{BOT_TOKEN}/sendChatAction",
            json={"chat_id": int(chat_id), "action": action},
            timeout=15,
        )
    except Exception:
        pass
