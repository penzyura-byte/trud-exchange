from __future__ import annotations

import time
from typing import Any, Dict, Optional

import requests

from common import (
    BOT_TOKEN,
    CRM_PUBLIC_URL,
    WEBAPP_URL,
    add_message,
    assign_conversation,
    build_text_keyboard,
    choose_manager_for_new_conversation,
    create_conversation,
    extract_short_name,
    get_conversation,
    get_open_conversation,
    init_db,
    now_iso,
    send_chat_action,
    send_telegram_message,
    set_conversation_status,
    upsert_client,
)

if not BOT_TOKEN:
    raise RuntimeError("BOT_TOKEN is not set in .env")

API_BASE = f"https://api.telegram.org/bot{BOT_TOKEN}"
OFFSET = 0


def api(method: str, payload: Dict[str, Any]) -> Dict[str, Any]:
    response = requests.post(f"{API_BASE}/{method}", json=payload, timeout=30)
    response.raise_for_status()
    data = response.json()
    if not data.get("ok"):
        raise RuntimeError(data)
    return data


def ensure_open_conversation(user: Dict[str, Any], source: str = "telegram") -> int:
    client_id = upsert_client(user)
    open_conv = get_open_conversation(client_id)
    if open_conv:
        return int(open_conv["id"])
    conv_id = create_conversation(client_id, source=source)
    manager = choose_manager_for_new_conversation()
    if manager:
        assign_conversation(conv_id, int(manager["id"]))
    return conv_id


def send_start_menu(chat_id: int) -> None:
    api(
        "sendMessage",
        {
            "chat_id": chat_id,
            "text": (
                "Здравствуйте. Это форма заявки Trud Exchange.\n\n"
                "Нажмите кнопку ниже, заполните заявку и она сразу попадёт в CRM менеджера."
            ),
            "reply_markup": build_text_keyboard(),
            "disable_web_page_preview": True,
        },
    )


def process_user_text(message: Dict[str, Any]) -> None:
    user = message["from"]
    chat_id = message["chat"]["id"]
    text = (message.get("text") or "").strip()
    if not text:
        return
    if text.startswith("/start"):
        send_start_menu(chat_id)
        return

    conv_id = ensure_open_conversation(user)
    add_message(conv_id, "client", text, sender_name=user.get("first_name") or user.get("username") or "Клиент", message_type="text")

    conversation = get_conversation(conv_id)
    if conversation and conversation["status"] == "closed":
        set_conversation_status(conv_id, "new")

    try:
        api(
            "sendMessage",
            {
                "chat_id": chat_id,
                "text": "Принял сообщение. Менеджер уже видит вашу заявку в CRM.",
                "reply_markup": build_text_keyboard(),
            },
        )
    except Exception:
        pass


def process_webapp_data(message: Dict[str, Any]) -> None:
    user = message["from"]
    chat_id = message["chat"]["id"]
    data = message["web_app_data"]["data"]
    conv_id = ensure_open_conversation(user, source="web_app")

    add_message(
        conv_id,
        "client",
        data,
        sender_name=user.get("first_name") or user.get("username") or "Клиент",
        message_type="text",
    )

    conversation = get_conversation(conv_id)
    if conversation and conversation["status"] == "closed":
        set_conversation_status(conv_id, "new")

    try:
        api(
            "sendMessage",
            {
                "chat_id": chat_id,
                "text": "Принял заявку. Сейчас менеджер продолжит общение в этом же чате.",
                "reply_markup": build_text_keyboard(),
            },
        )
    except Exception:
        pass


def handle_update(update: Dict[str, Any]) -> None:
    message = update.get("message") or update.get("edited_message")
    if not message:
        return
    if message.get("from", {}).get("is_bot"):
        return

    if message.get("web_app_data"):
        process_webapp_data(message)
        return

    if message.get("text"):
        process_user_text(message)
        return


def poll() -> None:
    global OFFSET
    while True:
        try:
            data = api(
                "getUpdates",
                {"offset": OFFSET, "timeout": 25, "allowed_updates": ["message", "edited_message"]},
            )
            for item in data.get("result", []):
                OFFSET = item["update_id"] + 1
                handle_update(item)
        except Exception as exc:
            print(f"Polling error: {exc}")
            time.sleep(3)


if __name__ == "__main__":
    init_db()
    print("Bot started")
    poll()
