from __future__ import annotations

import json
import time
from typing import Any, Dict, Optional

import requests

API_BASE = f"https://api.telegram.org/bot{BOT_TOKEN}"


def api(method: str, payload: Dict[str, Any]) -> Dict[str, Any]:
    if not BOT_TOKEN:
        raise RuntimeError("BOT_TOKEN is not set in .env")
    response = requests.post(f"{API_BASE}/{method}", json=payload, timeout=25)
    response.raise_for_status()
    data = response.json()
    if not data.get("ok"):
        raise RuntimeError(data)
    return data


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


def ensure_open_conversation(tg_user: Dict[str, Any], source: str = "telegram") -> int:
    client_id = upsert_client(tg_user)
    # get latest open conversation
    from common import get_open_conversation

    open_conv = get_open_conversation(client_id)
    if open_conv:
        conv_id = int(open_conv["id"])
    else:
        conv_id = create_conversation(client_id, source=source)
        manager = choose_manager_for_new_conversation()
        if manager:
            assign_conversation(conv_id, int(manager["id"]))
    return conv_id


def notify_manager(conversation_id: int, text: str) -> None:
    conv = None
    from common import get_conversation

    conv = get_conversation(conversation_id)
    if not conv:
        return
    manager_chat_id = conv["manager_tg_chat_id"]
    if manager_chat_id:
        try:
            api(
                "sendMessage",
                {
                    "chat_id": manager_chat_id,
                    "text": f"Новая/обновлённая заявка #{conversation_id}\n{CRM_PUBLIC_URL}/?c={conversation_id}\n\n{text}",
                    "disable_web_page_preview": True,
                },
            )
        except Exception:
            pass


def handle_user_text(message: Dict[str, Any]) -> None:
    user = message["from"]
    chat_id = message["chat"]["id"]
    text = message.get("text", "").strip()

    if not text:
        return

    if text.startswith("/start"):
        send_start_menu(chat_id)
        return

    conv_id = ensure_open_conversation(user)

    add_message(conv_id, "client", text, sender_name=user.get("first_name") or user.get("username") or "Клиент")
    from common import get_conversation
    conv = get_conversation(conv_id)
    if conv and conv["status"] == "closed":
        set_conversation_status(conv_id, "new")

    assigned_manager = conv["manager_name"] if conv else ""
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

    summary = f"Клиент: {user.get('first_name') or ''} @{user.get('username') or ''}\n\n{text}"
    notify_manager(conv_id, summary)


def handle_webapp_data(message: Dict[str, Any]) -> None:
    user = message["from"]
    chat_id = message["chat"]["id"]
    data = message["web_app_data"]["data"]

    parsed = None
    try:
        parsed = json.loads(data)
    except Exception:
        parsed = None

    if isinstance(parsed, dict):
        order = parsed
        summary = build_summary_from_order(order)
        content = json.dumps({"order": order, "summary": summary}, ensure_ascii=False)
    else:
        summary = data
        content = data

    conv_id = ensure_open_conversation(user, source="mini_app")
    add_message(conv_id, "client", content, sender_name=user.get("first_name") or user.get("username") or "Клиент", message_type="json" if isinstance(parsed, dict) else "text")

    try:
        api(
            "sendMessage",
            {
                "chat_id": chat_id,
                "text": "Заявка отправлена в CRM. Менеджер продолжит общение внутри бота.",
                "reply_markup": build_text_keyboard(),
            },
        )
    except Exception:
        pass

    notify_manager(conv_id, summary)


def poll() -> None:
    offset = None
    while True:
        try:
            payload: Dict[str, Any] = {"timeout": 30, "allowed_updates": ["message"]}
            if offset is not None:
                payload["offset"] = offset
            data = api("getUpdates", payload)["result"]
            for update in data:
                offset = update["update_id"] + 1
                message = update.get("message")
                if not message:
                    continue

                if "web_app_data" in message:
                    handle_webapp_data(message)
                    continue

                if message.get("text"):
                    handle_user_text(message)
                    continue

        except Exception as exc:
            print(f"[bot] error: {exc}")
            time.sleep(2)


def main() -> None:
    init_db()
    if not BOT_TOKEN:
        raise SystemExit("BOT_TOKEN is empty. Fill .env first.")
    print("Bot started")
    poll()


if __name__ == "__main__":
    main()
