from __future__ import annotations

import json
from functools import wraps
from pathlib import Path
import sys
from typing import Any, Dict, Optional

from flask import Flask, abort, jsonify, redirect, render_template, request, session, send_from_directory, url_for

from common import (
    ADMIN_PASSWORD,
    ADMIN_USERNAME,
    CRM_PUBLIC_URL,
    SECRET_KEY,
    BASE_DIR,
    WEBAPP_URL,
    add_manager,
    add_message,
    assign_conversation,
    build_summary_from_order,
    connect_db,
    create_conversation,
    extract_short_name,
    format_dt,
    get_client_by_tg_id,
    get_conversation,
    get_messages,
    get_manager,
    init_db,
    list_conversations,
    list_managers,
    manager_display_name,
    now_iso,
    safe_json_loads,
    send_telegram_message,
    set_conversation_status,
    choose_manager_for_new_conversation,
    upsert_client,
    verify_manager_login,
)

sys.path.append(str(Path(__file__).resolve().parent.parent))

app = Flask(__name__, template_folder=str(BASE_DIR / "templates"), static_folder=str(BASE_DIR / "static"))
app.config["SECRET_KEY"] = SECRET_KEY or "trud-crm-secret"


def current_manager():
    manager_id = session.get("manager_id")
    if not manager_id:
        return None
    return get_manager(int(manager_id))


def require_login(fn):
    @wraps(fn)
    def wrapper(*args, **kwargs):
        if not session.get("manager_id"):
            return redirect(url_for("login", next=request.path))
        return fn(*args, **kwargs)
    return wrapper


def is_admin() -> bool:
    mgr = current_manager()
    return bool(mgr and mgr["role"] == "admin")


def conversation_text(conv_row) -> str:
    client_name = extract_short_name(conv_row)
    last_message = ""
    with connect_db() as conn:
        row = conn.execute(
            "SELECT content, sender_type FROM messages WHERE conversation_id = ? ORDER BY id DESC LIMIT 1",
            (conv_row["id"],),
        ).fetchone()
        if row:
            last_message = row["content"][:120]
    return last_message


def format_message_content(msg):
    if msg["message_type"] == "json":
        data = safe_json_loads(msg["content"]) or {}
        order = data.get("order") or {}
        summary = data.get("summary") or build_summary_from_order(order)
        return summary
    return msg["content"]


@app.before_request
def _init():
    init_db()


@app.route("/app.html")
@app.route("/webapp")
def webapp():
    return send_from_directory(str(BASE_DIR / "webapp"), "app.html")


@app.route("/")
@require_login
def dashboard():
    mgr = current_manager()
    status = request.args.get("status", "all")
    q = request.args.get("q", "").strip()
    only_me = request.args.get("mine", "0") == "1"
    selected_id = request.args.get("c")
    if selected_id is None:
        selected_id = request.args.get("conversation_id")
    selected_id = int(selected_id) if selected_id and str(selected_id).isdigit() else None

    conversations = list_conversations(
        status=status,
        q=q,
        manager_id=int(mgr["id"]) if mgr else None,
        only_assigned_to_me=only_me,
    )
    selected = None
    messages = []
    if selected_id:
        selected = get_conversation(selected_id)
        if selected:
            if not is_admin() and selected["assigned_manager_id"] not in (None, int(mgr["id"])):
                # not yours
                selected = None
            else:
                messages = get_messages(selected_id)

    if not selected and conversations:
        selected = conversations[0]
        messages = get_messages(selected["id"])

    stats = {
        "all": len(list_conversations(status="all", q=q, only_assigned_to_me=only_me and not is_admin(), manager_id=int(mgr["id"]) if mgr else None)),
        "new": len(list_conversations(status="new", q=q, only_assigned_to_me=only_me and not is_admin(), manager_id=int(mgr["id"]) if mgr else None)),
        "in_progress": len(list_conversations(status="in_progress", q=q, only_assigned_to_me=only_me and not is_admin(), manager_id=int(mgr["id"]) if mgr else None)),
        "closed": len(list_conversations(status="closed", q=q, only_assigned_to_me=only_me and not is_admin(), manager_id=int(mgr["id"]) if mgr else None)),
    }

    managers = list_managers(active_only=False)
    return render_template(
        "dashboard.html",
        manager=mgr,
        conversations=conversations,
        selected=selected,
        messages=messages,
        managers=managers,
        stats=stats,
        query=q,
        status=status,
        only_me=only_me,
        crm_public_url=CRM_PUBLIC_URL,
        format_dt=format_dt,
        extract_short_name=extract_short_name,
        manager_display_name=manager_display_name,
        format_message_content=format_message_content,
        webapp_url=WEBAPP_URL,
    )


@app.route("/login", methods=["GET", "POST"])
def login():
    error = None
    if request.method == "POST":
        username = request.form.get("username", "").strip()
        password = request.form.get("password", "")
        manager = verify_manager_login(username, password)
        if manager:
            session["manager_id"] = int(manager["id"])
            session["manager_name"] = manager["name"]
            session["role"] = manager["role"]
            return redirect(url_for("dashboard"))
        error = "Неверный логин или пароль"
    return render_template("login.html", error=error, admin_username=ADMIN_USERNAME)


@app.route("/logout")
def logout():
    session.clear()
    return redirect(url_for("login"))


@app.route("/api/conversation/<int:conversation_id>/reply", methods=["POST"])
@require_login
def api_reply(conversation_id: int):
    mgr = current_manager()
    conversation = get_conversation(conversation_id)
    if not conversation:
        return jsonify({"ok": False, "error": "Conversation not found"}), 404

    if not is_admin() and conversation["assigned_manager_id"] not in (None, int(mgr["id"])):
        return jsonify({"ok": False, "error": "Not allowed"}), 403

    payload = request.get_json(force=True, silent=True) or {}
    text = (payload.get("message") or "").strip()
    if not text:
        return jsonify({"ok": False, "error": "Empty message"}), 400

    client_user_id = int(conversation["tg_user_id"])
    # store in DB and send to Telegram
    add_message(conversation_id, "manager", text, sender_name=manager_display_name(mgr), message_type="text")
    set_conversation_status(conversation_id, "in_progress")
    try:
        send_telegram_message(client_user_id, text)
    except Exception as exc:
        return jsonify({"ok": False, "error": f"Telegram send failed: {exc}"}), 500
    return jsonify({"ok": True})


@app.route("/api/conversation/<int:conversation_id>/status", methods=["POST"])
@require_login
def api_status(conversation_id: int):
    mgr = current_manager()
    conversation = get_conversation(conversation_id)
    if not conversation:
        return jsonify({"ok": False, "error": "Conversation not found"}), 404
    if not is_admin() and conversation["assigned_manager_id"] not in (None, int(mgr["id"])):
        return jsonify({"ok": False, "error": "Not allowed"}), 403
    payload = request.get_json(force=True, silent=True) or {}
    status = payload.get("status", "").strip()
    if status not in {"new", "in_progress", "closed"}:
        return jsonify({"ok": False, "error": "Invalid status"}), 400
    set_conversation_status(conversation_id, status)
    if status == "closed":
        try:
            send_telegram_message(int(conversation["tg_user_id"]), "Заявку закрыли. Если понадобится помощь, напишите снова.")
        except Exception:
            pass
    return jsonify({"ok": True})


@app.route("/api/conversation/<int:conversation_id>/assign", methods=["POST"])
@require_login
def api_assign(conversation_id: int):
    if not is_admin():
        return jsonify({"ok": False, "error": "Only admin"}), 403
    payload = request.get_json(force=True, silent=True) or {}
    manager_id = payload.get("manager_id")
    if manager_id in ("", None):
        assign_conversation(conversation_id, None)
        return jsonify({"ok": True})
    manager_id = int(manager_id)
    if not get_manager(manager_id):
        return jsonify({"ok": False, "error": "Manager not found"}), 404
    assign_conversation(conversation_id, manager_id)
    return jsonify({"ok": True})


@app.route("/admin/managers", methods=["GET", "POST"])
@require_login
def admin_managers():
    if not is_admin():
        abort(403)

    error = None
    success = None
    if request.method == "POST":
        action = request.form.get("action")
        if action == "add":
            name = request.form.get("name", "").strip()
            username = request.form.get("username", "").strip()
            password = request.form.get("password", "")
            role = request.form.get("role", "manager")
            tg_chat_id = request.form.get("tg_chat_id", "").strip()
            if not (name and username and password):
                error = "Заполните имя, логин и пароль"
            else:
                try:
                    add_manager(name, username, password, role=role, tg_chat_id=tg_chat_id)
                    success = "Менеджер добавлен"
                except Exception as exc:
                    error = str(exc)
        elif action == "toggle":
            manager_id = int(request.form.get("manager_id", "0"))
            manager = get_manager(manager_id)
            if manager:
                from common import update_manager
                update_manager(manager_id, active=0 if manager["active"] else 1)
                success = "Статус изменён"

    managers = list_managers(active_only=False)
    return render_template("managers.html", managers=managers, error=error, success=success, manager=current_manager())


@app.route("/api/bootstrap", methods=["GET"])
@require_login
def bootstrap():
    """Small JSON for future JS enhancements."""
    conversations = list_conversations(limit=20)
    return jsonify({
        "ok": True,
        "conversations": [
            {
                "id": c["id"],
                "status": c["status"],
                "client_id": c["client_id"],
                "tg_user_id": c["tg_user_id"],
                "manager_name": c["manager_name"],
                "updated_at": c["updated_at"],
            }
            for c in conversations
        ],
    })


if __name__ == "__main__":
    app.run(host="0.0.0.0", port=8000, debug=True)
