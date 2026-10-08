#!/usr/bin/env python3
"""Bot de Telegram -> ToDus usando cliente XMPP manual."""
import os, re, uuid, shutil, asyncio, logging
from pathlib import Path
import aiohttp, aiofiles
from telegram import Update
from telegram.ext import Application, CommandHandler, MessageHandler, filters, ContextTypes
from ptbcontrib.aiohttp_request import AiohttpRequest
from todus_client import (
    login_with_phone_only, ToDusXMPP, reserve_upload_url, upload_to_s3,
    FILE_TYPE_IMAGE, FILE_TYPE_VOICE, FILE_TYPE_VIDEO, FILE_TYPE_DOC,
)

TG_BOT_TOKEN = os.environ["TG_BOT_TOKEN"]
TODUS_PHONE = os.environ["TODUS_PHONE"]
WORK_DIR = "/tmp/todus_jobs"
os.makedirs(WORK_DIR, exist_ok=True)
MAX_SIZE = 2 * 1024 * 1024 * 1024

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
log = logging.getLogger("bot")
logging.getLogger("httpx").setLevel(logging.WARNING)

_xmpp = None

def get_xmpp():
    global _xmpp
    if _xmpp is None:
        log.info("Login toDus...")
        jwt = login_with_phone_only(TODUS_PHONE)
        log.info("Conectando XMPP...")
        _xmpp = ToDusXMPP(TODUS_PHONE, jwt)
        _xmpp.connect()
        log.info("toDus conectado")
    return _xmpp

pending = {}
URL_RE = re.compile(r"https?://[^\s]+", re.IGNORECASE)


def detect_file_type(url, content_type=""):
    ext = Path(url.split("?")[0]).suffix.lower()
    if ext in (".mp4", ".mkv", ".mov", ".avi", ".webm"): return "video"
    if ext in (".jpg", ".jpeg", ".png", ".gif", ".webp"): return "image"
    if ext in (".mp3", ".ogg", ".wav", ".m4a", ".aac"): return "audio"
    if content_type.startswith("video/"): return "video"
    if content_type.startswith("image/"): return "image"
    if content_type.startswith("audio/"): return "audio"
    return "document"


def fmt_size(b):
    if b < 1024: return f"{b} B"
    if b < 1048576: return f"{b/1024:.1f} KB"
    if b < 1073741824: return f"{b/1048576:.1f} MB"
    return f"{b/1073741824:.2f} GB"


def _send_stanza(xmpp, phone, url, ftype, size, name, extra=None):
    msg_id = uuid.uuid4().hex[:16]
    file_id = uuid.uuid4().hex[:16]
    extra = extra or {}
    if ftype == "video":
        stanza = f'<m to="{phone}@im.todus.cu" t="c" i="{msg_id}" xmlns="jc"><k xmlns="x8"/><video xmlns="video:n" i="{file_id}" mi="{msg_id}" url="{url}" s="{size}" h="" d="{extra.get("duration",0)}" n="{name}" w="{extra.get("width",0)}" he="{extra.get("height",0)}" tnail=""/><b/></m>'
    elif ftype == "image":
        stanza = f'<m to="{phone}@im.todus.cu" t="c" i="{msg_id}" xmlns="jc"><k xmlns="x8"/><image xmlns="image:n" i="{file_id}" mi="{msg_id}" url="{url}" n="{name}" s="{size}" h="" w="{extra.get("width",0)}" he="{extra.get("height",0)}" tnail=""/><b/></m>'
    elif ftype == "audio":
        stanza = f'<m to="{phone}@im.todus.cu" t="c" i="{msg_id}" xmlns="jc"><k xmlns="x8"/><voice xmlns="voice:n" i="{file_id}" mi="{msg_id}" url="{url}" s="{size}" h="" d="{extra.get("duration",0)}" ws=""/><b/></m>'
    else:
        stanza = f'<m to="{phone}@im.todus.cu" t="c" i="{msg_id}" xmlns="jc"><k xmlns="x8"/><file xmlns="file:n" i="{file_id}" mi="{msg_id}" n="{name}" url="{url}" s="{size}" h=""/><b/></m>'
    xmpp.send_stanza(stanza)
    return msg_id


async def cmd_start(update, context):
    await update.message.reply_text("🔗 Envío a ToDus\n\n1. Envíame una URL\n2. Te pido el número toDus\n3. Lo subo y lo envío")


async def handle_url(update, context):
    uid = update.effective_user.id
    m = URL_RE.search(update.message.text.strip())
    if not m:
        await update.message.reply_text("URL inválida.")
        return
    url = m.group(0)
    try:
        async with aiohttp.ClientSession() as s:
            async with s.head(url, headers={"User-Agent": "Mozilla/5.0"}, timeout=15) as r:
                ct = r.headers.get("Content-Type", "")
                size = int(r.headers.get("Content-Length", 0))
    except Exception as e:
        await update.message.reply_text(f"Error: {str(e)[:200]}")
        return
    if size > MAX_SIZE:
        await update.message.reply_text(f"Muy grande: {fmt_size(size)}")
        return
    ftype = detect_file_type(url, ct)
    pending[uid] = {"url": url, "file_type": ftype, "size": size}
    await update.message.reply_text(f"📎 Tipo: {ftype}\n📊 Tamaño: {fmt_size(size) if size else '?'}\n\n¿Número toDus?")


async def handle_phone(update, context):
    uid = update.effective_user.id
    if uid not in pending:
        return
    phone = update.message.text.strip().replace("+", "").replace(" ", "")
    if not phone.isdigit() or len(phone) < 8:
        await update.message.reply_text("Número inválido.")
        return
    info = pending.pop(uid)
    url = info["url"]
    ftype = info["file_type"]
    status = await update.message.reply_text(f"⏳ Procesando {ftype}...")
    temp_dir = Path(WORK_DIR) / uuid.uuid4().hex[:8]
    temp_dir.mkdir(parents=True, exist_ok=True)
    try:
        await status.edit_text("⬇️ Descargando...")
        ext = Path(url.split("?")[0]).suffix or ".bin"
        local_file = temp_dir / f"file{ext}"
        async with aiohttp.ClientSession() as s:
            async with s.get(url, headers={"User-Agent": "Mozilla/5.0"}) as r:
                if r.status >= 400:
                    raise RuntimeError(f"HTTP {r.status}")
                async with aiofiles.open(local_file, "wb") as f:
                    async for chunk in r.content.iter_chunked(1024 * 1024):
                        await f.write(chunk)
        size = local_file.stat().st_size
        xmpp = await asyncio.to_thread(get_xmpp)
        await status.edit_text("⬆️ Subiendo a ToDus S3...")
        with open(local_file, "rb") as f:
            data = f.read()
        if ftype == "video":
            code, ct_type, extra = FILE_TYPE_VIDEO, "video/mp4", {"duration": 0, "width": 0, "height": 0}
        elif ftype == "image":
            code, ct_type, extra = FILE_TYPE_IMAGE, "image/jpeg", {"width": 0, "height": 0}
        elif ftype == "audio":
            code, ct_type, extra = FILE_TYPE_VOICE, "audio/mpeg", {"duration": 0}
        else:
            code, ct_type, extra = FILE_TYPE_DOC, "application/octet-stream", {}
        def _do_upload():
            put_url, get_url = reserve_upload_url(xmpp, len(data), code)
            upload_to_s3(put_url, data, ct_type)
            return get_url
        get_url = await asyncio.to_thread(_do_upload)
        await status.edit_text("📤 Enviando mensaje...")
        msg_id = await asyncio.to_thread(_send_stanza, xmpp, phone, get_url, ftype, size, local_file.name, extra)
        await status.edit_text(f"✅ Enviado\n📎 {ftype}\n📊 {fmt_size(size)}\n📱 {phone}\n🆔 {msg_id}")
    except Exception as e:
        log.exception("Error")
        await status.edit_text(f"❌ Error: {str(e)[:300]}")
    finally:
        shutil.rmtree(temp_dir, ignore_errors=True)


async def handle_text(update, context):
    uid = update.effective_user.id
    if uid in pending:
        await handle_phone(update, context)
    else:
        await handle_url(update, context)


def main():
    application = Application.builder().token(TG_BOT_TOKEN).request(AiohttpRequest(connection_pool_size=256)).get_updates_request(AiohttpRequest()).build()
    application.add_handler(CommandHandler("start", cmd_start))
    application.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, handle_text))
    log.info("Bot listo")
    application.run_polling(allowed_updates=Update.ALL_TYPES)


if __name__ == "__main__":
    main()
