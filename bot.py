#!/usr/bin/env python3
"""Bot de Telegram -> ToDus usando cliente XMPP manual."""
import os, re, uuid, shutil, asyncio, logging, time
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
PENDING_TTL = 600  # ✅ 10 min de expiración

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
log = logging.getLogger("bot")
logging.getLogger("httpx").setLevel(logging.WARNING)

_xmpp = None
_xmpp_lock = asyncio.Lock()


def _is_xmpp_alive(x) -> bool:
    try:
        return x is not None and x._running and x.sock is not None
    except Exception:
        return False


async def get_xmpp():
    """Obtiene XMPP, reconectando si es necesario."""
    global _xmpp
    async with _xmpp_lock:
        if _is_xmpp_alive(_xmpp):
            return _xmpp
        log.info("Login toDus...")
        jwt = await asyncio.to_thread(login_with_phone_only, TODUS_PHONE)
        log.info("Conectando XMPP...")
        xmpp = ToDusXMPP(TODUS_PHONE, jwt)
        await asyncio.to_thread(xmpp.connect)
        log.info("toDus conectado")
        _xmpp = xmpp
        return _xmpp


pending = {}  # uid -> {"url","file_type","size","ts"}
URL_RE = re.compile(r"https?://[^\s]+", re.IGNORECASE)


def _cleanup_pending():
    now = time.time()
    expired = [uid for uid, v in pending.items() if now - v.get("ts", 0) > PENDING_TTL]
    for uid in expired:
        pending.pop(uid, None)


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


def _xml_escape(s: str) -> str:
    return (s.replace("&", "&amp;").replace("<", "&lt;").replace(">", "&gt;")
             .replace('"', "&quot;").replace("'", "&apos;"))


def _send_stanza(xmpp, phone, url, ftype, size, name, extra=None):
    msg_id = uuid.uuid4().hex[:16]
    file_id = uuid.uuid4().hex[:16]
    extra = extra or {}
    # ✅ Escapar valores que podrían romper el XML
    url_e = _xml_escape(url)
    name_e = _xml_escape(name)
    to = f"{phone}@im.todus.cu"
    if ftype == "video":
        stanza = (f'<m to="{to}" t="c" i="{msg_id}" xmlns="jc"><k xmlns="x8"/>'
                  f'<video xmlns="video:n" i="{file_id}" mi="{msg_id}" url="{url_e}" s="{size}" '
                  f'h="" d="{extra.get("duration",0)}" n="{name_e}" w="{extra.get("width",0)}" '
                  f'he="{extra.get("height",0)}" tnail=""/><b/></m>')
    elif ftype == "image":
        stanza = (f'<m to="{to}" t="c" i="{msg_id}" xmlns="jc"><k xmlns="x8"/>'
                  f'<image xmlns="image:n" i="{file_id}" mi="{msg_id}" url="{url_e}" n="{name_e}" '
                  f's="{size}" h="" w="{extra.get("width",0)}" he="{extra.get("height",0)}" tnail=""/><b/></m>')
    elif ftype == "audio":
        stanza = (f'<m to="{to}" t="c" i="{msg_id}" xmlns="jc"><k xmlns="x8"/>'
                  f'<voice xmlns="voice:n" i="{file_id}" mi="{msg_id}" url="{url_e}" s="{size}" '
                  f'h="" d="{extra.get("duration",0)}" ws=""/><b/></m>')
    else:
        stanza = (f'<m to="{to}" t="c" i="{msg_id}" xmlns="jc"><k xmlns="x8"/>'
                  f'<file xmlns="file:n" i="{file_id}" mi="{msg_id}" n="{name_e}" url="{url_e}" '
                  f's="{size}" h=""/><b/></m>')
    xmpp.send_stanza(stanza)
    return msg_id


async def cmd_start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await update.message.reply_text(
        "🔗 Envío a ToDus\n\n1. Envíame una URL\n2. Te pido el número toDus\n3. Lo subo y lo envío"
    )


async def _probe_url(session, url):
    """Intenta HEAD, cae a GET con Range si HEAD no está permitido."""
    headers = {"User-Agent": "Mozilla/5.0"}
    try:
        async with session.head(url, headers=headers, timeout=15, allow_redirects=True) as r:
            if r.status < 400:
                ct = r.headers.get("Content-Type", "")
                size = int(r.headers.get("Content-Length", 0) or 0)
                if size or r.status == 200:
                    return ct, size
    except Exception:
        pass
    # Fallback: GET con Range 0-0
    headers["Range"] = "bytes=0-0"
    async with session.get(url, headers=headers, timeout=20, allow_redirects=True) as r:
        if r.status >= 400:
            raise RuntimeError(f"HTTP {r.status}")
        ct = r.headers.get("Content-Type", "")
        cr = r.headers.get("Content-Range", "")
        size = 0
        if "/" in cr:
            try:
                size = int(cr.split("/")[-1])
            except ValueError:
                size = 0
        if not size:
            size = int(r.headers.get("Content-Length", 0) or 0)
        return ct, size


async def handle_url(update: Update, context: ContextTypes.DEFAULT_TYPE):
    uid = update.effective_user.id
    _cleanup_pending()
    text = (update.message.text or "").strip()
    m = URL_RE.search(text)
    if not m:
        await update.message.reply_text("URL inválida.")
        return
    url = m.group(0)
    try:
        async with aiohttp.ClientSession() as s:
            ct, size = await _probe_url(s, url)
    except Exception as e:
        await update.message.reply_text(f"Error: {str(e)[:200]}")
        return
    if size and size > MAX_SIZE:
        await update.message.reply_text(f"Muy grande: {fmt_size(size)}")
        return
    ftype = detect_file_type(url, ct)
    pending[uid] = {"url": url, "file_type": ftype, "size": size, "ts": time.time()}
    await update.message.reply_text(
        f"📎 Tipo: {ftype}\n📊 Tamaño: {fmt_size(size) if size else '?'}\n\n¿Número toDus?"
    )


async def handle_phone(update: Update, context: ContextTypes.DEFAULT_TYPE):
    uid = update.effective_user.id
    if uid not in pending:
        return
    phone = (update.message.text or "").strip().replace("+", "").replace(" ", "").replace("-", "")
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
            async with s.get(url, headers={"User-Agent": "Mozilla/5.0"}, timeout=aiohttp.ClientTimeout(total=None, sock_read=120)) as r:
                if r.status >= 400:
                    raise RuntimeError(f"HTTP {r.status}")
                async with aiofiles.open(local_file, "wb") as f:
                    async for chunk in r.content.iter_chunked(1024 * 1024):
                        await f.write(chunk)
        size = local_file.stat().st_size

        # ✅ Esperar a tener XMPP listo (con reconexión)
        try:
            xmpp = await get_xmpp()
        except Exception as e:
            raise RuntimeError(f"No se pudo conectar a ToDus: {e}")

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
        await status.edit_text(
            f"✅ Enviado\n📎 {ftype}\n📊 {fmt_size(size)}\n📱 {phone}\n🆔 {msg_id}"
        )
    except Exception as e:
        log.exception("Error")
        try:
            await status.edit_text(f"❌ Error: {str(e)[:300]}")
        except Exception:
            pass
    finally:
        shutil.rmtree(temp_dir, ignore_errors=True)


async def handle_text(update: Update, context: ContextTypes.DEFAULT_TYPE):
    uid = update.effective_user.id
    text = (update.message.text or "").strip()

    # ✅ Si hay pendiente y el texto parece número → tratar como teléfono
    if uid in pending:
        digits = text.replace("+", "").replace(" ", "").replace("-", "")
        if digits.isdigit() and len(digits) >= 8:
            await handle_phone(update, context)
            return
        # Si no es número, y contiene URL, reemplazar pendiente
        if URL_RE.search(text):
            await handle_url(update, context)
            return
        await update.message.reply_text(
            "Envíame un número toDus válido o una nueva URL para reemplazar la pendiente."
        )
        return

    # Sin pendiente: solo aceptar si es URL
    if URL_RE.search(text):
        await handle_url(update, context)
    else:
        await update.message.reply_text("Envíame una URL para descargar y enviar a toDus.")


def main():
    application = (
        Application.builder()
        .token(TG_BOT_TOKEN)
        .request(AiohttpRequest(connection_pool_size=256))
        .get_updates_request(AiohttpRequest())
        .build()
    )
    application.add_handler(CommandHandler("start", cmd_start))
    application.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, handle_text))
    log.info("Bot listo")
    application.run_polling(allowed_updates=Update.ALL_TYPES)


if __name__ == "__main__":
    main()