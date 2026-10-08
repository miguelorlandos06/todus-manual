#!/usr/bin/env python3
"""Cliente toDus manual: login protobuf + XMPP socket + stanza XML."""
import os, re, ssl, time, uuid, socket, base64, hashlib, logging, threading
from pathlib import Path
import requests

AUTH_URL = "https://auth.todus.cu/v2/auth/token"
XMPP_HOST = "ws.todus.cu"
XMPP_PORT = 5222
XMPP_DOMAIN = "im.todus.cu"

FAKE_UUID = "fake-1234-5678-90ab-cdef12345678"
FAKE_SECRET = FAKE_UUID.replace("-", "")[:32]

KEEPALIVE_INTERVAL = 25

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
log = logging.getLogger("todus")


def _varint(n):
    out = b""
    while True:
        b = n & 0x7F
        n >>= 7
        if n:
            out += bytes([b | 0x80])
        else:
            out += bytes([b])
            break
    return out


def _sf(field_num, value):
    if isinstance(value, str):
        value = value.encode("utf-8")
    tag = bytes([(field_num << 3) | 2])
    return tag + _varint(len(value)) + value


def login_with_phone_only(phone):
    phone = re.sub(r"[^\d]", "", phone)
    payload = _sf(1, phone) + _sf(2, FAKE_SECRET)
    r = requests.post(
        AUTH_URL, data=payload,
        headers={"Content-Type": "application/x-protobuf", "User-Agent": "ToDus 2.1.2 Auth"},
        timeout=30, verify=False,
    )
    r.raise_for_status()
    text = r.content.decode("utf-8", errors="ignore")
    m = re.search(r"eyJ[\w\-\.]+", text)
    if not m:
        raise RuntimeError(f"No JWT en respuesta: {r.content[:200]}")
    return m.group(0)


class ToDusXMPP:
    def __init__(self, phone, jwt):
        self.phone = re.sub(r"[^\d]", "", phone)
        self.jwt = jwt
        self.sock = None
        self.jid = f"{self.phone}@{XMPP_DOMAIN}"
        self.resource = hashlib.md5(self.phone.encode()).hexdigest() + "_Android"
        self.full_jid = f"{self.jid}/{self.resource}"
        self._buffer = b""
        self._keepalive_thread = None
        self._running = False
        self._iq_responses = {}
        self._reader_thread = None
        self._stanza_handlers = []

    def connect(self):
        ctx = ssl.create_default_context()
        ctx.check_hostname = False
        ctx.verify_mode = ssl.CERT_NONE
        log.info(f"Conectando a {XMPP_HOST}:{XMPP_PORT}")
        raw = socket.create_connection((XMPP_HOST, XMPP_PORT), timeout=30)
        self.sock = ctx.wrap_socket(raw, server_hostname=XMPP_HOST)
        self.sock.settimeout(30)

        self._send_raw(f'<?xml version="1.0"?><stream:stream to="{XMPP_DOMAIN}" xmlns="jc" xmlns:stream="x1" version="1.0">')
        time.sleep(0.5)
        self._drain()

        auth_data = b"\x00" + self.phone.encode() + b"\x00" + self.jwt.encode()
        auth_b64 = base64.b64encode(auth_data).decode()
        self._send_raw(f'<auth mechanism="PLAIN" xmlns="urn:ietf:params:xml:ns:xmpp-sasl">{auth_b64}</auth>')
        time.sleep(0.5)
        resp = self._drain()
        if "<success" not in resp:
            raise RuntimeError(f"SASL falló: {resp[:500]}")
        log.info("SASL PLAIN OK")

        self._send_raw(f'<?xml version="1.0"?><stream:stream to="{XMPP_DOMAIN}" xmlns="jc" xmlns:stream="x1" version="1.0">')
        time.sleep(0.5)
        self._drain()

        bind_id = uuid.uuid4().hex
        self._send_raw(f'<iq type="set" id="{bind_id}"><bind xmlns="urn:ietf:params:xml:ns:xmpp-bind"><resource>{self.resource}</resource></bind></iq>')
        time.sleep(0.5)
        resp = self._drain()
        if "<jid>" in resp:
            m = re.search(r"<jid>([^<]+)</jid>", resp)
            if m:
                self.full_jid = m.group(1)
                log.info(f"Bind OK: {self.full_jid}")

        self._send_raw('<iq type="set" id="sess1"><session xmlns="urn:ietf:params:xml:ns:xmpp-session"/></iq>')
        time.sleep(0.3)
        self._drain()

        self._send_raw("<presence/>")
        time.sleep(0.3)
        self._drain()

        log.info("XMPP conectado")
        self._running = True
        self._reader_thread = threading.Thread(target=self._reader_loop, daemon=True)
        self._reader_thread.start()
        self._keepalive_thread = threading.Thread(target=self._keepalive_loop, daemon=True)
        self._keepalive_thread.start()

    def _send_raw(self, data):
        if self.sock:
            self.sock.sendall(data.encode("utf-8"))

    def send_stanza(self, stanza):
        log.debug(f">>> {stanza}")
        self._send_raw(stanza)

    def _drain(self, timeout=2):
        if not self.sock:
            return ""
        self.sock.settimeout(timeout)
        data = b""
        try:
            while True:
                chunk = self.sock.recv(8192)
                if not chunk:
                    break
                data += chunk
        except socket.timeout:
            pass
        return data.decode("utf-8", errors="replace")

    def _reader_loop(self):
        self.sock.settimeout(1)
        while self._running:
            try:
                chunk = self.sock.recv(8192)
                if not chunk:
                    log.warning("Socket cerrado")
                    break
                self._buffer += chunk
                self._process_buffer()
            except socket.timeout:
                continue
            except Exception as e:
                if self._running:
                    log.warning(f"Lector error: {e}")
                break

    def _process_buffer(self):
        text = self._buffer.decode("utf-8", errors="replace")
        pattern = re.compile(r"<(iq|message|presence|m)\b[^>]*>.*?</\1>|<(iq|message|presence|m)\b[^>]*/>", re.DOTALL)
        consumed = 0
        for m in pattern.finditer(text):
            self._handle_stanza(m.group(0))
            consumed = m.end()
        if consumed:
            self._buffer = self._buffer[consumed:]

    def _handle_stanza(self, stanza):
        log.debug(f"<<< {stanza[:200]}")
        id_m = re.search(r'<iq[^>]*id="([^"]+)"', stanza)
        if id_m and id_m.group(1) in self._iq_responses:
            self._iq_responses[id_m.group(1)] = stanza
            return
        if "<tdack" in stanza:
            log.info(f"ACK: {stanza}")
            return
        if stanza.startswith("<message") or "<m " in stanza:
            for handler in self._stanza_handlers:
                try:
                    handler(stanza)
                except Exception as e:
                    log.exception(f"Handler error: {e}")

    def _keepalive_loop(self):
        while self._running:
            time.sleep(KEEPALIVE_INTERVAL)
            if self._running and self.sock:
                try:
                    self._send_raw(" ")
                except Exception:
                    break

    def send_iq_and_wait(self, iq, timeout=10):
        id_m = re.search(r'id="([^"]+)"', iq)
        if not id_m:
            raise ValueError("IQ sin id")
        iq_id = id_m.group(1)
        self._iq_responses[iq_id] = None
        self.send_stanza(iq)
        start = time.time()
        while time.time() - start < timeout:
            if self._iq_responses.get(iq_id):
                return self._iq_responses.pop(iq_id)
            time.sleep(0.1)
        self._iq_responses.pop(iq_id, None)
        raise TimeoutError(f"IQ {iq_id} sin respuesta")

    def on_message(self, callback):
        self._stanza_handlers.append(callback)

    def close(self):
        self._running = False
        if self.sock:
            try:
                self._send_raw("</stream:stream>")
                self.sock.close()
            except Exception:
                pass


FILE_TYPE_DOC = "0"
FILE_TYPE_VOICE = "1"
FILE_TYPE_AUDIO = "2"
FILE_TYPE_VIDEO = "3"
FILE_TYPE_IMAGE = "4"
FILE_TYPE_PROFILE = "5"
FILE_TYPE_PROFILE_THUMB = "6"


def reserve_upload_url(xmpp, size, file_type=FILE_TYPE_VIDEO, room=""):
    iq_id = uuid.uuid4().hex
    iq = f'<iq type="get" id="{iq_id}"><query xmlns="todus:purl" type="{file_type}" persistent="true" size="{size}" room="{room}"/></iq>'
    resp = xmpp.send_iq_and_wait(iq, timeout=15)
    put_m = re.search(r'put="([^"]+)"', resp)
    get_m = re.search(r'get="([^"]+)"', resp)
    if not put_m or not get_m:
        raise RuntimeError(f"PUrl sin put/get: {resp[:400]}")
    return put_m.group(1), get_m.group(1)


def upload_to_s3(put_url, data, content_type="application/octet-stream"):
    r = requests.put(put_url, data=data, headers={"Content-Type": content_type, "Content-Length": str(len(data))}, timeout=600, verify=False)
    r.raise_for_status()
    log.info(f"Subido a S3: {len(data)} bytes")


if __name__ == "__main__":
    PHONE = os.environ.get("TODUS_PHONE", "5350155246")
    jwt = login_with_phone_only(PHONE)
    log.info(f"JWT: {jwt[:50]}...")
    xmpp = ToDusXMPP(PHONE, jwt)
    xmpp.connect()
    log.info("Listo. Ctrl+C para salir.")
    try:
        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        xmpp.close()
