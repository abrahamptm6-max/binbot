"""
Bot de Telegram: consulta de BIN (banco emisor, red, tipo, país).
Siempre toma los primeros 6 dígitos aunque envíen más.

Fuente única: dataset local (data/bins.sqlite3, generado desde data/bins.csv).
Sin consultas en línea. Baja memoria: no carga los 458.051 BINs en RAM.

Instalación:  pip install -r requirements.txt
Ejecución:    BOT_TOKEN="123:ABC..." python bin_bot.py
"""
import asyncio
import csv
import html
import json
import os
import re
import sqlite3
import threading
import time
import logging
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path

from telegram import Update
from telegram.error import BadRequest, NetworkError, TimedOut
from telegram.ext import Application, CommandHandler, MessageHandler, ContextTypes, filters

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger("binbot")

DATA_FILE = Path(__file__).parent / "data" / "bins.csv"
DB_FILE = Path(__file__).parent / "data" / "bins.sqlite3"
CURRENCIES_FILE = Path(__file__).parent / "data" / "currencies.json"
USER_COOLDOWN = float(os.environ.get("USER_COOLDOWN", "0"))  # 0 = sin espera
TYPES = {
    "debit": "Débito",
    "credit": "Crédito",
    "charge": "Cargo",
    "prepaid": "Prepago",
}
EMOJI_ESTRELLA = '<tg-emoji emoji-id="5330184289852091742">⭐</tg-emoji>'
EMOJI_DIAMANTE = '<tg-emoji emoji-id="5332659934771310548">♦️</tg-emoji>'
EMOJI_GLOBO = '<tg-emoji emoji-id="5330250874730082574">🌐</tg-emoji>'
EMOJI_PUNTO = '<tg-emoji emoji-id="5318777573973444452">🔴</tg-emoji>'


class KeepAliveHandler(BaseHTTPRequestHandler):
    """Responde a UptimeRobot/Render para que el plan free no se duerma."""

    def _ok(self, head: bool = False) -> None:
        self.send_response(200)
        self.send_header("Content-Type", "text/plain; charset=utf-8")
        self.send_header("Content-Length", "2")
        self.end_headers()
        if not head:
            self.wfile.write(b"ok")

    def do_GET(self) -> None:
        self._ok()

    def do_HEAD(self) -> None:
        self._ok(head=True)

    def log_message(self, fmt: str, *args: object) -> None:
        pass


def start_keepalive_server() -> ThreadingHTTPServer | None:
    port = int(os.environ.get("PORT", "10000"))
    try:
        server = ThreadingHTTPServer(("0.0.0.0", port), KeepAliveHandler)
    except OSError as e:
        log.warning("No se pudo abrir keep-alive en el puerto %s: %s", port, e)
        return None
    threading.Thread(target=server.serve_forever, daemon=True).start()
    log.info("Keep-alive HTTP en 0.0.0.0:%s", port)
    return server


async def reply_html_safe(update: Update, text: str) -> None:
    """Envía HTML con custom emoji; tolera errores de formato y de red."""
    for intento in range(2):
        try:
            await update.message.reply_html(text)
            return
        except BadRequest as e:
            log.warning("HTML rechazado (%s); reenvío sin etiquetas tg-emoji", e)
            plain = re.sub(r"<tg-emoji[^>]*>.*?</tg-emoji>", "", text)
            plain = "\n".join(line.strip() for line in plain.splitlines())
            await update.message.reply_text(plain)
            return
        except (TimedOut, NetworkError) as e:
            log.warning("Telegram lento (%s); intento %d/2", e, intento + 1)
            if intento == 0:
                continue
            await update.message.reply_text("⚠️ Telegram está respondiendo lento. Intentá de nuevo.")
            return


last_call: dict[int, float] = {}
currencies: dict[str, str] = {}
db: sqlite3.Connection | None = None


def build_sqlite_from_csv() -> None:
    """Convierte el CSV descargado en SQLite con índice por BIN (una sola vez)."""
    if not DATA_FILE.is_file():
        raise FileNotFoundError(f"No existe {DATA_FILE}")
    tmp = DB_FILE.with_name(DB_FILE.name + ".tmp")
    tmp.unlink(missing_ok=True)
    conn = sqlite3.connect(tmp)
    try:
        conn.execute("PRAGMA journal_mode=OFF")
        conn.execute("PRAGMA synchronous=OFF")
        conn.execute(
            "CREATE TABLE bins ("
            "bin TEXT PRIMARY KEY, brand TEXT, type TEXT, category TEXT, "
            "country TEXT, alpha_2 TEXT, issuer TEXT)"
        )
        batch: list[tuple[str, ...]] = []
        with DATA_FILE.open("r", encoding="utf-8-sig", newline="") as fh:
            for row in csv.DictReader(fh):
                bin_ = (row.get("bin") or "").strip()
                if not bin_.isdigit():
                    continue
                batch.append(
                    (
                        bin_,
                        (row.get("brand") or "").strip().lower(),
                        (row.get("type") or "").strip().lower(),
                        (row.get("category") or "").strip(),
                        (row.get("country") or "").strip(),
                        (row.get("alpha_2") or "").strip().upper(),
                        (row.get("issuer") or "").strip(),
                    )
                )
                if len(batch) >= 5000:
                    conn.executemany("INSERT OR REPLACE INTO bins VALUES (?,?,?,?,?,?,?)", batch)
                    batch.clear()
        if batch:
            conn.executemany("INSERT OR REPLACE INTO bins VALUES (?,?,?,?,?,?,?)", batch)
        conn.commit()
    finally:
        conn.close()
    tmp.replace(DB_FILE)
    log.info("SQLite creado desde CSV: %s", DB_FILE)


def load_local_dataset() -> None:
    """Abre el SQLite local; si no existe, lo genera desde el CSV descargado."""
    if CURRENCIES_FILE.is_file():
        try:
            with CURRENCIES_FILE.open("r", encoding="utf-8") as fh:
                currencies.update(
                    {r["country"]: r["currency"] for r in json.load(fh) if r.get("currency")}
                )
        except (ValueError, KeyError, OSError) as e:
            log.warning("No se pudo cargar currencies.json: %s", e)

    global db
    if not DB_FILE.is_file():
        build_sqlite_from_csv()
    db = sqlite3.connect(DB_FILE, check_same_thread=False)
    db.row_factory = sqlite3.Row
    count = db.execute("SELECT COUNT(*) FROM bins").fetchone()[0]
    log.info("Dataset local (SQLite) listo: %s BINs", count)


def lookup_local(bin_: str) -> dict | None:
    """Consulta por BIN de 6 dígitos usando el índice primario de SQLite."""
    if db is None:
        return None
    row = db.execute(
        "SELECT brand, type, category, country, alpha_2, issuer FROM bins WHERE bin = ?",
        (bin_[:6],),
    ).fetchone()
    if row is None:
        return None
    category = row["category"] or ""
    alpha2 = row["alpha_2"] or ""
    return {
        "scheme": row["brand"] or "",
        "type": row["type"] or "",
        "brand": category,
        "prepaid": "PREPAID" in category.upper(),
        "country": {
            "name": row["country"] or "",
            "alpha2": alpha2,
            "currency": currencies.get(alpha2),
        },
        "bank": {
            "name": row["issuer"] or "",
            "url": "",
            "phone": "",
        },
    }


def format_result(bin_: str, j: dict) -> str:
    """Misma data de siempre, con el formato visual del bot de KIDSHARK."""
    banco = ((j.get("bank") or {}).get("name") or "DESCONOCIDO").upper()
    red = (j.get("scheme") or "DESCONOCIDA").replace("-", " ").title()

    tipo_raw = (j.get("type") or "").lower()
    tipo = TYPES.get(tipo_raw, tipo_raw.capitalize() if tipo_raw else "No especificado")
    if j.get("prepaid") and tipo != "Prepago":
        tipo += " · Prepago"

    categoria = (j.get("brand") or "NO ESPECIFICADA").upper()
    pais_obj = j.get("country") or {}
    pais = (pais_obj.get("name") or "DESCONOCIDO").upper()
    moneda = (pais_obj.get("currency") or "").upper()
    pais_texto = f"{pais} ({moneda})" if moneda else pais

    esc = html.escape
    return (
        f"{EMOJI_GLOBO} Bin - {esc(bin_)}\n"
        f"{EMOJI_PUNTO} Banco: {esc(banco)}\n"
        f"{EMOJI_PUNTO} Red: {esc(red)}\n"
        f"{EMOJI_PUNTO} Tipo: {esc(tipo)}\n"
        f"{EMOJI_PUNTO} Categoría: {esc(categoria)}\n"
        f"{EMOJI_PUNTO} País: {esc(pais_texto)}"
    )


async def lookup_bin(bin_: str) -> tuple[str, dict | None]:
    """Solo dataset local: nunca consulta APIs en línea."""
    local = lookup_local(bin_)
    if local:
        return "ok", local
    return "notfound", None


async def handle_bin(update: Update, raw: str) -> None:
    user_id = update.effective_user.id
    digits = re.sub(r"[\s\-]", "", raw)

    if not digits.isdigit():
        await update.message.reply_text("Envía solo números. Ejemplo: /bin 457173")
        return
    if len(digits) < 6:
        await update.message.reply_text("Necesito al menos 6 dígitos. Ejemplo: /bin 457173")
        return

    # Siempre toma únicamente los primeros 6 dígitos, aunque envíen más.
    digits = digits[:6]

    now = time.time()
    if now - last_call.get(user_id, 0) < USER_COOLDOWN:
        await update.message.reply_text("Espera unos segundos antes de la siguiente consulta.")
        return
    last_call[user_id] = now

    status, data = await lookup_bin(digits)
    if status == "ok":
        await reply_html_safe(update, format_result(digits, data))
    else:
        await update.message.reply_text("⚠️ BIN no encontrado en la base de datos.")


async def cmd_start(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    await reply_html_safe(
        update,
        f"{EMOJI_ESTRELLA} <b>CHK KIDSHARK — BIN Lookup</b>\n\n"
        "Consulta al instante el banco emisor, esquema, tipo de tarjeta y país "
        "de cualquier BIN.\n\n"
        f"{EMOJI_DIAMANTE} <b>Uso rápido:</b> <code>/bin 457173</code>\n"
        "También puedes enviar directamente los 6 dígitos.\n\n"
        f"{EMOJI_GLOBO} <b>Datos obtenidos:</b>\n"
        "Red • Banco • Nivel (Débito/Crédito) • País y Moneda\n\n"
        "Dev: @Sharkkx1",
    )


async def cmd_bin(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if not context.args:
        await update.message.reply_text("Uso: /bin 457173")
        return
    await handle_bin(update, "".join(context.args))


async def on_text(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    await handle_bin(update, update.message.text.strip())


def build_app(token: str) -> Application:
    """Timeouts moderados: cortan respuestas colgadas sin romper el arranque."""
    app = (
        Application.builder()
        .token(token)
        .connect_timeout(10)
        .read_timeout(15)
        .write_timeout(10)
        .pool_timeout(5)
        .get_updates_connect_timeout(15)
        .get_updates_read_timeout(30)
        .build()
    )
    app.add_handler(CommandHandler("start", cmd_start))
    app.add_handler(CommandHandler("bin", cmd_bin))
    app.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, on_text))
    return app


def main() -> None:
    token = os.environ.get("BOT_TOKEN")
    if not token:
        raise SystemExit("Falta la variable de entorno BOT_TOKEN")
    load_local_dataset()
    start_keepalive_server()
    while True:
        app = build_app(token)
        # Python 3.14 ya no crea un event loop implícito; PTB 21.x lo necesita.
        asyncio.set_event_loop(asyncio.new_event_loop())
        try:
            log.info("Bot en marcha")
            app.run_polling()
            break
        except (TimedOut, NetworkError) as e:
            log.warning("Telegram no respondió (%s); reintentando en 5 s", e)
            time.sleep(5)


if __name__ == "__main__":
    main()
