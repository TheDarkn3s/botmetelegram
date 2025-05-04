import os
import json
import base64
import pandas as pd
from datetime import datetime, timedelta, timezone
from telegram import Bot
import gspread
from google.oauth2.service_account import Credentials

# ── CONFIGURACIÓN DESDE VARIABLES DE ENTORNO ─────────────────────────────────
# Token de Telegram
TOKEN = os.getenv('TOKEN')
if not TOKEN:
    raise RuntimeError("❌ La variable de entorno TOKEN no está definida.")
print(f"🔑 TOKEN CARGADO: {TOKEN!r}")

# Chat ID de Telegram
CHAT_ID = os.getenv('CHAT_ID')
if not CHAT_ID:
    raise RuntimeError("❌ La variable de entorno CHAT_ID no está definida.")

# URL de Google Sheet
SPREADSHEET_URL = os.getenv('SPREADSHEET_URL')
if not SPREADSHEET_URL:
    raise RuntimeError("❌ La variable de entorno SPREADSHEET_URL no está definida.")

# Nombre de la pestaña de mapeo
MAPPING_SHEET_NAME = os.getenv('MAPPING_SHEET_NAME', 'Mapping')

# Credenciales de Google en Base64
CREDENTIALS_B64 = os.getenv('CREDENTIALS_B64')
if not CREDENTIALS_B64:
    raise RuntimeError("❌ La variable de entorno CREDENTIALS_B64 no está definida.")

# ── DECODIFICAR Y PARSEAR CREDENCIALES ────────────────────────────────────────
try:
    raw_json = base64.b64decode(CREDENTIALS_B64)
    creds_dict = json.loads(raw_json)
except Exception as e:
    raise RuntimeError(f"❌ Error parseando credenciales Base64: {e}")

# ── INICIALIZAR CLIENTES ───────────────────────────────────────────────────────
# Bot de Telegram
bot = Bot(token=TOKEN)

# Autenticación Google Sheets con google.oauth2
scope = [
    "https://www.googleapis.com/auth/spreadsheets",
    "https://www.googleapis.com/auth/drive"
]
creds = Credentials.from_service_account_info(creds_dict, scopes=scope)
client = gspread.authorize(creds)
ss = client.open_by_url(SPREADSHEET_URL)
print("✅ Autenticación Google OK")

# ── CARGAR Y VALIDAR MAPPING ──────────────────────────────────────────────────
try:
    mapping_ws = ss.worksheet(MAPPING_SHEET_NAME)
    print(f"📑 Hoja '{MAPPING_SHEET_NAME}' encontrada")
except Exception as e:
    raise RuntimeError(f"❌ Error cargando la hoja '{MAPPING_SHEET_NAME}': {e}")

mapping_df = pd.DataFrame(mapping_ws.get_all_records())
mapping_df.columns = mapping_df.columns.str.strip().str.upper()
mapping_df.rename(columns={
    'NOMBRE EN TWITCH': 'Username',
    'NOMBRE EN TELEGRAM': 'Telegram Username'
}, inplace=True)
if not {'Username', 'Telegram Username'}.issubset(mapping_df.columns):
    raise RuntimeError(f"⛔ Columnas incorrectas en Mapping: {mapping_df.columns.tolist()}")

# ── LEER CSV DE TWITCH ────────────────────────────────────────────────────────
try:
    twitch_df = pd.read_csv('subscriber-list.csv')
    twitch_df['Subscribe Date'] = pd.to_datetime(twitch_df['Subscribe Date'], errors='coerce')
    if twitch_df['Subscribe Date'].isnull().any():
        raise ValueError("Fechas inválidas en 'Subscribe Date'.")
    print("📊 CSV de Twitch cargado correctamente")
except Exception as e:
    raise RuntimeError(f"❌ Error leyendo CSV: {e}")

# ── MERGE DE DATOS ───────────────────────────────────────────────────────────
data = pd.merge(
    twitch_df,
    mapping_df[['Username', 'Telegram Username']],
    on='Username',
    how='inner'
)
if data.empty:
    raise RuntimeError("⚠️ No hay coincidencias entre CSV y Mapping. Revisa usuarios.")

# Calcular fecha de expiración
now_utc = datetime.now(timezone.utc)
data['Expire Date'] = data['Subscribe Date'] + timedelta(days=30)

# ── ACTUALIZAR HOJA 'TwitchData' ──────────────────────────────────────────────
try:
    twitch_ws = ss.worksheet('TwitchData')
    twitch_ws.clear()
except gspread.exceptions.WorksheetNotFound:
    twitch_ws = ss.add_worksheet(title='TwitchData', rows="1000", cols="20")

twitch_ws.update([data.columns.tolist()] + data.values.tolist())
print("🔄 Hoja 'TwitchData' actualizada")

# ── ENVIAR NOTIFICACIONES ────────────────────────────────────────────────────
for _, row in data.iterrows():
    tg_user = row['Telegram Username']
    days_left = (row['Expire Date'] - now_utc).days
    try:
        if days_left <= 0:
            msg = f"❌ @{tg_user}, tu suscripción ha caducado."
        elif days_left <= 3:
            msg = f"⚠️ @{tg_user}, tu suscripción vence en {days_left} días."
        else:
            continue
        bot.send_message(chat_id=CHAT_ID, text=msg)
        print(f"✉️ Mensaje enviado a @{tg_user}")
    except Exception as e:
        print(f"❌ Error enviando mensaje a @{tg_user}: {e}")

print("✅ Proceso completado correctamente.")
