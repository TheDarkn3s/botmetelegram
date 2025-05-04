import os
import json
import pandas as pd
from datetime import datetime, timedelta, timezone
from telegram import Bot
import gspread
from oauth2client.service_account import ServiceAccountCredentials

# CONFIGURACIÓN desde variables de entorno
TOKEN = os.getenv('TOKEN')
if not TOKEN:
    raise RuntimeError("❌ La variable de entorno TOKEN no está definida.")
print(f"🔑 TOKEN CARGADO: {TOKEN!r}")

CHAT_ID = os.getenv('CHAT_ID')
if not CHAT_ID:
    raise RuntimeError("❌ La variable de entorno CHAT_ID no está definida.")

SPREADSHEET_URL = os.getenv('SPREADSHEET_URL')
if not SPREADSHEET_URL:
    raise RuntimeError("❌ La variable de entorno SPREADSHEET_URL no está definida.")

MAPPING_SHEET_NAME = os.getenv('MAPPING_SHEET_NAME', 'Mapping')

# CARGA DE CREDENCIALES GOOGLE desde variable de entorno
CREDENTIALS_JSON = os.getenv('CREDENTIALS_JSON')
if not CREDENTIALS_JSON:
    raise RuntimeError("❌ La variable de entorno CREDENTIALS_JSON no está definida.")
try:
    creds_dict = json.loads(CREDENTIALS_JSON)
except json.JSONDecodeError as e:
    raise RuntimeError(f"❌ JSON de credenciales inválido: {e}")

# Inicializar bot de Telegram
bot = Bot(token=TOKEN)

# Autenticación con Google Sheets
scope = [
    "https://spreadsheets.google.com/feeds",
    "https://www.googleapis.com/auth/drive"
]
creds = ServiceAccountCredentials.from_json_keyfile_dict(creds_dict, scope)
client = gspread.authorize(creds)
ss = client.open_by_url(SPREADSHEET_URL)
print("✅ Autenticación Google OK")

# Cargar hoja de mapeo
try:
    mapping_ws = ss.worksheet(MAPPING_SHEET_NAME)
    print(f"📑 Hoja '{MAPPING_SHEET_NAME}' encontrada")
except Exception as e:
    raise RuntimeError(f"❌ Error cargando la hoja '{MAPPING_SHEET_NAME}': {e}")

# Leer datos de Mapping
tmapping_df = pd.DataFrame(mapping_ws.get_all_records())
mapping_df = tmapping_df.copy()
# Normalizar y renombrar columnas
mapping_df.columns = mapping_df.columns.str.strip().str.upper()
mapping_df.rename(columns={
    'NOMBRE EN TWITCH': 'Username',
    'NOMBRE EN TELEGRAM': 'Telegram Username'
}, inplace=True)
if not {'Username', 'Telegram Username'}.issubset(mapping_df.columns):
    raise RuntimeError(f"⛔ Columnas incorrectas en Mapping: {mapping_df.columns.tolist()}")

# Leer CSV de Twitch
twitch_df = pd.read_csv('subscriber-list.csv')
twitch_df['Subscribe Date'] = pd.to_datetime(twitch_df['Subscribe Date'], errors='coerce')
if twitch_df['Subscribe Date'].isnull().any():
    raise RuntimeError("❌ Hay fechas inválidas en 'Subscribe Date' del CSV")
print("📊 CSV de Twitch cargado correctamente")

# Merge de datos
data = pd.merge(
    twitch_df,
    mapping_df[['Username', 'Telegram Username']],
    on='Username',
    how='inner'
)
if data.empty:
    raise RuntimeError("⚠️ No hay coincidencias entre CSV y Mapping. Revisa los usuarios.")

# Calcular fecha de expiración
now = datetime.now(timezone.utc)
data['Expire Date'] = data['Subscribe Date'] + timedelta(days=30)

# Actualizar hoja TwitchData
try:
    twitch_ws = ss.worksheet('TwitchData')
    twitch_ws.clear()
except gspread.exceptions.WorksheetNotFound:
    twitch_ws = ss.add_worksheet(title='TwitchData', rows="1000", cols="20")
twitch_ws.update([data.columns.tolist()] + data.values.tolist())
print("🔄 Hoja 'TwitchData' actualizada")

# Enviar notificaciones
for _, row in data.iterrows():
    tg_user = row['Telegram Username']
    days_left = (row['Expire Date'] - now).days
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
