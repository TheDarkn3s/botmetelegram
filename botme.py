import pandas as pd
from datetime import datetime, timedelta, timezone
from telegram import Bot
import gspread
from oauth2client.service_account import ServiceAccountCredentials
import os, json

# CONFIGURACIÓN (mejor usar variables de entorno para seguridad)
TOKEN = os.getenv('TOKEN')
CHAT_ID = os.getenv('CHAT_ID')
SPREADSHEET_URL = os.getenv('SPREADSHEET_URL')
MAPPING_SHEET_NAME = os.getenv('MAPPING_SHEET_NAME', 'Mapping')
CREDENTIALS_JSON = os.getenv('CREDENTIALS_JSON')  # JSON completo en variable

# Inicializar bot
print(f"🔑 TOKEN CARGADO: {TOKEN!r}")
bot = Bot(token=TOKEN)

# Autenticación Google usando JSON desde variable
try:
    creds_dict = json.loads(CREDENTIALS_JSON)
    scope = [
        "https://spreadsheets.google.com/feeds",
        "https://www.googleapis.com/auth/drive"
    ]
    creds = ServiceAccountCredentials.from_json_keyfile_dict(creds_dict, scope)
    client = gspread.authorize(creds)
    ss = client.open_by_url(SPREADSHEET_URL)
    print("✅ Autenticación Google OK")
except Exception as e:
    print(f"❌ Error autenticación Google: {e}")
    raise

# Cargar Mapping
try:
    mapping_ws = ss.worksheet(MAPPING_SHEET_NAME)
    print(f"📑 Hoja '{MAPPING_SHEET_NAME}' encontrada")
except Exception as e:
    print(f"❌ Error cargando Mapping: {e}")
    raise

mapping_df = pd.DataFrame(mapping_ws.get_all_records())
mapping_df.columns = mapping_df.columns.str.strip().str.upper()
mapping_df.rename(columns={
    'NOMBRE EN TWITCH': 'Username',
    'NOMBRE EN TELEGRAM': 'Telegram Username'
}, inplace=True)
if not {'Username', 'Telegram Username'}.issubset(mapping_df.columns):
    print("⛔ Columnas incorrectas en Mapping:", mapping_df.columns.tolist())
    raise ValueError("Faltan columnas requeridas")

# Leer CSV
twitch_df = pd.read_csv('subscriber-list.csv')
twitch_df['Subscribe Date'] = pd.to_datetime(twitch_df['Subscribe Date'])
print("📊 CSV cargado correctamente")

# Merge datos
data = pd.merge(twitch_df, mapping_df, on='Username', how='inner')
if data.empty:
    print("⚠️ ¡No hay coincidencias entre CSV y Mapping!")
    raise ValueError("No hay datos para procesar")

# Calcular fechas
hoy = datetime.now(timezone.utc).replace(tzinfo=None)
data['Expire Date'] = data['Subscribe Date'] + timedelta(days=30)

# Actualizar TwitchData
try:
    twitch_ws = ss.worksheet('TwitchData')
    twitch_ws.clear()
except gspread.exceptions.WorksheetNotFound:
    twitch_ws = ss.add_worksheet(title='TwitchData', rows="1000", cols="20")
twitch_ws.update([data.columns.tolist()] + data.values.tolist())
print("🔄 Hoja TwitchData actualizada")

# Enviar mensajes
for _, row in data.iterrows():
    tg_user = row['Telegram Username']
    days_left = (row['Expire Date'] - hoy).days
    try:
        if days_left <= 0:
            msg = f"❌ @{tg_user}, SUSCRIPCIÓN CADUCADA"
        elif days_left <= 3:
            msg = f"⚠️ @{tg_user}, VENCE EN {days_left} DÍAS"
        else:
            continue
        bot.send_message(chat_id=CHAT_ID, text=msg)
        print(f"✉️ Mensaje enviado a @{tg_user}")
    except Exception as e:
        print(f"❌ Error enviando mensaje a @{tg_user}: {e}")

print("✅ Proceso completado")
