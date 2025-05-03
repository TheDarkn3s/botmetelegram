import pandas as pd
from datetime import datetime, timedelta
from telegram import Bot
import gspread
from oauth2client.service_account import ServiceAccountCredentials

# CONFIGURACIÓN
TOKEN = '8141878664:AAGMP787xZnUh4868g5afMH6plZQHvbqt74'
CHAT_ID = '-1001204584896'
SPREADSHEET_URL = 'https://docs.google.com/spreadsheets/d/1myvGba5Kk75W3ZNRv2cvmSrhLfuwi6jSFsVvN6aHOcI'
CREDENTIALS_PATH = 'credenciales.json'
CSV_PATH = 'subscriber-list.csv'
MAPPING_SHEET_NAME = 'Mapping'  # Nombre de la pestaña que contiene NOMBRE EN TELEGRAM y NOMBRE EN TWITCH

# Inicializar bot de Telegram
bot = Bot(token=TOKEN)

# Autenticación Google Sheets
scope = ["https://spreadsheets.google.com/feeds", "https://www.googleapis.com/auth/drive"]
creds = ServiceAccountCredentials.from_json_keyfile_name(CREDENTIALS_PATH, scope)
client = gspread.authorize(creds)
ss = client.open_by_url(SPREADSHEET_URL)

# Intentar cargar la pestaña de mapeo, informar si no existe
try:
    mapping_ws = ss.worksheet(MAPPING_SHEET_NAME)
except gspread.exceptions.WorksheetNotFound:
    hojas = [ws.title for ws in ss.worksheets()]
    raise ValueError(f"Pestaña '{MAPPING_SHEET_NAME}' no encontrada. Hojas disponibles: {hojas}")

# Leer registros de Mapping
mapping_df = pd.DataFrame(mapping_ws.get_all_records())

# Normalizar y renombrar columnas
mapping_df.columns = mapping_df.columns.str.strip().str.upper()
mapping_df.rename(columns={
    'NOMBRE EN TWITCH': 'Username',
    'NOMBRE EN TELEGRAM': 'Telegram Username'
}, inplace=True)

# Verificar que existan las dos columnas necesarias
required = {'Username', 'Telegram Username'}
if not required.issubset(mapping_df.columns):
    raise ValueError(f"Faltan columnas en Mapping. Se requieren: {required}, encontradas: {mapping_df.columns.tolist()}")

# Leer CSV de Twitch con fechas
twitch_df = pd.read_csv(CSV_PATH)
twitch_df['Subscribe Date'] = pd.to_datetime(twitch_df['Subscribe Date'])

# Merge seguro
data = pd.merge(
    twitch_df,
    mapping_df[['Username', 'Telegram Username']],
    on='Username',
    how='inner'
)
data['Expire Date'] = data['Subscribe Date'] + timedelta(days=30)

# (Opcional) Actualizar pestaña TwitchData
try:
    twitch_ws = ss.worksheet('TwitchData')
    twitch_ws.clear()
except gspread.exceptions.WorksheetNotFound:
    twitch_ws = ss.add_worksheet(title='TwitchData', rows="1000", cols="20")
# Subir datos
twitch_ws.update([data.columns.tolist()] + data.values.tolist())

# Envío de notificaciones
hoy = datetime.utcnow()
for _, row in data.iterrows():
    tg = row['Telegram Username']
    days_left = (row['Expire Date'] - hoy).days
    if days_left <= 0:
        msg = f"❌ {tg}, tu suscripción Prime ha caducado. Renuévala para no perder privilegios."
    elif days_left <= 3:
        msg = f"⚠️ {tg}, tu suscripción Prime vence en {days_left} días. No lo olvides."
    else:
        continue
    bot.send_message(chat_id=CHAT_ID, text=msg)

print("Proceso completado correctamente.")