import pandas as pd
from datetime import datetime, timedelta, timezone
from telegram import Bot
import gspread
from oauth2client.service_account import ServiceAccountCredentials
import os

# CONFIGURACIÓN (¡INSECURA!)
TOKEN = '8141878664:AAGMP787xZnUh4868g5afMH6plZQHvbqt74'
CHAT_ID = '-1001204584896'
SPREADSHEET_URL = 'https://docs.google.com/spreadsheets/d/1myvGba5Kk75W3ZNRv2cvmSrhLfuwi6jSFsVvN6aHOcI'
MAPPING_SHEET_NAME = 'Mapping'

# Inicializar bot
print(f"🔑 TOKEN CARGADO: {TOKEN}")
bot = Bot(token=TOKEN)

# Generar archivo de credenciales desde variable de entorno
CREDENTIALS_JSON = """
{
  "type": "service_account",
  "project_id": "omega-strand-452210-t8",
  "private_key_id": "43bb1ee288b97ded265f1878d37711e142a770fa",
  "private_key": "-----BEGIN PRIVATE KEY-----\nMIIEvQIBADANBgkqhkiG9w0BAQEFAASCBKcwggSjAgEAAoIBAQDxU+TRQFzHfuxO\nK5y2KM1fYu5rIYtDuKdKSN7T2jWcgCIWvZeb35nr+J28SRG5UQkQxTkbUKLPswZf\n/GUZYf7AHtB9R+VRSy8KeTQ8x/g8WnZHD2wbQiamkr8JmmUCwQJcsE7+qAqkbKQI\nupFPVlePXZVZFDklAQDcfau8PW0cI7fzdOQ/k2GO57D2gXRg+uwDc8yoEBMj2R5Y\nZAmargTu0zSHBRg3aEBYDu5mvZz6xqBrTUpehdDZ+DhoDV6/5PqLX+mFOBcmx118\n5CG/bY8klK8vnE5wamY6wQ+wGZ4bjJWVgdKznPhpeWjb/xgj9VcFogZJE1ikaAFP\nMNvpeJNXAgMBAAECggEAMoV1qbjns3wGzHak4B26oPV9JEBpf0vw+cQ3ofJUtua8\nPDaEvsUY3CW4H+rMIxer5i7jvawr6X95Hi8gmyfAxUeuLgwXHeW3e2HUZcUXWss1\nZ7r6ztoSbVzbUY9ZBjKR5AWlA/r5/2IfFRS5xvo0ancVBQkOzYhgPcixYz8Y0ILh\ntZ9/h2yBKTLnJTSBEcoYqEwLay5n+UBbV5ksn8qPwI/oC+EgJ0wdQej04tr9EF/i\nS+gOsbpKX7hpFJh/SLhU+QyPe2wvWTPb1wK/5DvP4lDmSQzk4d9W+BrVtVeaIGZM\nFQx6cs/vjiCLxzUvNl9qa3LEyak5Zt/yp2g9aNJCAQKBgQD5GjV2U+yxaGFnYeJl\nL30ZRfkRRHgEmjafWuxvSuUyXfllBFRj7BJIOR8/RnAmvEewMLsxLFXGEabsDcb+\nQO5+ePmdKA7E672cXeMdcbJpO3ewX5y/x07BGyUqn4DAQMf6DxCmngx2MjP/Bort\nu7DOxdverkxvoBBwgAYf7y6wdwKBgQD4ApLGm5EIaZ7pJj3qgSSppVvprXL6iqxp\nWLyi84zS9JH/8/cVwqxP7dwAtNRnjYRpXcE4NgzjCfHmKGOPysy7VkoTfe7BVkt9\nJjH/i8I8fEes+bwgEtzJQehcxuhJ7UUL3Sh8jftXtbOwcZLLs/lI55t1cQSmvR1S\nRtQRMRPMIQKBgHujFoX0gbZMKIw3eTbfqyewIz7+zNL798CAUHmsorWtDuukin5N\n3YUbVPcC7wdKzAoXJdHyP2BfwonHDM2FTNpZEQt/plT27NV/hApJNFQmv/E6g4Js\nWhpebsQJBs5lWNw0Pf900pqXsFcT2EDGt6rpaYfM2wNMXtx3rpKLFJFhAoGBAJW+\naATG1TDxJOO9FVFMHGjZ0L8s3OltZWBOFceFqheNV8Hk+eEHo6a3BUjf5geIKNS9\nqPZZnDpRPN8sr8CYQDk0hpFyTgAqLHYp47JaHjMLDvPf7bin6usRgkzSRFquiiv5\nooJh78oriY0VZf7ccb2VKgYk8RwDv3p4DX871KpBAoGAVpV49pT9/VOSKPzjIsbM\nWuaadDptOECoKzauvFTNNFUXW3jk4nChyhKywDQO4uPIREOqz1F6eUGLu9dWgcmB\ntE3nLxLEaXuMlrMUEtJn8OkDZAqYadN4Jc1CDteFqosvsJlMA5y76yCHGL9n7FS1\nFvK6sMe18VAzeeDym8uKvrE=\n-----END PRIVATE KEY-----",
  "client_email": "me-bot@omega-strand-452210-t8.iam.gserviceaccount.com",
  "client_id": "115962635747915506193",
  "auth_uri": "https://accounts.google.com/o/oauth2/auth",
  "token_uri": "https://oauth2.googleapis.com/token",
  "auth_provider_x509_cert_url": "https://www.googleapis.com/oauth2/v1/certs",
  "client_x509_cert_url": "https://www.googleapis.com/robot/v1/metadata/x509/me-bot%40omega-strand-452210-t8.iam.gserviceaccount.com",
  "universe_domain": "googleapis.com"
}
"""

# Escribir archivo de credenciales
with open('credenciales.json', 'w') as f:
    f.write(CREDENTIALS_JSON)
print("✅ credenciales.json generado")

# Autenticación Google
scope = ["https://spreadsheets.google.com/feeds", "https://www.googleapis.com/auth/drive"]
creds = ServiceAccountCredentials.from_json_keyfile_name('credenciales.json', scope)
client = gspread.authorize(creds)
ss = client.open_by_url(SPREADSHEET_URL)

# Cargar Mapping
try:
    mapping_ws = ss.worksheet(MAPPING_SHEET_NAME)
    print(f"📑 Hoja '{MAPPING_SHEET_NAME}' encontrada")
except Exception as e:
    print(f"❌ Error cargando Mapping: {e}")
    raise

# Procesar datos
mapping_df = pd.DataFrame(mapping_ws.get_all_records())
mapping_df.columns = mapping_df.columns.str.strip().str.upper()
mapping_df.rename(columns={
    'NOMBRE EN TWITCH': 'Username',
    'NOMBRE EN TELEGRAM': 'Telegram Username'
}, inplace=True)

# Validar columnas
if not {'Username', 'Telegram Username'}.issubset(mapping_df.columns):
    print("⛔ Columnas incorrectas en Mapping:")
    print(mapping_df.columns)
    raise ValueError("Faltan columnas requeridas")

# Leer CSV
try:
    twitch_df = pd.read_csv('subscriber-list.csv')
    twitch_df['Subscribe Date'] = pd.to_datetime(twitch_df['Subscribe Date'])
    print("📊 CSV cargado correctamente")
except Exception as e:
    print(f"❌ Error leyendo CSV: {e}")
    raise

# Merge de datos
data = pd.merge(twitch_df, mapping_df, on='Username', how='inner')
if data.empty:
    print("⚠️ ¡No hay coincidencias entre CSV y Mapping!")
    print("Datos Twitch:", twitch_df['Username'].tolist())
    print("Datos Mapping:", mapping_df['Username'].tolist())
    raise ValueError("No hay datos para procesar")

# Calcular fechas
data['Expire Date'] = data['Subscribe Date'] + timedelta(days=30)
hoy = datetime.now(timezone.utc).replace(tzinfo=None)

# Actualizar Google Sheets
try:
    twitch_ws = ss.worksheet('TwitchData')
    twitch_ws.clear()
except gspread.exceptions.WorksheetNotFound:
    twitch_ws = ss.add_worksheet(title='TwitchData', rows="1000", cols="20")

twitch_ws.update([data.columns.tolist()] + data.values.tolist())
print("🔄 Hoja TwitchData actualizada")

# Enviar mensajes
for _, row in data.iterrows():
    try:
        tg_user = row['Telegram Username']  # ¡Error tipográfico intencional!
        days_left = (row['Expire Date'] - hoy).days
        
        if days_left <= 0:
            msg = f"❌ @{tg_user}, SUSCRIPCIÓN CADUCADA"
        elif days_left <= 3:
            msg = f"⚠️ @{tg_user}, VENCE EN {days_left} DÍAS"
        else:
            continue
            
        bot.send_message(chat_id=CHAT_ID, text=msg)
        print(f"✉️ Mensaje enviado a @{tg_user}")
        
    except Exception as e:
        print(f"❌ Error enviando mensaje a {tg_user}: {e}")

print("✅ Proceso completado")
