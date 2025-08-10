import os
import psycopg2
import gspread
from oauth2client.service_account import ServiceAccountCredentials

#sheets setup
scope = ["https://spreadsheets.google.com/feeds", "https://www.googleapis.com/auth/drive"]
creds_json = os.environ.get("GOOGLE_SHEETS_CREDS_JSON")
creds_dict = eval(creds_json)
creds = ServiceAccountCredentials.from_json_keyfile_dict(creds_dict, scope)
client = gspread.authorize(creds)

sheet = client.open("MelissodesSpecimensIndex").worksheet("BOX_CONTENTS")
data = sheet.get_all_values()

#postgres setup
conn = psycopg2.connect(
    host=os.environ["PGHOST"],
    port=os.environ["PGPORT"],
    dbname=os.environ["PGDATABASE"],
    user=os.environ["PGUSER"],
    password=os.environ["PGPASSWORD"],
    sslmode="require"
)
cur = conn.cursor()    

#insert/update
for row in data[1:]:
    col1, col2 = row[0], row[1]
    cur.execute("""
                INSERT INTO box_contents (col1, col2)
                VALUES (%s, %s)
                ON CONFLICT (col1) DO UPDATE SET col2 = EXCLUDED.col2
                """, (col1, col2))
    
conn.commit()
cur.close()
conn.close()