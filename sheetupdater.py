import os
import psycopg2
import gspread
from oauth2client.service_account import ServiceAccountCredentials
from datetime import datetime

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
    try:
        b_box = int(row[0]) if row[0] else None
        b_group = row[1] or None
        b_label = row[2] or None
        accession = int(row[3]) if row[3] else None
        source = row[4] or None
        sex = row[5] or None
        collection_event = row[6] or None
        latitude = float(row[7]) if row[7] else None
        longitude = float(row[8]) if row[8] else None
        host = row[9] or None
        date_str = row[10]
        date = datetime.strptime(date_str, "%m/%d/%Y").date() if date_str else None

        cur.execute("""
            INSERT INTO box_contents 
            (b_box, b_group, b_label, accession, source, sex, collection_event, latitude, longitude, host, date)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            ON CONFLICT (accession) DO UPDATE SET
                "b_box" = EXCLUDED."b_box",
                "b_group" = EXCLUDED."b_group",
                "b_label" = EXCLUDED."b_label",
                "source" = EXCLUDED."source",
                "sex" = EXCLUDED."sex",
                "collection_event" = EXCLUDED."collection_event",
                "latitude" = EXCLUDED."latitude",
                "longitude" = EXCLUDED."longitude",
                "host" = EXCLUDED."host",
                "date" = EXCLUDED."date"
            """, (b_box, b_group, b_label, accession, source, sex, collection_event, latitude, longitude, host, date))
    
    except Exception as e:
        print(f"Skipping row due to error: {row} | Error: {e}")
    
conn.commit()
cur.close()
conn.close()