import requests
import json
from datetime import datetime, timedelta
import pymongo

client = pymongo.MongoClient("mongodb://localhost:27017/")
db = client["sofascore"]
collection = db["soccer_events_sofascore_history"]

# Exclui os documentos que correspondem ao filtro
# result = collection.delete_many({})
# print(result.deleted_count, "documentos excluídos")

daysRange = 5

# Define a data atual
currentDate = datetime.now()

# Itera sobre as datas dos últimos xx dias
for i in range(daysRange):
    # Subtrai um dia da data atual
    currentDate = currentDate - timedelta(days=1)
    strDate = currentDate.strftime("%Y-%m-%d")
    # Imprime a data no formato "yyyy-mm-dd"
    print('Processando: ' + strDate)

    url = 'https://api.sofascore.com/api/v1/sport/football/scheduled-events/' + strDate

    headers = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64)',
        'Accept-Language': 'en-US,en;q=0.5',
        'Content-Type': 'application/json'
    }

    response = requests.get(url, headers=headers)

    if response.status_code == 200:
        data = json.loads(response.content)
        for event in data['events']:
            # sofascore duplica os eventos, constando por dois dias seguidos
            if not collection.find_one({"id": event['id']}):
                collection.insert_one(event)
    else:
        print('Error:', response.status_code)

print('terminou! Qtd total de dias processados: ', daysRange)
