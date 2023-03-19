import requests
import json
from datetime import datetime, timedelta
import pymongo

client = pymongo.MongoClient("mongodb://localhost:27017/")
db = client["sofascore"]
collection = db["soccer_events_tips"]
collectionHistory = db["soccer_events_sofascore_history"]

# Exclui os documentos que correspondem ao filtro
result = collection.delete_many({})
print(result.deleted_count, "documentos excluídos")

# Define a data atual
currentDate = datetime.now()
# Add um dia da data atual
currentDate += timedelta(days=1)
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
        eventDate = datetime.fromtimestamp(event['startTimestamp']);
        # sofascore duplica os eventos, constando por dois dias seguidos
        if (not collection.find_one({"id": event['id']})) and (eventDate.date() == currentDate.date()):
            # Executa a consulta com agregação (home)
            pipelineHomeData = [
                {
                    "$match": {
                        "homeTeam.id": event['homeTeam']['id']
                    }
                },
                {
                    "$sort": {
                        "startTimestamp": -1,
                    },
                },
                {
                    "$limit": 5
                },
                {
                    "$group": {
                        "_id": "$homeTeam.id",
                        "average_goals": { "$avg": "$homeScore.current" },
                        "average_goals_conceded": { "$avg": "$awayScore.current" },
                        "count_goals": { "$sum": "$homeScore.current" },
                        "count_goals_conceded": { "$sum": "$awayScore.current" },
                        "count": {"$sum": 1}
                    }
                }
            ]

            resultListAggHome = list(collectionHistory.aggregate(pipelineHomeData))

            jsonTip = {}
            jsonTip['event'] = event

            for doc in resultListAggHome:
                jsonTip['homeData'] = doc

            # Executa a consulta com agregação (home)
            pipelineAwayData = [
                {
                    "$match": {
                        "awayTeam.id": event['awayTeam']['id']
                    }
                },
                {
                    "$sort": {
                        "startTimestamp": -1,
                    },
                },
                {
                    "$limit": 5
                },
                {
                    "$group": {
                        "_id": "$awayTeam.id",
                        "average_goals": { "$avg": "$awayScore.current" },
                        "average_goals_conceded": { "$avg": "$homeScore.current" },
                        "count_goals": { "$sum": "$awayScore.current" },
                        "count_goals_conceded": { "$sum": "$homeScore.current" },
                        "count": {"$sum": 1}
                    }
                }
            ]

            resultListAggAway = list(collectionHistory.aggregate(pipelineAwayData))

            for doc in resultListAggAway:
                jsonTip['awayData'] = doc
            
            collection.insert_one(jsonTip)
else:
    print('Error:', response.status_code)

pipelineResult = [
    {
        "$sort": {
            "homeData.average_goals": -1,
            "awayData.average_goals": -1,
        },
    },
    {
        "$project": {
            "_id": 0,
            "event.tournament.name": 1,
            "event.tournament.category.name": 1,
            "event.homeTeam.name": 1,
            "event.awayTeam.name": 1,
            "event.startTimestamp": 1,
            "homeData": 1,
            "awayData": 1,
        },
    }
]

resultList = list(collection.aggregate(pipelineResult))

for doc in resultList:
    doc['startDate'] = datetime.fromtimestamp(doc['event']['startTimestamp'])
    strHomeData = ''
    strAwayData = ''

    if 'homeData' in doc:
        strHomeData = '\n*Médias Mandante* | Gols marcados (C): ' + str(doc['homeData']['average_goals']) + ' - Gols sofridos (C): ' + str(doc['homeData']['average_goals_conceded'])
    if 'awayData' in doc:
        strAwayData = '\n*Médias Visitante* | Gols marcados (F): ' + str(doc['awayData']['average_goals']) + ' - Gols sofridos (F): ' + str(doc['awayData']['average_goals_conceded'])

    print('\n', '*' + str(doc['event']['tournament']['name']), '-', str(doc['event']['tournament']['category']['name']) + '*', '|', doc['startDate'], '-', '*' + str(doc['event']['homeTeam']['name']) + '*', 'x', '*' + str(doc['event']['awayTeam']['name']) + '*', strHomeData, strAwayData)


print('terminou! Qtd total de eventos processados no dia: ', len(data['events']))