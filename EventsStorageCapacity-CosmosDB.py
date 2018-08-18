import pydocumentdb.documents as documents
import pydocumentdb.document_client as document_client
import pydocumentdb.errors as errors
import json, pymongo, datetime, sys, bson, random

url = 'https://sampleCosmosDB.documents.azure.com:443/'
key = 'bb6kLVqsg8W3ejyRiYAvufGndrujbvYSY1pxE8V00cVl0lOazwwGKaEORXu8o8iEDTbokRje11qJFv5Mi4Lioa==' # primary key

mongoUri = "mongodb://sampleCosmosDB:bb6kLVqsg8W3ejyRiYAvufGndrujbvYSY1pxE8V00cVl0lOazwwGKaEORXu8o8iEDTbokRje11qJFv5Mi4Lioa==@sampleCosmosDB.documents.azure.com:10255/?ssl=true&replicaSet=globaldb"

databaseName = 'EventTestStorage'
collectionName = 'Event_Records'

def main():
    connection_policy = documents.ConnectionPolicy()
    connection_policy.EnableEndpointDiscovery = True

    #Reading json document - Event Data
    with open('eventDocument.json') as eventDoc:
        eventRecord = json.load(eventDoc)
    
    #print(eventRecord)

    LogEvents(eventRecord)

def LogEvents(eventRecord):

    #Initialize pymongo client
    try:
        pyMongoclient = pymongo.MongoClient(mongoUri)
        pyMongodb = pyMongoclient[databaseName]

        pyMongoCollection = pyMongodb[collectionName]
        #pyMongoCollection.drop_indexes()
        
        

        #Insert a document
        #eventId = pyMongoCollection.insert_one(eventRecord)
        #print(eventId.inserted_id)

        #Get Total Count
        print(pyMongoCollection.count())

        #Log 1M Events
        #InsertRecords(pyMongoCollection)

        print("Inserted 1M Records!")

    except Exception as bwe:
        print(bwe)

#def CheckSingleRecordSize():
    # doc1 = pyMongoCollection.find_one({'event.SourceIP': '192.111.222.90'})
    # print(str(doc1))

    # print(str(len(bson.BSON.encode(doc1))))

def InsertRecords(pyMongoCollection):
    print("Each batch has 1000 records.")
    for batchCount in range(100):
        allRecords = GenerateSampleData()
        pyMongoCollection.insert_many(allRecords)
        print("Inserted Batch Count : " + str(batchCount + 1))

def GenerateSampleData():
    allEvents = []

    try:
        for eventCount in range(1000):
            with open('eventDocument.json') as eventDoc:
                eventRecord = json.load(eventDoc)

            eventRecord['InsertedRecordTime'] = str(datetime.datetime.now())
            eventRecord['previousHmac'] = str(eventRecord['previousHmac'] + str(random.randint(1,700)))
            eventRecord['currentHmac'] = str(eventRecord['currentHmac'] + str(random.randint(1,700)))
            eventRecord['event']['SourceIP'] = str(eventRecord['event']['SourceIP'] + str(random.randint(1,700)))
            eventRecord['event']['SourceType'] = str(random.randint(1,3))
            eventRecord['event']['Category'] = str(random.randint(1, 4))
            allEvents.append(eventRecord)


    except Exception as eventRecordExc:
        print(eventRecordExc)

    return allEvents

main()




