from azure.servicebus import ServiceBusService, Message, Queue
import sys, datetime

#Initiating service bus connection
bus_service = ServiceBusService(
    service_namespace='SampleTestBus',
    shared_access_key_name='SharedSharedAccessKey',
    shared_access_key_value='Y89IZY0BwutpsCoOjgLR7/iUyWXrGar+HKYPg3244v4=')  

#Datetime - Timestamp
currentStamp = datetime.datetime

#Entry point to the script
def main():
    try:
         createNewQueue = "sampleTestQueue"
        
         CreateASBQueue(createNewQueue)

         DeleteASBQueue(createNewQueue)

    except Exception as exception:
        print(exception)


#CreateQueue(Up to 10)
def CreateASBQueue(queueName):
    for queueCount in range(10):
        print(str(currentStamp.now().time().replace(microsecond=0)) + ": Creating Queue - " + queueName + str(queueCount++))
        bus_service.create_queue(queueName + str(queueCount + 1))
        print(str(currentStamp.now().time().replace(microsecond=0)) + ": Created Queue - " + queueName + str(queueCount++))

#DeleteQueue(Up to 10)
def DeleteASBQueue(queueName):
    for queueCount in range(10):
        print(str(currentStamp.now().time().replace(microsecond=0)) + ": Deleting Queue - " + queueName + str(queueCount++))
        bus_service.delete_queue(queueName + str(queueCount + 1))
        print(str(currentStamp.now().time().replace(microsecond=0)) + ": Deleted Queue - " + queueName + str(queueCount++))


main()