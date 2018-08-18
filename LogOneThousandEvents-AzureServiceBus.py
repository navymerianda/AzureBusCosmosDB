from azure.servicebus import ServiceBusService, Message, Queue
import sys, asyncio, json, datetime

class SampleEvent:

    def __init__(self):
        self.FieldTypeID = 1
        self.FieldType = "SUP"
        self.FieldCategory = 2
        self.FieldTime = str(datetime.datetime.now())
        self.FieldIP = "192.111.222.110"
        self.FieldDetails = """In computer science, a tail call is a subroutine call performed as the final action of a procedure. If a tail call might lead to the same subroutine being called again later in the call chain, the subroutine is said to be tail-recursive, which is a special case of recursion. Tail recursion (or tail-end recursion) is particularly useful, and often easy to handle in implementations.
                            Tail calls can be implemented without adding a new stack frame to the call stack. Most of the frame of the current procedure is no longer needed, and can be replaced by the frame of the tail call, modified as appropriate (similar to overlay for processes, but for function calls). The program can then jump to the called subroutine. Producing such code instead of a standard call sequence is called tail call elimination. Tail call elimination allows procedure calls in tail position to be implemented as efficiently as goto statements, thus allowing efficient structured programming. In the words of Guy L. Steele.Peek wil."""
        self.FieldDetailSize = 1024
        self.FieldSignature = "Y89IZY0BwutpsCoOjgLR7CooraKleeaPofg"

#Initiating service bus connection
bus_service = ServiceBusService(
    service_namespace='testBus',
    shared_access_key_name='your_key_name',
    shared_access_key_value='your_key_value')  

#Count of events to report progress.
countOfEvents = 0

#QueueName
currentQueue = "testQueue"

#Datetime - Timestamp
currentStamp = datetime.datetime

#Entry point to the script
def main():
    try:
        #Sample Event to be sent to the Queue.
        event = Message(json.dumps(SampleEvent().__dict__))

        ClearUpASBQueue()

        LogOneThousandEvents(event)

    except Exception as exception:
        print(exception)

#Log 1k events.
def LogOneThousandRecords(event):
    global countOfEvents
    allEvents = []
    for batchList in range(5):
        allEvents.append(BuildEventsInBatch(event))

    asyncEventLoop = asyncio.get_event_loop()
    allEventTasks = []

    for batchCount in range(5):
        #start = time.time()
        eventTask = asyncio.ensure_future(LogEventsToQueue(allEvents[batchCount]))
        allEventTasks.append(eventTask)
        #end = time.time()
    
    print(str(currentStamp.now().time().replace(microsecond=0)) + ": Logging 1k Events..Please Wait.. ")
    asyncEventLoop.run_until_complete(asyncio.wait(allEventTasks))
    print(str(currentStamp.now().time().replace(microsecond=0)) + ": All Events Logged Successfully.. ")
    countOfEvents = 0


#Building a batch of events of up to 200.
def BuildEventsInBatch(event):
    eventsList = []
    for msgCount in range(200):
        eventsList.append(event)

    return eventsList

#Async Logging of events to ASB.
async def LogEventsToQueue(allMessages):
    try:
        global countOfEvents
        countOfEvents += len(allMessages)
        bus_service.send_queue_message_batch(currentQueue, allMessages)
        print(str(currentStamp.now().time().replace(microsecond=0)) + ": Events Logged : " + str(countOfEvents))

    except Exception as exception:
        exceptionMessage = str(exception)
        if "Request Entity Too Large" in exceptionMessage:
            print(str(currentStamp.now().time().replace(microsecond=0)) + ": Please check the event size and try again!")
            print(exception)
            sys.exit()
        else:
            print(exceptionMessage)

#Clear up ASB Queue by reading all the events.
def ClearUpASBQueue():
    totalEventsCount = bus_service.get_queue(currentQueue).message_count
    print(str(currentStamp.now().time().replace(microsecond=0)) + ": Clearing ASB Queue...Please wait..")
    for eventCount in range(totalEventsCount):
        bus_service.read_delete_queue_message(currentQueue)
        print(str(currentStamp.now().time().replace(microsecond=0)) + ": Remaining Events : " + str(totalEventsCount - eventCount) + 
        ", Progress% : " + str(format((eventCount / totalEventsCount) * 100, '.3f')))
    
    print(str(currentStamp.now().time().replace(microsecond=0)) + ": All Events From The Queue Deleted! Queue is Empty Now!")


main()