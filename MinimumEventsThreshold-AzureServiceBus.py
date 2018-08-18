from azure.servicebus import ServiceBusService, Message, Queue
import sys, asyncio, json, datetime

class Event:

    def __init__(self):
        self.SourceType = 1
        self.SourceName = "SUP"
        self.Category = 2
        self.EventDateTime = str(datetime.datetime.now())
        self.SourceIP = "192.111.222.110"
        self.EventDetails = """In computer science, a tail call is a subroutine call performed as the final action of a procedure. If a tail call might lead to the same subroutine being called again later in the call chain, the subroutine is said to be tail-recursive, which is a special case of recursion. Tail recursion (or tail-end recursion) is particularly useful, and often easy to handle in implementations.
                            Tail calls can be implemented without adding a new stack frame to the call stack. Most of the frame of the current procedure is no longer needed, and can be replaced by the frame of the tail call, modified as appropriate (similar to overlay for processes, but for function calls). The program can then jump to the called subroutine. Producing such code instead of a standard call sequence is called tail call elimination. Tail call elimination allows procedure calls in tail position to be implemented as efficiently as goto statements, thus allowing efficient structured programming. In the words of Guy L. Steele.Peek wil."""
        self.EventDetailSize = 1024
        self.Signature = "Y89IZY0BwutpsCoOjgLR7CooraKleeaPofg"
        
#Initiating service bus connection
bus_service = ServiceBusService(
    service_namespace='SampleGenTestBus',
    shared_access_key_name='SampleSharedAccessKey',
    shared_access_key_value='Y89IZY0BwutpsCoOjgLR7/iUyWXrGtr+HKYPg3VX212v5=')  

#Count of events to report progress.
countOfEvents = 0

#Datetime - Timestamp
currentStamp = datetime.datetime

#QueueName
currentQueue = "nextgentestqueue"

#Entry point to the script
def main():
    try:    
        #Sample Event to be sent to the Queue.
        event = Message(json.dumps(Event().__dict__))

        #Size in bytes, so we'll need to convert it to GB by dividing it by a billion.
        filledQueueSize = (bus_service.get_queue(currentQueue).size_in_bytes)/1000000000

        ClearUpASBQueue()

        print(str(currentStamp.now().time().replace(microsecond=0)) + ": Minimum ASB Queue Size Threshold To Be Achieved - 11.3 GB (8,640,000 events)")           
        print(str(currentStamp.now().time().replace(microsecond=0)) + ": Currently Used ASB Queue Size(GB) - " + str(format(filledQueueSize, '.2f')))
        print(str(currentStamp.now().time().replace(microsecond=0)) + ": Adding more events in the Queue to reach ASB Queue Size for minimal survival threshold!")
        
        LogEventToFillUpASBToMinThreshold(event)

    except Exception as exception:
        print(exception)


#Log a batch of events to the service bus Queue asynchronously.
#Currently set the batch limit to 200.
#Log events up to 11.3 GB.
def LogEventToFillUpASBToMinThreshold(event):
    global countOfEvents
    allEvents = BuildEventsInBatch(event)
    asyncEventLoop = asyncio.get_event_loop()
    allEventTasks = []

    for batchCount in range(45000):
        #start = time.time()
        msgTask = asyncio.ensure_future(LogEventsToQueue(allEvents))
        allEventTasks.append(msgTask)
        #end = time.time()
    
    print(str(currentStamp.now().time().replace(microsecond=0)) + ": Logging Events..Please Wait.. ")
    asyncEventLoop.run_until_complete(asyncio.wait(allEventTasks))
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
        print(str(currentStamp.now().time().replace(microsecond=0)) + ": Events Logged : " + str(countOfEvents)
                + ", Progress% - " +  ReportProgress())

    except Exception as exception:
        exceptionMessage = str(exception)
        if "Request Entity Too Large" in exceptionMessage:
            print(str(currentStamp.now().time().replace(microsecond=0)) + ": Please check the event size and try again!")
            print(exception)
            sys.exit()
        else:
            print(exceptionMessage)

#Responsible for reporting progress of the process.
def ReportProgress():
    currentEventsCount = bus_service.get_queue(currentQueue).message_count
    eventsMaxLimit = 8640000
    if currentEventsCount > eventsMaxLimit:
        print(str(currentStamp.now().time().replace(microsecond=0)) + ": Minimum events survival ASB Queue Size threshold Reached!")
        sys.exit()
    
    else:
        return str(format((currentEventsCount / eventsMaxLimit) * 100, '.3f'))

#Clear up ASB Queue by reading all the events.
def ClearUpASBQueue():
    totalEventsCount = bus_service.get_queue(currentQueue).message_count
    print(str(currentStamp.now().time().replace(microsecond=0)) + ": Clearing ASB Queue...Please wait...")
    for eventCount in range(totalEventsCount):
        bus_service.read_delete_queue_message(currentQueue)
        print(str(currentStamp.now().time().replace(microsecond=0)) + ": Remaining Events : " + str(totalEventsCount - eventCount) + 
        ", Progress% : " + str(format((eventCount / totalEventsCount) * 100, '.3f')))
    
    print(str(currentStamp.now().time().replace(microsecond=0)) + ": All Events From The Queue Deleted! Queue is Empty Now!")


main()