namespace Eventful

open System
open FSharpx

type ExpectedAggregateVersion =
| Any
| NewStream
| AggregateVersion of int

type RunFailure<'a> =
| HandlerError of 'a
| WrongExpectedVersion
| WriteCancelled
| AlreadyProcessed // idempotency check failed
| WriteError of System.Exception
| Exception of exn

type WriteResult =
| WriteSuccess of EventPosition
| WrongExpectedVersion
| WriteCancelled
| WriteError of System.Exception

type EventStreamEventData<'TMetadata> = {
    Body : obj
    EventType : string
    Metadata : 'TMetadata
}

type EventStreamMetadata = {
    MaxCount : int option
    MaxAge : TimeSpan option
}
with static member Default = { MaxCount = None; MaxAge = None }

type EventStoreTypeToClassMap = FSharpx.Collections.PersistentHashMap<string, Type>
type ClassToEventStoreTypeMap = FSharpx.Collections.PersistentHashMap<Type, string>

type StateSnapshot = {
    LastEventNumber : int
    State : Map<string, obj>
}
with static member Empty = { LastEventNumber = -1; State = Map.empty }

type EventStreamEvent<'TMetadata> = 
| Event of (EventStreamEventData<'TMetadata>)
| EventLink of (string * int * 'TMetadata)

type PersistedEventLink<'TMetadata> = {
    StreamId : string
    EventNumber : int
    EventId : Guid
    LinkedStreamId : string
    LinkedEventNumber : int
    LinkedBody : obj
    LinkedEventType : string
    LinkedMetadata : 'TMetadata
}

type PersistedEvent<'TMetadata> = {
    StreamId : string
    EventNumber : int
    EventId : Guid
    Body : obj
    EventType : string
    Metadata : 'TMetadata
}

type PersistedStreamEntry<'TMetadata> = 
    | PersistedStreamEvent of PersistedEvent<'TMetadata>
    | PersistedStreamLink of PersistedEventLink<'TMetadata>

type LogMessageLevel = 
| Debug
| Info
| Warn
| Error

module EventStream =
    open FSharpx.Operators
    open FSharpx.Collections

    type EventToken = {
        Stream : string
        Number : int
        EventType : string
    }

    /// Statements in a DSL for processsing event streams.
    type EventStreamLanguage<'N,'TMetadata> =
    | ReadFromStream of streamName : string * startEventNumber : int * (EventToken option -> 'N)
    | ReadSnapshot of string * Map<string, Type> * (StateSnapshot -> 'N)
    | GetEventStoreTypeToClassMap of unit * (EventStoreTypeToClassMap -> 'N)
    | GetClassToEventStoreTypeMap of unit * (ClassToEventStoreTypeMap -> 'N)
    | ReadValue of EventToken *  ((obj * 'TMetadata) -> 'N)
    | RunAsync of Async<'N>
    | LogMessage of LogMessageLevel * string * obj[] * 'N
    | WriteToStream of string * ExpectedAggregateVersion * seq<EventStreamEvent<'TMetadata>> * (WriteResult -> 'N)
    | WriteStreamMetadata of string * EventStreamMetadata * 'N
    | NotYetDone of (unit -> 'N)

    type FreeEventStream<'F,'R,'TMetadata> =
    | FreeEventStream of EventStreamLanguage<FreeEventStream<'F,'R,'TMetadata>,'TMetadata>
    | Pure of 'R

    /// A program in a DSL for processing event streams.
    type EventStreamProgram<'A,'TMetadata> = FreeEventStream<obj,'A,'TMetadata>

    /// Transform the output of the continuation in an event stream statement
    let fmap f streamWorker = 
        match streamWorker with
        | ReadFromStream (stream, number, streamRead) -> 
            ReadFromStream (stream, number, (streamRead >> f))
        | ReadSnapshot (typeMap, stream, next) -> 
            ReadSnapshot (typeMap, stream, (next >> f))
        | GetEventStoreTypeToClassMap (eventStoreTypeToClassMap,next) -> 
            GetEventStoreTypeToClassMap (eventStoreTypeToClassMap, next >> f)
        | GetClassToEventStoreTypeMap (classToEventStoreTypeMap,next) -> 
            GetClassToEventStoreTypeMap (classToEventStoreTypeMap, next >> f)
        | ReadValue (eventToken, readValue) -> 
            ReadValue (eventToken, readValue >> f)
        | WriteToStream (stream, expectedVersion, events, next) -> 
            WriteToStream (stream, expectedVersion, events, (next >> f))
        | LogMessage (logLevel, messageTemplate, data, next) -> 
            LogMessage (logLevel, messageTemplate, data, f next)
        | WriteStreamMetadata (stream, streamMetadata, next) ->
            WriteStreamMetadata (stream, streamMetadata, f next)
        | NotYetDone (delay) ->
            NotYetDone (fun () -> f (delay()))
        | RunAsync asyncBlock -> 
            RunAsync <| Async.map f asyncBlock

    let empty = Pure ()

    /// Lift an event stream statement into an event stream program
    // liftF :: (Functor f) => f r -> Free f r -- haskell signature
    let liftF command = FreeEventStream (fmap Pure command)

    /// Create an event stream program that reads the first event at or after `startEventNumber` from the `streamName` stream.
    /// The program outputs None if there are no events at or after `startEventNumber` on the stream.
    /// To read the next event, read from (previousEventToken.Number + 1) *not* (previousStartEventNumber + 1)
    let readFromStream streamName startEventNumber = 
        ReadFromStream (streamName, startEventNumber, id) |> liftF

    /// Create an event stream program that outputs the current state snapshot for the `streamName` stream.
    /// `typeMap` maps event names to event types.
    let readSnapshot streamName typeMap = 
        ReadSnapshot (streamName, typeMap, id) |> liftF

    /// Create an event stream program that outputs the event name to event type mapping.
    let getEventStoreTypeToClassMap unit =
        GetEventStoreTypeToClassMap ((), id) |> liftF

    /// Create an event stream program that outputs the event type to event name mapping.
    let getClassToEventStoreTypeMap unit =
        GetClassToEventStoreTypeMap ((), id) |> liftF

    /// Create an event stream program that outputs the event data referenced by the given `eventToken`.
    let readValue eventToken = 
        ReadValue(eventToken, id) |> liftF

    /// Create an event stream program that writes the given `events` to the `streamName` stream
    /// and outputs the outcome of the write.  The stream's current position must match the position
    /// specified by `number`.
    let writeToStream streamName number events = 
        WriteToStream(streamName, number, events, id) |> liftF

    /// Create an event stream program that writes the given stream metadata to the stream.
    let writeStreamMetadata streamName streamMetadata = 
        WriteStreamMetadata(streamName, streamMetadata, ()) |> liftF

    /// Create an event stream program that logs the given message to the logger.
    let logMessage logLevel messageTemplate data = 
        LogMessage (logLevel, messageTemplate, data, ()) |> liftF

    /// Create an event stream program that outputs the result of an asynchronous computation.
    let runAsync (a : Async<'a>) : FreeEventStream<'f2,'a,'m> =  
        RunAsync(a) |> liftF

    /// Compose two event stream programs, where the second program is determined based on the output of the first.
    let rec bind f v =
        match v with
        | FreeEventStream x -> FreeEventStream (fmap (bind f) x)
        | Pure r -> f r

    // Return the final value wrapped in the Free type.
    let result value = Pure value

    // The whileLoop operator.
    // This is boilerplate in terms of "result" and "bind".
    let rec whileLoop pred body =
        if pred() then body |> bind (fun _ -> whileLoop pred body)
        else result ()

    // The delay operator.
    let delay (func : unit -> FreeEventStream<'a,'b,'TMetadata>) : FreeEventStream<'a,'b,'TMetadata> = 
        let notYetDone = NotYetDone (fun () -> ()) |> liftF
        bind func notYetDone

    // The sequential composition operator.
    // This is boilerplate in terms of "result" and "bind".
    let combine expr1 expr2 =
        expr1 |> bind (fun () -> expr2)

    // The forLoop operator.
    // This is boilerplate in terms of "catch", "result", and "bind".
    let forLoop (collection:seq<_>) func =
        let ie = collection.GetEnumerator()
        (whileLoop (fun () -> ie.MoveNext())
            (delay (fun () -> let value = ie.Current in func value)))

    type EventStreamBuilder() =
        member x.Zero() = Pure ()
        member x.Return(r:'R) : FreeEventStream<'F,'R,'TMetadata> = Pure r
        member x.ReturnFrom(r:FreeEventStream<'F,'R,'TMetadata>) : FreeEventStream<'F,'R,'TMetadata> = r
        member x.Bind (inp : FreeEventStream<'F,'R,'TMetadata>, body : ('R -> FreeEventStream<'F,'U,'TMetadata>)) : FreeEventStream<'F,'U,'TMetadata>  = bind body inp
        member x.Combine(expr1, expr2) = combine expr1 expr2
        member x.For(a, f) = forLoop a f 
        member x.While(func, body) = whileLoop func body
        member x.Delay(func) = delay func

    let eventStream = new EventStreamBuilder()

    let inline returnM x = returnM eventStream x

    let inline (<*>) f m = applyM eventStream eventStream f m

    let inline lift2 f x y = returnM f <*> x <*> y

    let inline sequence s =
        let inline cons a b = lift2 List.cons a b
        List.foldBack cons s (returnM [])

    let inline mapM f x = sequence (List.map f x)

    // Higher level eventstream operations

    /// Create an event stream program that writes an event link to a stream
    let writeLink stream expectedVersion linkStream linkEventNumber metadata =
        let writes : seq<EventStreamEvent<'TMetadata>> = Seq.singleton (EventStreamEvent.EventLink(linkStream, linkEventNumber, metadata))
        writeToStream stream expectedVersion writes

    /// Create an event stream program that converts an event instance into an EventStreamEvent
    let getEventStreamEvent evt metadata = eventStream {
        let! eventTypeMap = getClassToEventStoreTypeMap ()
        let eventType = eventTypeMap.Item (evt.GetType())
        return EventStreamEvent.Event { Body = evt :> obj; EventType = eventType; Metadata = metadata }
    }

    /// Create an event stream program that folds over the specified stream
    let rec foldStream stream (start : int) acc init = eventStream {
        let! item = readFromStream stream start

        return!
            match item with
            | Some x -> 
                eventStream { 
                    let! evt = readValue x
                    let newValue = acc init evt
                    return! foldStream stream (x.Number + 1) acc newValue
                }
            | None -> eventStream { return init } 
    }

    /// Create an event stream program that retries a given sub-program up to 100 times
    /// or until it outputs something other than `Choice2Of2 RunFailure.WrongExpectedVersion`.
    let retryOnWrongVersion f = eventStream {
        let maxTries = 100
        let retry = ref true
        let count = ref 0
        // WriteCancelled should never be used
        let finalResult = ref (Choice2Of2 RunFailure.WriteCancelled)
        while !retry do
            let! result = f !count
            match result with
            | Choice2Of2 RunFailure.WrongExpectedVersion ->
                count := !count + 1
                if !count < maxTries then
                    retry := true
                else
                    retry := false
                    finalResult := (Choice2Of2 RunFailure.WrongExpectedVersion)
            | x -> 
                retry := false
                finalResult := x

        return !finalResult
    }
