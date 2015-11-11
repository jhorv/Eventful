namespace Eventful.EventStore

open Eventful
open EventStore.ClientAPI

open System
open FSharpx
open FSharpx.Collections
open FSharpx.Functional

/// An event sourcing system backed by EventStore.
/// This is the primary entrypoint to the library.
type EventStoreSystem<'TCommandContext, 'TEventContext,'TMetadata, 'TBaseEvent when 'TMetadata : equality and 'TEventContext :> System.IDisposable> 
    ( 
        handlers : EventfulHandlers<'TCommandContext, 'TEventContext,'TMetadata, 'TBaseEvent> (* the system configuration *),
        client : EventStoreClient (* for communicating with the EventStore *),
        serializer: ISerializer (* for reading and writing events *),
        getEventContextFromMetadata : PersistedEvent<'TMetadata> -> 'TEventContext (* calculates the event context from an event *),
        getSnapshot (* function which takes a stream id and mapping from state block names to types and asynchronously calculates the current state snapshot *),
        buildWakeupMonitor : (string -> string -> UtcDateTime -> unit) -> IWakeupMonitor (* function to create a wakeup monitor given a function to run a wakeup handler *)
    ) =

    let log = createLogger "Eventful.EventStoreSystem"

    /// The name of the stream to store the position we've run event handlers up until.
    [<Literal>]
    let positionStream = "EventStoreProcessPosition"

    /// The last position we wrote to the `positionStream`.
    /// Note that we may have run handlers for events beyond this position but not yet recorded that fact.
    let mutable lastPositionUpdate = EventPosition.Start

    /// Callbacks that will be executed after all event handlers have been executed for an event
    let mutable onCompleteCallbacks : List<EventPosition * string * int * EventStreamEventData<'TMetadata> -> unit> = List.empty

    /// A timer to regularly write out the current processing position
    let mutable updatePositionTimer : System.Threading.Timer = null

    /// EventStore subscription to get events to send to event handlers
    let mutable subscription : EventStoreAllCatchUpSubscription = null

    /// Record which event we've processed up until.
    /// NOTE: Because we always process events in sequence this could be replaced with a simple EventPosition variable.
    let completeTracker = new LastCompleteItemAgent<EventPosition>()

    /// Async to store the latest processing position if it's changed since last time.
    let updatePositionLock = obj()
    let updatePosition _ = async {
        try
            return!
                // TODO: This lock will only be held while the async computation is generated, not while it is executing.
                //       Find out what it's protecting and fix it so it correctly protects it or remove it if it's not required.
                lock updatePositionLock 
                    (fun () -> 
                        async {
                            let! lastComplete = completeTracker.LastComplete()
                            log.Debug <| lazy ( sprintf "Updating position %A" lastComplete )
                            match lastComplete with
                            | Some position when position <> lastPositionUpdate ->
                                do! ProcessingTracker.setPosition client positionStream position
                                lastPositionUpdate <- position
                            | _ -> () })
        with | e ->
            log.ErrorWithException <| lazy("failure updating position", e)}

    /// A cache for events read from EventStore
    let inMemoryCache = new System.Runtime.Caching.MemoryCache("EventfulEvents")

    /// Log the start of an operation
    let logStart (correlationId : Guid option) name extraTemplate (extraVariables : obj[]) =
        let contextId = Guid.NewGuid()
        let startMessageTemplate = sprintf "Start %s: %s {@CorrelationId} {@ContextId}" name extraTemplate 
        let startMessageVariables = Array.append extraVariables [|correlationId;contextId|]
        log.RichDebug startMessageTemplate startMessageVariables
        let sw = startStopwatch()
        {
            ContextStartData.ContextId = contextId
            CorrelationId = correlationId
            Stopwatch = sw
            Name = name
            ExtraTemplate = extraTemplate
            ExtraVariables = extraVariables
        }

    /// Log the end of an operation
    let logEnd (startData : ContextStartData) = 
        let elapsed = startData.Stopwatch.ElapsedMilliseconds 
        let completeMessageTemplate = sprintf "Complete %s: {@CorrelationId} {@ContextId} {Elapsed:000} ms" startData.Name
        let completeMessageVariables : obj[] = [|startData.CorrelationId;startData.ContextId;elapsed|]
        log.RichDebug completeMessageTemplate completeMessageVariables

    /// Log an exception
    let logException (startData : ContextStartData) (ex) = 
        let elapsed = startData.Stopwatch.ElapsedMilliseconds 
        let messageTemplate = sprintf "Exception in %s: {@Exception} %s {@CorrelationId} {@ContextId} {Elapsed:000} ms" startData.Name startData.ExtraTemplate
        let messageVariables : obj[] = 
            seq {
                yield (ex :> obj)
                yield! startData.ExtraVariables |> Seq.ofArray
                yield startData.CorrelationId :> obj
                yield startData.ContextId :> obj
                yield elapsed :> obj
            }
            |> Array.ofSeq
        log.RichError messageTemplate messageVariables
        
    /// Convert an event stream program into an async computation that executes the program
    let interpreter startContext program = 
        EventStreamInterpreter.interpret 
            client 
            inMemoryCache 
            serializer 
            handlers.EventStoreTypeToClassMap 
            handlers.ClassToEventStoreTypeMap
            getSnapshot 
            startContext
            program

    /// Run the registered command handler for a command
    let runCommand context cmd = async {
        let correlationId = handlers.GetCommandCorrelationId context
        let startContext = logStart correlationId "command handler" "{@Command}" [|cmd|]
        let program = EventfulHandlers.getCommandProgram context cmd handlers
        let! result = 
            interpreter startContext program

        logEnd startContext
        return result
    }

    /// Run all the registered event handlers for an event
    let runHandlerForEvent buildContext (persistedEvent : PersistedEvent<'TMetadata>) program =
        let correlationId = handlers.GetEventCorrelationId persistedEvent.Metadata
        async {
            let startContext = logStart correlationId "event handler" "{@EventType} {@StreamId} {@EventNumber}" [|persistedEvent.EventType;persistedEvent.StreamId;persistedEvent.EventNumber|]
            try
                use context = buildContext persistedEvent
                let! program = program context
                return! interpreter startContext program
            with | e ->
                logException startContext e
        }

    /// Run all the registered multi-command event handlers for an event
    let runMultiCommandHandlerForEvent buildContext (persistedEvent : PersistedEvent<'TMetadata>) program =
        let correlationId = handlers.GetEventCorrelationId persistedEvent.Metadata
        async {
            let startContext = logStart correlationId "multi command event handler" "{@EventType} {@StreamId} {@EventNumber}" [|persistedEvent.EventType;persistedEvent.StreamId;persistedEvent.EventNumber|]
            try
                use context = buildContext persistedEvent
                do! MultiCommandInterpreter.interpret (program context) (flip runCommand)
            with | e ->
                logException startContext e
        }

    /// Run all registered normal and multi-command event handlers for an event
    let runEventHandlers (handlers : EventfulHandlers<'TCommandContext, 'TEventContext,'TMetadata, 'TBaseEvent>) (persistedEvent : PersistedEvent<'TMetadata>) =
        async {
            let regularEventHandlers = 
                handlers
                |> EventfulHandlers.getHandlerPrograms persistedEvent
                |> List.map (runHandlerForEvent getEventContextFromMetadata persistedEvent)

            let multiCommandEventHandlers =
                handlers
                |> EventfulHandlers.getMultiCommandEventHandlers persistedEvent
                |> List.map (runMultiCommandHandlerForEvent getEventContextFromMetadata persistedEvent)

            do! 
                List.append regularEventHandlers multiCommandEventHandlers
                |> Async.Parallel
                |> Async.Ignore
        }

    /// Run the specified wakeup handler
    let runWakeupHandler streamId aggregateType time =
        let correlationId = Some <| Guid.NewGuid()
        let startContext = logStart correlationId "multi wakeup handler" "{@StreamId} {@AggregateType} {@Time}" [|streamId;aggregateType;time|]

        let config = handlers.AggregateTypes.TryFind aggregateType
        match config with
        | Some config -> 
            match config.Wakeup with
            | Some (EventfulWakeupHandler (_, handler)) ->
                handler streamId time
                |> interpreter startContext
                |> Async.RunSynchronously
            | None ->
                ()
        | None ->
            logException startContext (sprintf "Found wakeup for AggregateType %A but could not find any configuration" aggregateType)

    /// The wakeup monitor which will invoke `runWakeupHandler` as required
    let wakeupMonitor = buildWakeupMonitor runWakeupHandler

    /// Get the name of the stream used to record the current event handler processing position
    member x.PositionStream = positionStream

    /// Add a callback to be called after all event handlers have been invoked for each event
    member x.AddOnCompleteEvent callback = 
        onCompleteCallbacks <- callback::onCompleteCallbacks

    /// Convert the given event stream program to an async computation
    member x.RunStreamProgram correlationId name program = 
        async {
            let startContext = logStart correlationId name String.Empty Array.empty 
            let! result = interpreter startContext program
            logEnd startContext 
            return result
        }

    /// Start the system.
    /// This will initiate the processing of event handlers and wakeup handlers.
    member x.Start () =  async {
        try
            do! ProcessingTracker.ensureTrackingStreamMetadata client positionStream
            let! currentEventStorePosition = ProcessingTracker.readPosition client positionStream
            let! nullablePosition = 
                match currentEventStorePosition with
                | x when x = EventPosition.Start ->
                    log.Debug <| lazy("No event position found. Starting from current head.")
                    async {
                        let! nextPosition = client.getNextPosition ()
                        return Nullable(nextPosition) }
                | _ -> 
                    async { return Nullable(currentEventStorePosition |> EventPosition.toEventStorePosition) }

            let timeBetweenPositionSaves = TimeSpan.FromSeconds(5.0)
            updatePositionTimer <- new System.Threading.Timer((updatePosition >> Async.RunSynchronously), null, TimeSpan.Zero, timeBetweenPositionSaves)
            subscription <- client.subscribe (nullablePosition |> Nullable.toOption) x.EventAppeared (fun () -> log.Debug <| lazy("Live"))
            wakeupMonitor.Start() 
        with | e ->
            log.ErrorWithException <| lazy("Exception starting EventStoreSystem",e)
            raise ( new System.Exception("See inner exception",e)) // cannot use reraise in an async block
        }

    /// Stop the system.
    /// This will halt the processing of event handlers and wakeup handlers.
    member x.Stop () = 
        subscription.Stop()
        wakeupMonitor.Stop()
        if updatePositionTimer <> null then
            updatePositionTimer.Dispose()

    /// Process the event handlers for the given event.
    /// NOTE: After the system has been started this will be called automatically for each event that appears in the $all stream.
    member x.EventAppeared eventId (event : ResolvedEvent) : Async<unit> =
        match handlers.EventStoreTypeToClassMap.ContainsKey event.Event.EventType with
        | true ->
            let eventType = handlers.EventStoreTypeToClassMap.Item event.Event.EventType
            async {
                let position = { Commit = event.OriginalPosition.Value.CommitPosition; Prepare = event.OriginalPosition.Value.PreparePosition }
                completeTracker.Start position

                try
                    let evt = serializer.DeserializeObj (event.Event.Data) eventType

                    let metadata = (serializer.DeserializeObj (event.Event.Metadata) typeof<'TMetadata>) :?> 'TMetadata
                    let correlationId = handlers.GetEventCorrelationId metadata
                    log.RichDebug "Running Handlers for: {@EventType} {@StreamId} {@EventNumber} {@CorrelationId}" [|event.Event.EventType; event.OriginalEvent.EventStreamId; event.OriginalEvent.EventNumber;correlationId|]
                    let eventData = { Body = evt; EventType = event.Event.EventType; Metadata = metadata }

                    let eventStreamEvent = {
                        PersistedEvent.StreamId = event.Event.EventStreamId
                        EventNumber = event.Event.EventNumber
                        EventId = eventId
                        Body = evt
                        Metadata = metadata
                        EventType = event.Event.EventType
                    }

                    do! runEventHandlers handlers eventStreamEvent

                    for callback in onCompleteCallbacks do
                        callback (position, event.Event.EventStreamId, event.Event.EventNumber, eventData)
                with | e ->
                    log.RichError "Exception thrown while running event handlers {@Exception} {@StreamId} {@EventNumber}}" [|e;event.Event.EventStreamId; event.Event.EventNumber|]

                completeTracker.Complete position
            }
        | false -> 
            async {
                let position = event.OriginalPosition.Value |> EventPosition.ofEventStorePosition
                completeTracker.Start position
                completeTracker.Complete position
            }

    /// Run the registered command handler for the given command.
    member x.RunCommand (context:'TCommandContext) (cmd : obj) = runCommand context cmd

    /// Get the mapping from event name to type
    member x.EventStoreTypeToClassMap = handlers.EventStoreTypeToClassMap

    /// Get the mapping from event type to name
    member x.ClassToEventStoreTypeMap = handlers.ClassToEventStoreTypeMap

    /// Get the system configuration
    member x.Handlers = handlers

    interface IDisposable with
        member x.Dispose () = x.Stop()
