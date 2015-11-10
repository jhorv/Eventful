namespace Eventful

open System
open FSharpx
open FSharpx.Choice
open FSharpx.Option
open FSharpx.Collections
open FSharp.Control

open Eventful.EventStream
open Eventful.MultiCommand
open Eventful.Validation

/// The result from a successful command execution
type CommandSuccess<'TBaseEvent, 'TMetadata> = {
    /// New events that were written as a result of the command
    Events : ((* stream name *) string * 'TBaseEvent * 'TMetadata) list
    /// The new EventPosition after the write
    Position : EventPosition option
}
with 
    static member Empty : CommandSuccess<'TBaseEvent, 'TMetadata> = {
        Events = List.empty
        Position = None
    }

/// Indicates success or failure of a command execution
type CommandResult<'TBaseEvent,'TMetadata> = Choice<CommandSuccess<'TBaseEvent,'TMetadata>,NonEmptyList<CommandFailure>> 

type IRegistrationVisitor<'T,'U> =
    abstract member Visit<'TCmd> : 'T -> 'U

type IRegistrationVisitable =
    abstract member Receive<'T,'U> : 'T -> IRegistrationVisitor<'T,'U> -> 'U

type EventResult = unit

type metadataBuilder<'TMetadata> = string -> 'TMetadata

type IStateChangeHandler<'TMetadata, 'TBaseEvent> =
    /// Get a list of all state builders for the state change handler including also the extra ones passed in to the function
    abstract member AddStateBuilder : IStateBlockBuilder<'TMetadata, unit> list -> IStateBlockBuilder<'TMetadata, unit> list 
    /// A handler that can produce extra events to write when the aggregate state has changed
    abstract member Handler : StateSnapshot -> StateSnapshot -> seq<'TBaseEvent * 'TMetadata>

// 'TContext can either be CommandContext or EventContext
type AggregateConfiguration<'TContext, 'TAggregateId, 'TMetadata, 'TBaseEvent> = {
    /// used to make processing idempotent
    GetUniqueId : 'TMetadata -> string option

    /// Configuration for the aggreagate's event stream (e.g. max age for events for keep, etc.)
    StreamMetadata : EventStreamMetadata

    /// Get the event stream name based on the context and aggreate id
    GetStreamName : 'TContext -> 'TAggregateId -> string

    /// The state builder that produces the complete aggregate state
    StateBuilder : IStateBuilder<Map<string,obj>, 'TMetadata, unit>

    /// The list of state change handlers
    StateChangeHandlers : LazyList<IStateChangeHandler<'TMetadata, 'TBaseEvent>>
}

/// Interface for an aggregate command handler.
type ICommandHandler<'TAggregateId,'TCommandContext, 'TMetadata, 'TBaseEvent> =
    /// The type of command that this handler can process
    abstract member CmdType : Type

    /// Get a list of all state builders for the command handler including also the extra ones passed in to the function
    abstract member AddStateBuilder : IStateBlockBuilder<'TMetadata, unit> list -> IStateBlockBuilder<'TMetadata, unit> list

    /// Get the aggregate instance id from the command context and command
    abstract member GetId : 'TCommandContext-> obj -> 'TAggregateId

    /// Get the command handler event stream program
    abstract member Handler : AggregateConfiguration<'TCommandContext, 'TAggregateId, 'TMetadata, 'TBaseEvent> -> 'TCommandContext -> (* command *) obj -> EventStreamProgram<CommandResult<'TBaseEvent,'TMetadata>,'TMetadata>

    abstract member Visitable : IRegistrationVisitable

/// Interface for an aggregate event handler.
type IEventHandler<'TAggregateId,'TMetadata, 'TEventContext,'TBaseEvent when 'TAggregateId : equality> =
    /// Get a list of all state builders for the event handler including also the extra ones passed in to the function
    abstract member AddStateBuilder : IStateBlockBuilder<'TMetadata, unit> list -> IStateBlockBuilder<'TMetadata, unit> list

    /// The type of event that this handler can process
    abstract member EventType : Type

    /// Get the event handler event stream program
    abstract member Handler : AggregateConfiguration<'TEventContext, 'TAggregateId, 'TMetadata, 'TBaseEvent> -> 'TEventContext -> PersistedEvent<'TMetadata> -> Async<EventStreamProgram<EventResult,'TMetadata>>

/// Interface for an aggregate event handler. The handler program can execute commands in multiple aggregates.
type IMultiCommandEventHandler<'TMetadata, 'TEventContext,'TCommandContext, 'TBaseEvent> = 
    /// The type of event that this handler can process
    abstract member EventType : Type

    /// Get the event handler multi command program
    abstract member Handler : 'TEventContext -> PersistedEvent<'TMetadata> -> MultiCommandProgram<unit,'TCommandContext,CommandResult<'TBaseEvent,'TMetadata>>

/// Interface for a wakeup handler.
type IWakeupHandler<'TAggregateId,'TCommandContext, 'TMetadata, 'TBaseEvent> =
    /// The state builder that calculates the next wakeup time
    abstract member WakeupFold : WakeupFold<'TMetadata>

    /// Get the wakeup handler event stream program
    abstract member Handler : AggregateConfiguration<'TEventContext, 'TAggregateId, 'TMetadata, 'TBaseEvent> -> (* streamId *) string -> UtcDateTime -> EventStreamProgram<EventResult,'TMetadata>

type AggregateCommandHandlers<'TAggregateId,'TCommandContext,'TMetadata, 'TBaseEvent> = seq<ICommandHandler<'TAggregateId,'TCommandContext,'TMetadata, 'TBaseEvent>>
type AggregateEventHandlers<'TAggregateId,'TMetadata, 'TEventContext,'TBaseEvent  when 'TAggregateId : equality> = seq<IEventHandler<'TAggregateId, 'TMetadata, 'TEventContext,'TBaseEvent >>

type private AggregateHandlerState<'TId,'TCommandContext,'TEventContext,'TMetadata,'TBaseEvent when 'TId : equality> = {
    commandHandlers : list<ICommandHandler<'TId,'TCommandContext,'TMetadata, 'TBaseEvent>> 
    eventHandlers : list<IEventHandler<'TId,'TMetadata, 'TEventContext,'TBaseEvent>>
    stateChangeHandlers : list<IStateChangeHandler<'TMetadata, 'TBaseEvent>>
    multiCommandEventHandlers : list<IMultiCommandEventHandler<'TMetadata, 'TEventContext,'TCommandContext, 'TBaseEvent>>
}
with 
    static member Empty = 
        { commandHandlers = List.empty
          eventHandlers = List.empty
          stateChangeHandlers = List.empty
          multiCommandEventHandlers = List.empty }

/// A container for all of an aggregate's handlers
type AggregateHandlers<'TId,'TCommandContext,'TEventContext,'TMetadata,'TBaseEvent when 'TId : equality> private 
    (
        state : AggregateHandlerState<'TId,'TCommandContext,'TEventContext,'TMetadata,'TBaseEvent> 
    ) =
    member x.CommandHandlers = state.commandHandlers
    member x.EventHandlers = state.eventHandlers
    member x.StateChangeHandlers = state.stateChangeHandlers
    member x.MultiCommandEventHandlers = state.multiCommandEventHandlers
    member x.AddCommandHandler handler = 
        new AggregateHandlers<'TId,'TCommandContext,'TEventContext,'TMetadata,'TBaseEvent>({ state with commandHandlers = handler::state.commandHandlers})
    member x.AddEventHandler handler = 
        new AggregateHandlers<'TId,'TCommandContext,'TEventContext,'TMetadata,'TBaseEvent>({ state with eventHandlers = handler::state.eventHandlers})
    member x.AddStateChangeHandler handler = 
        new AggregateHandlers<'TId,'TCommandContext,'TEventContext,'TMetadata,'TBaseEvent>({ state with stateChangeHandlers = handler::state.stateChangeHandlers})
    member x.AddMultiCommandEventHandler handler = 
        new AggregateHandlers<'TId,'TCommandContext,'TEventContext,'TMetadata,'TBaseEvent>({ state with multiCommandEventHandlers = handler::state.multiCommandEventHandlers})
    member x.Combine (y:AggregateHandlers<_,_,_,_,_>) =
        new AggregateHandlers<_,_,_,_,_>(
            { 
                commandHandlers = List.append state.commandHandlers y.CommandHandlers
                eventHandlers = List.append state.eventHandlers y.EventHandlers
                stateChangeHandlers = List.append state.stateChangeHandlers y.StateChangeHandlers
                multiCommandEventHandlers = List.append state.multiCommandEventHandlers y.MultiCommandEventHandlers
            })

    static member Empty =
        let state = 
            { commandHandlers = List.empty
              eventHandlers = List.empty
              stateChangeHandlers = List.empty
              multiCommandEventHandlers = List.empty } 
        new AggregateHandlers<'TId,'TCommandContext,'TEventContext,'TMetadata,'TBaseEvent>(state)

module AggregateHandlers =
    let addCommandHandler handler (aggregateHandlers : AggregateHandlers<_,_,_,_,_>) =
        aggregateHandlers.AddCommandHandler handler

    let addCommandHandlers handlers (aggregateHandlers : AggregateHandlers<_,_,_,_,_>) =
        handlers
        |> Seq.fold (fun (s:AggregateHandlers<_,_,_,_,_>) h -> s.AddCommandHandler h) aggregateHandlers

    let addEventHandler handler (aggregateHandlers : AggregateHandlers<_,_,_,_,_>) =
        aggregateHandlers.AddEventHandler handler

    let addEventHandlers handlers (aggregateHandlers : AggregateHandlers<_,_,_,_,_>) =
        handlers
        |> Seq.fold (fun (s:AggregateHandlers<_,_,_,_,_>) h -> s.AddEventHandler h) aggregateHandlers

    let addStateChangeHandler handler (aggregateHandlers : AggregateHandlers<_,_,_,_,_>) =
        aggregateHandlers.AddStateChangeHandler handler

    let addMultiCommandEventHandler handler (aggregateHandlers : AggregateHandlers<_,_,_,_,_>) =
        aggregateHandlers.AddMultiCommandEventHandler handler

/// A command-handler (more strongly-typed than ICommandHandler)
type CommandHandler<'TCmd, 'TCommandContext, 'TCommandState, 'TAggregateId, 'TMetadata, 'TBaseEvent> = {
    GetId : 'TCommandContext -> 'TCmd -> 'TAggregateId
    StateBuilder : IStateBuilder<'TCommandState, 'TMetadata, unit>
    Handler : 'TCommandState -> 'TCommandContext -> 'TCmd -> Async<Choice<seq<'TBaseEvent * 'TMetadata>, NonEmptyList<ValidationFailure>>>
}

/// A pair of destination aggregate instance id and a function from handler state to new events to be written
type MultiEventRun<'TAggregateId,'TMetadata,'TState,'TBaseEvent when 'TAggregateId : equality> = ('TAggregateId * ('TState -> seq<'TBaseEvent * 'TMetadata>))


module AggregateActionBuilder =
    open EventStream

    let log = createLogger "Eventful.AggregateActionBuilder"

    /// Construct a new CommandHandler instance with an async result
    let fullHandlerAsync<'TId, 'TState,'TCmd, 'TCommandContext,'TMetadata, 'TBaseEvent,'TKey> getId (stateBuilder : IStateBuilder<_,_,'TKey>) f =
        {
            GetId = getId
            StateBuilder = StateBuilder.withUnitKey stateBuilder
            Handler = f
        } : CommandHandler<'TCmd, 'TCommandContext, 'TState, 'TId, 'TMetadata, 'TBaseEvent> 

    /// Construct a new CommandHandler instance
    let fullHandler<'TId, 'TState,'TCmd,'TCommandContext,'TMetadata, 'TBaseEvent,'TKey> getId (stateBuilder : IStateBuilder<_,_,'TKey>) f =
        {
            GetId = getId
            StateBuilder = StateBuilder.withUnitKey stateBuilder
            Handler = (fun a b c ->  f a b c |> Async.returnM)
        } : CommandHandler<'TCmd, 'TCommandContext, 'TState, 'TId, 'TMetadata, 'TBaseEvent> 

    /// Construct a new CommandHandler instance which ignores state and command context and produces exactly one event
    let simpleHandler<'TAggregateId, 'TState, 'TCmd, 'TCommandContext, 'TMetadata, 'TBaseEvent> getId stateBuilder (f : 'TCmd -> ('TBaseEvent * 'TMetadata)) =
        {
            GetId = getId
            StateBuilder = stateBuilder
            Handler = (fun _ _ -> f >> Seq.singleton >> Success >> Async.returnM)
        } : CommandHandler<'TCmd, 'TCommandContext, 'TState, 'TAggregateId, 'TMetadata, 'TBaseEvent> 

    /// Relax the signature of a CommandHandler.GetId function to accept `obj` as the command type
    let untypedGetId<'TId,'TCmd,'TState, 'TCommandContext, 'TMetadata, 'TValidatedState, 'TBaseEvent> (sb : CommandHandler<'TCmd, 'TCommandContext, 'TState, 'TId, 'TMetadata, 'TBaseEvent>) (context : 'TCommandContext) (cmd:obj) =
        match cmd with
        | :? 'TCmd as cmd ->
            sb.GetId context cmd
        | _ -> failwith <| sprintf "Invalid command %A" (cmd.GetType())

    /// Create a state builder that records all seen unique ids
    let uniqueIdBuilder (getUniqueId : 'TMetadata -> string option) =
        StateBuilder.Empty "$Eventful::UniqueIdBuilder" Set.empty
        |> StateBuilder.allEventsHandler 
            (fun m -> ()) 
            (fun (s,e,m) -> 
                match getUniqueId m with
                | Some uniqueId ->
                    s |> Set.add uniqueId
                | None -> s)
        |> StateBuilder.toInterface

    /// Creates an event stream program that writes the given events to the specified stream.
    /// If the `lastEventNumber` indicates that the stream is a new stream, additionally write the `streamMetadata`.
    let writeEvents stream lastEventNumber streamMetadata events =  eventStream {
        let rawEventToEventStreamEvent (event, metadata) = getEventStreamEvent event metadata
        let! eventStreamEvents = EventStream.mapM rawEventToEventStreamEvent events

        let expectedVersion = 
            match lastEventNumber with
            | -1 -> NewStream
            | x -> AggregateVersion x

        if streamMetadata <> EventStreamMetadata.Default then
            if expectedVersion = NewStream then
                do! writeStreamMetadata stream streamMetadata

        return! writeToStream stream expectedVersion eventStreamEvents
    }

    /// Expand a list of events to also include any events generated by state changes caused by the events.
    /// Events generated from the state changes can themselves cause further state change events.
    /// The events generated from the state change are inserted immediately after the event that triggered the
    /// state change and processing continues starting from the first generated event.
    let addStateChangeEvents (blockBuilders : IStateBlockBuilder<'TMetadata, _> list) snapshot stateChangeHandlers (events : LazyList<'TBaseEvent * _>) = 
        let applyEventToSnapshot (event, metadata) snapshot = 
            AggregateStateBuilder.applyToSnapshot blockBuilders () event (snapshot.LastEventNumber + 1) metadata snapshot

        let runStateChangeHandlers beforeSnapshot afterSnapshot =
            LazyList.map (fun (h:IStateChangeHandler<_, _>) -> h.Handler beforeSnapshot afterSnapshot |> LazyList.ofSeq)
            >> LazyList.concat
        
        let generator (beforeSnapshot, events) = maybe {
            let! (x,xs) = LazyList.tryUncons events
            let afterSnapshot = applyEventToSnapshot x beforeSnapshot
            let remainingEvents = 
                stateChangeHandlers
                |> runStateChangeHandlers beforeSnapshot afterSnapshot

            return 
                (x, (afterSnapshot, LazyList.append remainingEvents xs))
        }
        
        LazyList.unfold generator (snapshot, events)

    /// Determine whether a batch of events contains any event with a unique id that has already been seen before.
    let anyNewUniqueIdsAlreadyExist getUniqueId snapshot events =
        let existingUniqueIds = (uniqueIdBuilder getUniqueId).GetState snapshot.State
        let newUniqueIds = 
            events
            |> Seq.map snd
            |> Seq.map getUniqueId
            |> Seq.collect (Option.toList)
            |> Set.ofSeq

        existingUniqueIds |> Set.intersect newUniqueIds |> Set.isEmpty |> not
        
    /// Create an event stream program that executes the given handler function
    /// which takes some handler-specific state and asynchronously produces
    /// either a sequence of events and metadata or an error.
    /// If the handler successfully produces events then these events will be
    /// written to the stream (assuming none of the events have been produced before
    /// and the expected version from the snapshot is still correct).
    let inline runHandler 
        getUniqueId (* for determining if an event has been produced before *)
        streamMetadata (* stream metadata in case this is a new stream *)
        blockBuilders (* state builders for the full aggregate state, used by the stateChangeHandlers *)
        stateChangeHandlers (* state change handlers for producing events based on state changes *)
        streamId (* the name of the stream the events should be written to *)
        snapshot (* the snapshot of the full aggregate state at the time this handler is executing *)
        (commandStateBuilder : IStateBuilder<'TChildState, 'TMetadata, unit>) (* the state builder for the handler-specific state *)
        f (* the handler *) = 

        eventStream {
            let commandState = commandStateBuilder.GetState snapshot.State

            let! result = runAsync <| f commandState

            return! 
                match result with
                | Choice1Of2 (handlerEvents : seq<'TBaseEvent * 'TMetadata>) -> 
                    eventStream {
                        if (anyNewUniqueIdsAlreadyExist getUniqueId snapshot handlerEvents) then
                            return Choice2Of2 RunFailure<_>.AlreadyProcessed
                        else
                            let events = 
                                handlerEvents
                                |> LazyList.ofSeq
                                |> addStateChangeEvents blockBuilders snapshot stateChangeHandlers
                                |> List.ofSeq

                            let! writeResult = writeEvents streamId snapshot.LastEventNumber streamMetadata events

                            log.Debug <| lazy (sprintf "WriteResult: %A" writeResult)
                            
                            return 
                                match writeResult with
                                | WriteResult.WriteSuccess pos ->
                                    Choice1Of2 {
                                        Events = events |> List.map (fun (a,b) -> (streamId, a, b))
                                        Position = Some pos
                                    }
                                | WriteResult.WrongExpectedVersion -> 
                                    Choice2Of2 RunFailure<_>.WrongExpectedVersion
                                | WriteResult.WriteError ex -> 
                                    Choice2Of2 <| RunFailure<_>.WriteError ex
                                | WriteResult.WriteCancelled -> 
                                    Choice2Of2 RunFailure<_>.WriteCancelled
                    }
                | Choice2Of2 x ->
                    eventStream { 
                        return
                            HandlerError x
                            |> Choice2Of2
                    }
        }

    let mapValidationFailureToCommandFailure (x : RunFailure<_>) : NonEmptyList<CommandFailure> =
        match x with
        | HandlerError x -> 
            (NonEmptyList.map CommandFailure.ofValidationFailure) x
        | RunFailure.WrongExpectedVersion ->
            CommandFailure.CommandError "WrongExpectedVersion"
            |> NonEmptyList.singleton 
        | AlreadyProcessed ->
            CommandFailure.CommandError "AlreadyProcessed"
            |> NonEmptyList.singleton 
        | RunFailure.WriteCancelled ->
            CommandFailure.CommandError "WriteCancelled"
            |> NonEmptyList.singleton 
        | RunFailure.WriteError ex 
        | RunFailure.Exception ex ->
            CommandFailure.CommandException (None, ex)
            |> NonEmptyList.singleton 

    /// Repeatedly re-run `handler` with the current state snapshot until
    /// it no longer returns WrongExpectedVersion (limited to 100 attempts)
    let retryOnWrongVersion streamId stateBuilder handler = 
        let program attempt = eventStream {
            log.RichDebug "Starting attempt {@Attempt}" [|attempt|]
            let! streamState = 
                    stateBuilder
                    |> AggregateStateBuilder.toStreamProgram streamId ()

            return! handler streamState
        }
        EventStream.retryOnWrongVersion program

    /// Create an event stream program that will execute a commandHandler on a command 
    let handleCommand
        (commandHandler:CommandHandler<'TCmd, 'TCommandContext, 'TState, 'TId, 'TMetadata, 'TBaseEvent>) 
        (aggregateConfiguration : AggregateConfiguration<'TCommandContext,_,_,_>) 
        (commandContext : 'TCommandContext) 
        (cmd : obj)
        : EventStreamProgram<CommandResult<'TBaseEvent,'TMetadata>,'TMetadata> =
        let processCommand cmd commandState = async {
            return! commandHandler.Handler commandState commandContext cmd
        }

        match cmd with
        | :? 'TCmd as cmd -> 
            let getId = FSharpx.Choice.protect (commandHandler.GetId commandContext) cmd
            match getId with
            | Choice1Of2 aggregateId ->
                let stream = aggregateConfiguration.GetStreamName commandContext aggregateId

                let f snapshot = 
                   runHandler 
                        aggregateConfiguration.GetUniqueId 
                        aggregateConfiguration.StreamMetadata
                        aggregateConfiguration.StateBuilder.GetBlockBuilders
                        aggregateConfiguration.StateChangeHandlers
                        stream 
                        snapshot
                        commandHandler.StateBuilder 
                        (processCommand cmd) 

                eventStream {
                    do! logMessage LogMessageLevel.Debug "Starting command tries" [||]
                    let! result = retryOnWrongVersion stream aggregateConfiguration.StateBuilder f
                    return
                        result |> Choice.mapSecond mapValidationFailureToCommandFailure
                }
            | Choice2Of2 exn ->
                eventStream {
                    return 
                        (Some "Retrieving aggregate id from command",exn)
                        |> CommandException
                        |> NonEmptyList.singleton 
                        |> Choice2Of2
                }
        | _ -> 
            eventStream {
                return 
                    (sprintf "Invalid command type: %A expected %A" (cmd.GetType()) typeof<'TCmd>)
                    |> CommandError
                    |> NonEmptyList.singleton 
                    |> Choice2Of2
            }
        
    /// Weaken the strongly-typed CommandHandler into an ICommandHandler
    let ToInterface (sb : CommandHandler<'TCmd, 'TCommandContext, 'TState, 'TAggregateId, 'TMetadata,'TBaseEvent>) = 
        let cmdType = typeof<'TCmd>
        if cmdType = typeof<obj> then
            failwith "Command handler registered for type object. You might need to specify a type explicitely."
        else
            { new ICommandHandler<'TAggregateId,'TCommandContext,'TMetadata, 'TBaseEvent> with 
                 member this.GetId context cmd = untypedGetId sb context cmd
                 member this.CmdType = cmdType
                 member this.AddStateBuilder builders = AggregateStateBuilder.combineHandlers sb.StateBuilder.GetBlockBuilders builders
                 member this.Handler (aggregateConfig : AggregateConfiguration<'TCommandContext, 'TAggregateId, 'TMetadata, 'TBaseEvent>) commandContext cmd = 
                    handleCommand sb aggregateConfig commandContext cmd
                 member this.Visitable = {
                    new IRegistrationVisitable with
                        member x.Receive a r = r.Visit<'TCmd>(a)
                 }
            }

    /// Create a new CommandHandler from an existing one and a GetId function which takes only the command as a parameter
    let withCmdId (getId : 'TCmd -> 'TId) (builder : CommandHandler<'TCmd, 'TCommandContext, 'TState, 'TId, 'TMetadata, 'TBaseEvent>) = 
        { builder with GetId = (fun _ cmd -> getId cmd )}

    /// Weaken the strongly-typed CommandHandler into an ICommandHandler
    let buildCmd (sb : CommandHandler<'TCmd, 'TCommandContext, 'TState, 'TId, 'TMetadata, 'TBaseEvent>) : ICommandHandler<'TId,'TCommandContext,'TMetadata, 'TBaseEvent> = 
        ToInterface sb

    /// Create a new IStateChangeHandler from a handler function.
    /// The handler function will only be executed if the state produced by the stateBuilder has changed.
    let buildStateChange (stateBuilder : IStateBuilder<'TState,'TMetadata,unit>) (handler : 'TState -> 'TState -> seq<'TBaseEvent * 'TMetadata>) = {
        new IStateChangeHandler<'TMetadata, 'TBaseEvent> with
            member this.AddStateBuilder builders = AggregateStateBuilder.combineHandlers stateBuilder.GetBlockBuilders builders
            member this.Handler beforeSnapshot afterSnapshot =
                let beforeState = stateBuilder.GetState beforeSnapshot.State
                let afterState = stateBuilder.GetState afterSnapshot.State
                if beforeState <> afterState then
                    handler beforeState afterState
                else
                    Seq.empty
    }

    let getEventInterfaceForLink<'TLinkEvent,'TAggregateId,'TMetadata, 'TCommandContext, 'TEventContext, 'TBaseEvent when 'TAggregateId : equality> (fId : 'TLinkEvent -> 'TAggregateId) (metadataBuilder : metadataBuilder<'TMetadata>) = {
        new IEventHandler<'TAggregateId,'TMetadata, 'TEventContext,'TBaseEvent> with 
             member this.EventType = typeof<'TLinkEvent>
             member this.AddStateBuilder builders = builders
             member this.Handler aggregateConfig eventContext (evt : PersistedEvent<'TMetadata>) = 
                eventStream {
                    let aggregateId = fId (evt.Body :?> 'TLinkEvent)
                    let sourceMessageId = evt.EventId.ToString()
                    let metadata = metadataBuilder sourceMessageId

                    let resultingStream = aggregateConfig.GetStreamName eventContext aggregateId

                    let! result = EventStream.writeLink resultingStream Any evt.StreamId evt.EventNumber metadata

                    return 
                        match result with
                        | WriteResult.WriteSuccess _ -> 
                            log.Debug <| lazy (sprintf "Wrote Link To %A %A %A" DateTime.Now.Ticks evt.StreamId evt.EventNumber)
                            ()
                        | WriteResult.WrongExpectedVersion -> failwith "WrongExpectedVersion writing event. TODO: retry"
                        | WriteResult.WriteError ex -> failwith <| sprintf "WriteError writing event: %A" ex
                        | WriteResult.WriteCancelled -> failwith "WriteCancelled writing event" } 
                |> Async.returnM
    } 

    /// Map an AsyncSeq of event handlers into an AsyncSeq of event stream programs which will execute the handlers.
    let processSequence
        (aggregateConfiguration : AggregateConfiguration<'TEventContext,'TId,'TMetadata, 'TBaseEvent>)
        (stateBuilder : IStateBuilder<'TState,_,_>)  (* state builder for the state required by the handlers *)
        (event: PersistedEvent<'TMetadata>)
        (eventContext : 'TEventContext)  
        (handlers : AsyncSeq<MultiEventRun<'TId,'TMetadata,'TState,'TBaseEvent>>) 
        =
        handlers
        |> AsyncSeq.map (fun (id, handler) ->
            eventStream {
                let resultingStream = aggregateConfiguration.GetStreamName eventContext id
                let f snapshot = 
                    let stateBuilder = stateBuilder |> StateBuilder.withUnitKey
                    runHandler 
                        aggregateConfiguration.GetUniqueId 
                        aggregateConfiguration.StreamMetadata
                        aggregateConfiguration.StateBuilder.GetBlockBuilders
                        aggregateConfiguration.StateChangeHandlers
                        resultingStream 
                        snapshot
                        stateBuilder 
                        (handler >> Choice1Of2 >> Async.returnM) 

                let! result = retryOnWrongVersion resultingStream aggregateConfiguration.StateBuilder f

                return 
                    match result with
                    | Choice1Of2 _ -> ()
                    | Choice2Of2 AlreadyProcessed ->
                        log.Warn <| lazy (sprintf "Got AlreadyProcessed when running event handlers for event #%i on stream %s (%s)" event.EventNumber event.StreamId event.EventType)
                    | Choice2Of2 a ->
                        failwith <| sprintf "Event handler failed: %A" a
            }
        )

    /// Create an IEventHandler from a state builder and handler function
    let getEventInterfaceForOnEvent<'TOnEvent, 'TId, 'TState, 'TMetadata, 'TCommandContext, 'TEventContext,'TBaseEvent,'TKey when 'TId : equality> (stateBuilder: IStateBuilder<_,_,'TKey>) (fId : ('TOnEvent * 'TEventContext) -> AsyncSeq<MultiEventRun<'TId,'TMetadata,'TState,'TBaseEvent>>) = 
        let evtType = typeof<'TOnEvent>
        if evtType = typeof<obj> then
            failwith "Event handler registered for type object. You might need to specify a type explicitely."
        else 
            {
                new IEventHandler<'TId,'TMetadata, 'TEventContext,'TBaseEvent> with 
                    member this.EventType = typeof<'TOnEvent>
                    member this.AddStateBuilder builders = 
                        let stateBuilderBlockBuilders = 
                            stateBuilder
                            |> StateBuilder.withUnitKey
                            |> (fun x -> x.GetBlockBuilders)
                        AggregateStateBuilder.combineHandlers stateBuilderBlockBuilders builders
                    member this.Handler aggregateConfig eventContext evt = 
                        let typedEvent = evt.Body :?> 'TOnEvent

                        fId (typedEvent, eventContext) 
                        |> processSequence aggregateConfig stateBuilder evt eventContext
                        |> AsyncSeq.fold EventStream.combine EventStream.empty
            }

    let linkEvent<'TLinkEvent,'TId,'TCommandContext,'TEventContext,'TMetadata, 'TBaseEvent when 'TId : equality> fId (metadata : metadataBuilder<'TMetadata>) = 
        getEventInterfaceForLink<'TLinkEvent,'TId,'TMetadata,'TCommandContext,'TEventContext, 'TBaseEvent> fId metadata

    /// Create an IEventHandler from a state builder and handler function that may write new events to multiple aggregate instances of the same aggregate type.
    /// The aggregate instances are calculated asynchronously.
    let onEventMultiAsync<'TOnEvent,'TEventState,'TId, 'TMetadata, 'TCommandContext,'TEventContext,'TBaseEvent,'TKey when 'TId : equality> 
        (stateBuilder : IStateBuilder<'TEventState, 'TMetadata, 'TKey>)  
        (fId : ('TOnEvent * 'TEventContext) -> AsyncSeq<MultiEventRun<'TId,'TMetadata,'TEventState,'TBaseEvent>>) = 
        getEventInterfaceForOnEvent<'TOnEvent,'TId,'TEventState, 'TMetadata, 'TCommandContext,'TEventContext,'TBaseEvent,'TKey> stateBuilder fId

    /// Create an IEventHandler from a state builder and handler function that may write new events to multiple aggregate instances of the same aggregate type.
    /// The aggregate instances are calculated synchronously.
    let onEventMulti<'TOnEvent,'TEventState,'TId, 'TMetadata, 'TCommandContext,'TEventContext,'TBaseEvent,'TKey when 'TId : equality> 
        (stateBuilder : IStateBuilder<'TEventState, 'TMetadata, 'TKey>)  
        (fId : ('TOnEvent * 'TEventContext) -> seq<MultiEventRun<'TId,'TMetadata,'TEventState,'TBaseEvent>>) = 
        onEventMultiAsync stateBuilder (fId >> AsyncSeq.ofSeq)

    /// Create an IEventHandler from a state builder and handler function which writes new events to a single aggregate instance
    let onEvent<'TOnEvent,'TEventState,'TId,'TMetadata,'TEventContext,'TBaseEvent, 'TKey when 'TId : equality> 
        (fId : 'TOnEvent -> 'TEventContext -> 'TId) (* get the aggregate instance id to write the new events to *)
        (stateBuilder : IStateBuilder<'TEventState, 'TMetadata, 'TKey>) (* state builder for state required by the handler *)
        (runEvent : 'TEventState -> 'TOnEvent -> 'TEventContext -> seq<'TBaseEvent * 'TMetadata>) (* handler *) = 
        
        let runEvent' = fun evt eventCtx state ->
            runEvent state evt eventCtx

        let handler = 
            (fun (evt, eventCtx) -> 
                let aggId = fId evt eventCtx
                let h = runEvent' evt eventCtx
                (aggId, h) |> Seq.singleton
            )
        onEventMulti stateBuilder handler

    /// Create an IMultiCommandEventHandler where the handler is a multi-command program
    let multiCommandEventHandler (f : 'TOnEvent -> 'TEventContext -> MultiCommandProgram<unit,'TCommandContext,CommandResult<'TBaseEvent,'TMetadata>>) =
        {
            new IMultiCommandEventHandler<'TMetadata, 'TEventContext,'TCommandContext, 'TBaseEvent> with 
                member this.EventType = typeof<'TOnEvent>
                member this.Handler eventContext evt =
                    let typedEvent = evt.Body :?> 'TOnEvent

                    f typedEvent eventContext
        }

/// The primary definition of a single aggregate
type AggregateDefinition<'TAggregateId, 'TCommandContext, 'TEventContext, 'TMetadata,'TBaseEvent when 'TAggregateId : equality> = {
    /// Get a unique id from the event metadata.
    /// When writing a batch of events to the stream, they will only be writen if no unique id in the batch has been seen before.
    GetUniqueId : 'TMetadata -> string option

    /// Get the aggregate instance stream name from the command context and aggregate id
    GetCommandStreamName : 'TCommandContext -> 'TAggregateId -> string

    /// Get the aggregate instance stream name from the event context and aggregate id
    GetEventStreamName : 'TEventContext -> 'TAggregateId -> string

    /// The set of command, event and state change handlers for the aggregate
    Handlers : AggregateHandlers<'TAggregateId, 'TCommandContext, 'TEventContext, 'TMetadata, 'TBaseEvent>

    /// A unique name identifying the aggregate
    AggregateType : string

    /// The wakeup handler for the aggregate
    Wakeup : IWakeupHandler<'TAggregateId,'TCommandContext, 'TMetadata, 'TBaseEvent> option

    /// Configuration options to be applied to aggregate instance streams
    StreamMetadata : EventStreamMetadata

    /// Extra state builders to be added to the set of state to be projected for the aggregate
    ExtraStateBuilders : IStateBlockBuilder<'TMetadata, unit> list
}

module Aggregate = 
    /// Create a new AggregateDefinition with the specified AggregateHandlers
    let aggregateDefinitionFromHandlers
        (aggregateType : string)
        (getUniqueId : 'TMetadata -> string option)
        (getCommandStreamName : 'TCommandContext -> 'TAggregateId -> string)
        (getEventStreamName : 'TEventContext -> 'TAggregateId -> string) 
        handlers =
            {
                AggregateType = aggregateType
                GetUniqueId = getUniqueId
                GetCommandStreamName = getCommandStreamName
                GetEventStreamName = getEventStreamName
                Handlers = handlers
                Wakeup = None
                StreamMetadata = EventStreamMetadata.Default
                ExtraStateBuilders = []
            }

    /// Create a new AggregateDefinition with specified command and event handlers
    let toAggregateDefinition<'TEvents, 'TAggregateId, 'TCommandContext, 'TEventContext, 'TMetadata,'TBaseEvent when 'TAggregateId : equality>
        (aggregateType : string)
        (getUniqueId : 'TMetadata -> string option)
        (getCommandStreamName : 'TCommandContext -> 'TAggregateId -> string)
        (getEventStreamName : 'TEventContext -> 'TAggregateId -> string) 
        (commandHandlers : AggregateCommandHandlers<'TAggregateId,'TCommandContext, 'TMetadata,'TBaseEvent>)
        (eventHandlers : AggregateEventHandlers<'TAggregateId,'TMetadata, 'TEventContext, 'TBaseEvent>)
        = 
            let handlers =
                commandHandlers |> Seq.fold (fun (x:AggregateHandlers<_,_,_,_,_>) h -> x.AddCommandHandler h) AggregateHandlers.Empty

            let handlers =
                eventHandlers |> Seq.fold (fun (x:AggregateHandlers<_,_,_,_,_>) h -> x.AddEventHandler h) handlers

            aggregateDefinitionFromHandlers 
                aggregateType
                getUniqueId
                getCommandStreamName
                getEventStreamName
                handlers

    /// Add extra state builders to an AggregateDefinition.
    /// These state builders will have their state added to the aggregate's full state snapshot
    let withExtraStateBuilders
        (extraStateBuilders : IStateBlockBuilder<'TMetadata, unit> list) 
        (aggregateDefinition : AggregateDefinition<_,_,_,'TMetadata,'TBaseEvent>) =

        { aggregateDefinition with ExtraStateBuilders = extraStateBuilders }

    /// Add a wakeup handler to an AggregateDefinition
    let withWakeup 
        (wakeupFold : WakeupFold<'TMetadata>) 
        (stateBuilder : IStateBuilder<'T,'TMetadata, unit>) 
        (wakeupHandler : UtcDateTime -> 'T ->  seq<'TBaseEvent * 'TMetadata>)
        (aggregateDefinition : AggregateDefinition<_,_,_,'TMetadata,'TBaseEvent>) =

        let handler time t : Async<Choice<seq<'TBaseEvent * 'TMetadata>,_>> =
            wakeupHandler time t 
            |> Choice1Of2
            |> Async.returnM

        let wakeup = {
            new IWakeupHandler<'TAggregateId,'TCommandContext, 'TMetadata, 'TBaseEvent> with
                member x.WakeupFold = wakeupFold
                member x.Handler aggregateConfiguration streamId (time : UtcDateTime) =
                    eventStream {
                        let run streamState = 
                            // ensure that the handler is only run if the state matches the time
                            let nextWakeupTimeFromState = wakeupFold.GetState streamState.State
                            if Some time = nextWakeupTimeFromState then
                                AggregateActionBuilder.runHandler 
                                    aggregateConfiguration.GetUniqueId 
                                    aggregateConfiguration.StreamMetadata 
                                    aggregateConfiguration.StateBuilder.GetBlockBuilders 
                                    aggregateConfiguration.StateChangeHandlers 
                                    streamId 
                                    streamState 
                                    stateBuilder 
                                    (handler time)
                            else
                                // if the time is out of date just return success with no events
                                eventStream {
                                    do! logMessage LogMessageLevel.Debug "Ignoring out of date wakeup time {@NextExpectedWakeupTime} {@WakeupTime}" [|nextWakeupTimeFromState;time|]
                                    return
                                        CommandSuccess<'TBaseEvent,'TMetadata>.Empty
                                        |> Choice1Of2
                                }
                        let! result = AggregateActionBuilder.retryOnWrongVersion streamId aggregateConfiguration.StateBuilder run
                        match result with
                        | Choice2Of2 failure ->
                            failwith <| sprintf "WakeupHandler failed %A" failure
                        | _ -> 
                            ()
                    }
        }
        { aggregateDefinition with Wakeup = Some wakeup }

    /// Add stream metadata to an AggregateDefinition
    let withStreamMetadata 
        (streamMetadata : EventStreamMetadata) 
        (aggregateDefinition : AggregateDefinition<_,_,_,'TMetadata,'TBaseEvent>) =
        { aggregateDefinition with StreamMetadata = streamMetadata }
