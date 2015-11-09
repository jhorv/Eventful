namespace Eventful

open System
open FSharpx
open FSharpx.Functional
open Eventful

/// A function that takes current state, metadata, and an event, and produces the new state
type internal HandlerFunction<'TState, 'TMetadata, 'TEvent> = 'TState * 'TEvent * 'TMetadata -> 'TState

/// Gets a key from the metadata, used to restrict handler functions to run only when the key has a certain value.
type internal GetAllEventsKey<'TMetadata, 'TKey> = 'TMetadata -> 'TKey

/// Gets a key from the event and metadata, used to restrict handler functions to run only when the key has a certain value.
type internal GetEventKey<'TMetadata, 'TEvent, 'TKey> = 'TEvent -> 'TMetadata -> 'TKey

/// A handler that updates a piece of state according to incoming events
type StateBuilderHandler<'TState, 'TMetadata, 'TKey> = 
    /// A handler that will be informed of any type of event
    | AllEvents of GetAllEventsKey<'TMetadata, 'TKey> * HandlerFunction<'TState, 'TMetadata, obj>
    /// A handler that will only be informed of a specific type of event
    | SingleEvent of Type * GetEventKey<'TMetadata, obj, 'TKey> * HandlerFunction<'TState, 'TMetadata, obj>

/// The initial value for a piece of state and the set of handlers that update the state according to incoming events.
type internal EventFold<'TState, 'TMetadata, 'TKey> 
    (
        initialState : 'TState, 
        handlers : StateBuilderHandler<'TState, 'TMetadata, 'TKey> list
    ) = 
    static member Empty initialState = new EventFold<'TState, 'TMetadata, 'TKey>(initialState, List.empty)

    member x.InitialState = initialState

    member x.Handlers = handlers

    member x.AddHandler<'T> (h:StateBuilderHandler<'TState, 'TMetadata, 'TKey>) =
        new EventFold<'TState, 'TMetadata, 'TKey>(initialState, h::handlers)

[<CompilationRepresentation(CompilationRepresentationFlags.ModuleSuffix)>]
module internal EventFold = 
    /// Generalize the event type accepted by a handler to `obj`
    let untypedHandler f (state : 'TState, (evt : obj), metadata : 'TMetadata) = 
        match evt with
        | :? 'TEvent as evt ->
            f (state, evt, metadata) 
        | _ -> failwith <| sprintf "Expecting type: %s but got type: %s" typeof<'TEvent>.FullName (evt.GetType().FullName)

    /// Generalize the event type accepted by a get key function to `obj`
    let untypedGetKey f (evt : obj) (metadata : 'TMetadata) = 
        match evt with
        | :? 'TEvent as evt ->
            f evt metadata
        | _ -> failwith <| sprintf "Expecting type: %s but got type: %s" typeof<'TEvent>.FullName (evt.GetType().FullName)

    /// Add a handler for a single event type to a set of handlers
    let handler (getKey : GetEventKey<'TMetadata, 'TEvent, 'TKey>) (f : HandlerFunction<'TState, 'TMetadata, 'TEvent>) (b : EventFold<'TState, 'TMetadata, 'TKey>) =
        b.AddHandler <| SingleEvent (typeof<'TEvent>, untypedGetKey getKey, untypedHandler f)

/// A function that transforms current state into new state according to an event and its metadata
type internal StateRunner<'TMetadata, 'TState, 'TEvent> = 'TEvent -> 'TMetadata -> 'TState -> 'TState

/// A generalized state builder for a single piece of state, which stores its state in a `Map<string, obj>`
/// where the key is the name of the state builder.
type IStateBlockBuilder<'TMetadata, 'TKey> = 
    /// The type of the state
    abstract Type : Type

    /// A unique name for this piece of state / state builder
    abstract Name : string
    
    /// The initial value of the state
    abstract InitialState : obj

    /// A function to get the handlers to update the state for a given event type.
    /// These handlers must only update the state corresponding to this state builder's name in the state map.
    // TODO: We should change this definition so that the handlers can only access the appropriate bit of state 
    //       and factor out the code that combines state into a single map.
    abstract GetRunners<'TEvent> : unit -> (GetEventKey<'TMetadata, 'TEvent, 'TKey> * StateRunner<'TMetadata, Map<string,obj>, 'TEvent>) seq

/// A state builder for a piece of state, where the components of the state are
/// calculated as a `Map<string, obj>` by IStateBlockBuilders. The final state is
/// then a function of this map.
/// This is the primary interface for all state builders.
type IStateBuilder<'TState, 'TMetadata, 'TKey> = 
    abstract GetBlockBuilders : IStateBlockBuilder<'TMetadata, 'TKey> list
    abstract GetState : Map<string, obj> -> 'TState

/// A state builder for a single piece of state.
/// To support the IStateBuilder interface, `name` is used as the key for the value
/// to update in the state map.
type StateBuilder<'TState, 'TMetadata, 'TKey>
    private (name: string, eventFold : EventFold<'TState, 'TMetadata, 'TKey>) = 

    let getStateFromMap (stateMap : Map<string,obj>) =
       stateMap 
       |> Map.tryFind name 
       |> Option.map (fun s -> s :?> 'TState)
       |> Option.getOrElse eventFold.InitialState 

    /// A state builder with the given name and initial state but no handlers
    static member Empty name initialState = new StateBuilder<'TState, 'TMetadata, 'TKey>(name, EventFold.Empty initialState)

    /// Get the initial value of the state
    member x.InitialState = eventFold.InitialState

    /// Constructs a new state builder from this state builder with an additional handler
    member x.AddHandler<'T> (h:StateBuilderHandler<'TState, 'TMetadata, 'TKey>) =
        new StateBuilder<'TState, 'TMetadata, 'TKey>(name, eventFold.AddHandler h)

    /// Get the handlers that update the state
    member x.GetRunners<'TEvent> () : (GetEventKey<'TMetadata, 'TEvent, 'TKey> * StateRunner<'TMetadata, 'TState, 'TEvent>) seq = 
        seq {
            for handler in eventFold.Handlers do
               match handler with
               | AllEvents (getKey, handlerFunction) ->
                    let getKey _ metadata = getKey metadata
                    let stateRunner (evt : 'TEvent) metadata state = 
                        handlerFunction (state, evt, metadata)
                    yield (getKey, stateRunner)
               | SingleEvent (eventType, getKey, handlerFunction) ->
                    if eventType = typeof<'TEvent> then
                        let getKey evt metadata = getKey evt metadata
                        let stateRunner (evt : 'TEvent) metadata state = 
                            handlerFunction (state, evt, metadata)
                        yield (getKey, stateRunner)
        }

    // Plumbing to support the general IStateBuilder interface
    interface IStateBlockBuilder<'TMetadata, 'TKey> with
        member x.Name = name
        member x.Type = typeof<'TState>
        member x.InitialState = eventFold.InitialState :> obj
        member x.GetRunners<'TEvent> () =
            x.GetRunners<'TEvent> ()
            |> Seq.map 
                (fun (getKey, handler) ->
                    let mapHandler evt metadata (stateMap : Map<string,obj>) =
                        let state = getStateFromMap stateMap 
                        let state' = handler evt metadata state
                        stateMap |> Map.add name (state' :> obj)

                    (getKey, mapHandler)
                )

    // Support the general IStateBuilder interface
    interface IStateBuilder<'TState, 'TMetadata, 'TKey> with
        member x.GetBlockBuilders = [x :> IStateBlockBuilder<'TMetadata, 'TKey>]
        member x.GetState stateMap = getStateFromMap stateMap

[<CompilationRepresentation(CompilationRepresentationFlags.ModuleSuffix)>]
module StateBuilder =
    /// A state builder which always produces ()
    let nullStateBuilder<'TMetadata, 'TKey when 'TKey : equality> =
        StateBuilder<unit, 'TMetadata, 'TKey>.Empty "$Empty" ()

    /// Change an IStateBlockBuilder's 'TKey to unit (effectively ignoring keys)
    let withUnitKeyBlockBuilder (stateBlockBuilder : IStateBlockBuilder<'TMetadata, 'TKey>) : IStateBlockBuilder<'TMetadata, unit> =
        let getUnitKey _ _ = ()

        {
            new IStateBlockBuilder<'TMetadata, unit> with
                member x.Type = stateBlockBuilder.Type
                member x.Name = stateBlockBuilder.Name
                member x.InitialState = stateBlockBuilder.InitialState
                member x.GetRunners () = 
                    stateBlockBuilder.GetRunners() 
                    |> Seq.map (fun (_, stateRunner) -> (getUnitKey, stateRunner))
        }
        
    /// Change an IStateBuilder's 'TKey to unit (effectively ignoring keys)
    let withUnitKey (stateBuilder : IStateBuilder<'TState, 'TMetadata, 'TKey>) : IStateBuilder<'TState, 'TMetadata, unit> = {
        new IStateBuilder<'TState, 'TMetadata, unit> with
            member x.GetBlockBuilders = 
                stateBuilder.GetBlockBuilders
                |> List.map withUnitKeyBlockBuilder 
            member x.GetState stateMap = stateBuilder.GetState stateMap}

    /// Add a handler for a single event type to a StateBuilder
    let handler (getKey : GetEventKey<'TMetadata, 'TEvent, 'TKey>) (f : HandlerFunction<'TState, 'TMetadata, 'TEvent>) (b : StateBuilder<'TState, 'TMetadata, 'TKey>) =
        SingleEvent (typeof<'TEvent>, EventFold.untypedGetKey getKey, EventFold.untypedHandler f)
        |> b.AddHandler

    /// Add a handler for all event types to a StateBuilder
    let allEventsHandler getKey (f : ('TState * obj * 'TMetadata) -> 'TState) (b : StateBuilder<'TState, 'TMetadata, 'TKey>) =
        AllEvents (getKey, EventFold.untypedHandler f)
        |> b.AddHandler

    // Add a handler for a single event type to a StateBuilder, ignoring keys (aggregate state has no key, it is scoped to the stream)
    let aggregateStateHandler (f : HandlerFunction<'TState, 'TMetadata, 'TEvent>) (b : StateBuilder<'TState, 'TMetadata, unit>) =
        handler (fun _ _ -> ()) f b

    /// Add a handler for all event types to a StateBuilder, ignoring keys (aggregate state has no key, it is scoped to the stream)
    let allAggregateEventsHandler (f : ('TState * obj * 'TMetadata) -> 'TState) (b : StateBuilder<'TState, 'TMetadata, unit>) =
        allEventsHandler (konst ()) f b

    /// Calculate a StateBuilders new state from the current state, event, metadata, and key
    let run (key : 'TKey) (evt : 'TEvent) (metadata : 'TMetadata) (builder: StateBuilder<'TState, 'TMetadata, 'TKey> , currentState : 'TState) =
        let keyHandlers = 
            builder.GetRunners<'TEvent>()
            |> Seq.map (fun (getKey, handler) -> (getKey evt metadata, handler))
            |> Seq.filter (fun (k, _) -> k = key)
            |> Seq.map snd

        let acc state (handler : StateRunner<'TMetadata, 'TState, 'TEvent>) =
            handler evt metadata state

        let state' = keyHandlers |> Seq.fold acc currentState
        (builder, state')

    /// Get keys of all handlers for a given event and metadata
    let getKeys (evt : 'TEvent) (metadata : 'TMetadata) (builder: StateBuilder<'TState, 'TMetadata, 'TKey>) =
        builder.GetRunners<'TEvent>()
        |> Seq.map (fun (getKey, _) -> (getKey evt metadata))
        |> Seq.distinct

    /// Cast a StateBuilder to the general IStateBuilder interface
    let toInterface (builder: StateBuilder<'TState, 'TMetadata, 'TKey>) =
        builder :> IStateBuilder<'TState, 'TMetadata, 'TKey>

    /// A simple StateBuilder that counts the number of events of the specified type it has seen
    let eventTypeCountBuilder (getId : 'TEvent -> 'TMetadata -> 'TId) =
        StateBuilder.Empty (sprintf "%sCount" typeof<'TEvent>.Name) 0
        |> handler getId (fun (s,_,_) -> s + 1)

    let getTypeMapFromBlockBuilders (blockBuilders : IStateBlockBuilder<'TMetadata, 'TKey> list) =
        blockBuilders
        |> List.map(fun x -> x.Name,x.Type)
        |> Map.ofList

    let getBlockBuilders (stateBuilder : IStateBuilder<'TState, 'TMetadata, 'TKey>) =
        stateBuilder.GetBlockBuilders

    let getTypeMapFromStateBuilder (stateBuilder : IStateBuilder<'TState, 'TMetadata, 'TKey>) =
        stateBuilder
        |> getBlockBuilders
        |> getTypeMapFromBlockBuilders

/// A state builder that can calculate state by composing other state builders
type AggregateStateBuilder<'TState, 'TMetadata, 'TKey when 'TKey : equality>
    (
        unitBuilders : IStateBlockBuilder<'TMetadata, 'TKey> list,
        extract : Map<string, obj> -> 'TState
    ) = 

    static let log = createLogger "AggregateStateBuilder"

    let buildersByName =
        unitBuilders
        |> Seq.map (fun builder -> (builder.Name, builder))
        |> Map.ofSeq

    let duplicateNonIdenticalBuilders =
        unitBuilders
        |> Seq.filter (fun builder -> buildersByName |> Map.find builder.Name <> builder)
        |> Seq.map (fun builder -> builder.Name)
        |> Set.ofSeq

    do if (not << Set.isEmpty) duplicateNonIdenticalBuilders then
        log.Warn <| lazy(sprintf "Unit state builders found with identical names but distinct instances, assuming them to be equivalent (%A)" duplicateNonIdenticalBuilders)

    let uniqueUnitBuilders = buildersByName |> Map.toSeq |> Seq.map snd |> List.ofSeq

    static member Empty name initialState = StateBuilder.Empty name initialState

    member x.InitialState = 
        let acc s (b : IStateBlockBuilder<'TMetadata, 'TKey>) =
            s |> Map.add b.Name b.InitialState

        uniqueUnitBuilders 
        |> List.fold acc Map.empty
        |> extract

    interface IStateBuilder<'TState, 'TMetadata, 'TKey> with
        member x.GetBlockBuilders = uniqueUnitBuilders
        member x.GetState unitStates = extract unitStates

[<CompilationRepresentation(CompilationRepresentationFlags.ModuleSuffix)>]
module AggregateStateBuilder =
    /// An AggregateStateBuilder which always produces the same value
    let constant<'TState,'TMetdata,'TKey when 'TKey : equality> value = new AggregateStateBuilder<'TState,'TMetdata,'TKey>([], konst value)

    /// Combine two state builders into a new state builder whose state is a function of the source state builders
    let combine f (b1 : IStateBuilder<'TState1, 'TMetadata, 'TKey>) (b2 : IStateBuilder<'TState2, 'TMetadata, 'TKey>) : IStateBuilder<'TStateCombined, 'TMetadata, 'TKey> =
        let combinedUnitBuilders = 
            Seq.append b1.GetBlockBuilders b2.GetBlockBuilders 
            |> Seq.distinct
            |> List.ofSeq

        let extract unitStates = 
            f (b1.GetState unitStates) (b2.GetState unitStates)

        new AggregateStateBuilder<'TStateCombined, 'TMetadata, 'TKey>(combinedUnitBuilders, extract) :> IStateBuilder<'TStateCombined, 'TMetadata, 'TKey>

    let combineHandlers (h1 : IStateBlockBuilder<'TMetadata, 'TId> list) (h2 : IStateBlockBuilder<'TMetadata, 'TId> list) =
        List.append h1 h2 
        |> Seq.distinct
        |> List.ofSeq

    let ofStateBuilderList (builders : IStateBlockBuilder<'TMetadata, 'TKey> list) =
        new AggregateStateBuilder<Map<string,obj>,'TMetadata, 'TKey>(builders, id)

    /// Calculate the new block states for a set of IStateBlockBuilders from the current state, event, metadata, and key
    let run<'TMetadata, 'TKey, 'TEvent when 'TKey : equality> (unitBuilders : IStateBlockBuilder<'TMetadata, 'TKey> list) key evt metadata currentUnitStates =
        let runBuilder unitStates (builder : IStateBlockBuilder<'TMetadata, 'TKey>) = 
            let keyHandlers = 
                builder.GetRunners<'TEvent>()
                |> Seq.map (fun (getKey, handler) -> (getKey evt metadata, handler))
                |> Seq.filter (fun (k, _) -> k = key)
                |> Seq.map snd

            let acc state (handler : StateRunner<'TMetadata, 'TState, 'TEvent>) =
                handler evt metadata state

            let state' = keyHandlers |> Seq.fold acc unitStates
            state'

        unitBuilders |> List.fold runBuilder currentUnitStates

    let genericRunMethod = 
        let moduleInfo = 
          System.Reflection.Assembly.GetExecutingAssembly().GetTypes()
          |> Seq.find (fun t -> t.FullName = "Eventful.AggregateStateBuilderModule")
        let name = "run"
        moduleInfo.GetMethod(name)

    /// Calculate the new block states for a set of IStateBlockBuilders from the current state, event, metadata, and key
    /// where the event type is not known at compile time
    let dynamicRun (unitBuilders : IStateBlockBuilder<'TMetadata, 'TKey> list) (key : 'TKey) evt (metadata : 'TMetadata) (currentUnitStates:Map<string,obj>) =
        let specializedMethod = genericRunMethod.MakeGenericMethod(typeof<'TMetadata>, typeof<'TKey>, evt.GetType())
        specializedMethod.Invoke(null, [| unitBuilders; key; evt; metadata; currentUnitStates |]) :?> Map<string, obj>

    /// Map the state type produced by an IStateBuilder
    let map (f : 'T1 -> 'T2) (stateBuilder: IStateBuilder<'T1, 'TMetadata, 'TKey>) =
        let extract unitStates = stateBuilder.GetState unitStates |> f
        new AggregateStateBuilder<'T2, 'TMetadata, 'TKey>(stateBuilder.GetBlockBuilders, extract) :> IStateBuilder<'T2, 'TMetadata, 'TKey>

    /// Update a state snapshot with a new event
    let applyToSnapshot blockBuilders key value eventNumber metadata (snapshot : StateSnapshot) = 
        let state' = dynamicRun blockBuilders key value metadata snapshot.State
        { snapshot with LastEventNumber = eventNumber; State = state' }

    /// Create a stream program that calculates the current state snapshot for the given stream
    let toStreamProgram streamName (key : 'TKey) (stateBuilder:IStateBuilder<'TState, 'TMetadata, 'TKey>) = EventStream.eventStream {
        let rec loop (snapshot : StateSnapshot) = EventStream.eventStream {
            let startEventNumber = snapshot.LastEventNumber + 1
            let! token = EventStream.readFromStream streamName startEventNumber
            match token with
            | Some token -> 
                let! (value, metadata : 'TMetadata) = EventStream.readValue token
                return! loop <| applyToSnapshot stateBuilder.GetBlockBuilders key value token.Number metadata snapshot
            | None -> 
                return snapshot }
            
        let typeMap = stateBuilder |> StateBuilder.getTypeMapFromStateBuilder
        let! currentSnapshot = EventStream.readSnapshot streamName typeMap
        return! loop currentSnapshot
    }

    /// Combine two state builders into a single state builder which produces a tuple of the original states.
    let tuple2 b1 b2 =
        combine FSharpx.Functional.Prelude.tuple2 b1 b2

    /// Combine three state builders into a single state builder which produces a tuple of the original states.
    let tuple3 b1 b2 b3 =
        (tuple2 b2 b3)
        |> combine FSharpx.Functional.Prelude.tuple2 b1
        |> map (fun (a,(b,c)) -> (a,b,c))

    /// Combine four state builders into a single state builder which produces a tuple of the original states.
    let tuple4 b1 b2 b3 b4 =
        (tuple3 b2 b3 b4)
        |> combine FSharpx.Functional.Prelude.tuple2 b1
        |> map (fun (a,(b,c,d)) -> (a,b,c,d))

    /// Combine five state builders into a single state builder which produces a tuple of the original states.
    let tuple5 b1 b2 b3 b4 b5 =
        (tuple4 b2 b3 b4 b5)
        |> combine FSharpx.Functional.Prelude.tuple2 b1
        |> map (fun (a,(b,c,d,e)) -> (a,b,c,d,e))

    /// Combine six state builders into a single state builder which produces a tuple of the original states.
    let tuple6 b1 b2 b3 b4 b5 b6 =
        (tuple5 b2 b3 b4 b5 b6)
        |> combine FSharpx.Functional.Prelude.tuple2 b1
        |> map (fun (a,(b,c,d,e,f)) -> (a,b,c,d,e,f))

    /// Combine seven state builders into a single state builder which produces a tuple of the original states.
    let tuple7 b1 b2 b3 b4 b5 b6 b7 =
        (tuple6 b2 b3 b4 b5 b6 b7)
        |> combine FSharpx.Functional.Prelude.tuple2 b1
        |> map (fun (a,(b,c,d,e,f,g)) -> (a,b,c,d,e,f,g))

    /// Combine eight state builders into a single state builder which produces a tuple of the original states.
    let tuple8 b1 b2 b3 b4 b5 b6 b7 b8 =
        (tuple7 b2 b3 b4 b5 b6 b7 b8)
        |> combine FSharpx.Functional.Prelude.tuple2 b1
        |> map (fun (a,(b,c,d,e,f,g,h)) -> (a,b,c,d,e,f,g,h))
