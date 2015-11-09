namespace Eventful

open System
open FSharpx
open Eventful

/// A function that takes current state, metadata, and an event, and produces the new state
type HandlerFunction<'TState, 'TMetadata, 'TEvent> = 'TState * 'TEvent * 'TMetadata -> 'TState

type GetAllEventsKey<'TMetadata, 'TKey> = 'TMetadata -> 'TKey
type GetEventKey<'TMetadata, 'TEvent, 'TKey> = 'TEvent -> 'TMetadata -> 'TKey

/// A handler that updates a piece of state according to incoming events
type StateBuilderHandler<'TState, 'TMetadata, 'TKey> = 
    /// A handler that will be informed of any type of event
    | AllEvents of GetAllEventsKey<'TMetadata, 'TKey> * HandlerFunction<'TState, 'TMetadata, obj>
    /// A handler that will only be informed of a specific type of event
    | SingleEvent of Type * GetEventKey<'TMetadata, obj, 'TKey> * HandlerFunction<'TState, 'TMetadata, obj>

/// The initial value for a piece of state and the set of handlers that update the state according to incoming events.
type EventFold<'TState, 'TMetadata, 'TKey> 
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
module EventFold = 
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
