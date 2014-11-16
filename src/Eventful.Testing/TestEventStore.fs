﻿namespace Eventful.Testing

open FSharpx.Collections
open Eventful
open System

type WakeupRecord<'TAggregateType> = {
    Time : DateTime
    Stream: string
    Type : 'TAggregateType
}

type TestEventStore<'TMetadata, 'TAggregateType when 'TMetadata : equality and 'TAggregateType : comparison> = {
    Position : EventPosition
    Events : Map<string,Vector<EventPosition * EventStreamEvent<'TMetadata>>>
    AllEventsStream : Queue<string * int * EventStreamEvent<'TMetadata>>
    AggregateStateSnapShots : Map<(string * 'TAggregateType), Map<string,obj>>
    WakeupQueue : IPriorityQueue<WakeupRecord<'TAggregateType>>
}

[<CompilationRepresentation(CompilationRepresentationFlags.ModuleSuffix)>]
module TestEventStore =
    let nextPosition (position : EventPosition) = 
        {
            Commit = position.Commit + 1L
            Prepare = position.Commit + 1L
        }

    let empty<'TMetadata, 'TAggregateType when 'TMetadata : equality and 'TAggregateType : comparison> : TestEventStore<'TMetadata, 'TAggregateType> = { 
        Position = EventPosition.Start
        Events = Map.empty
        AllEventsStream = Queue.empty 
        AggregateStateSnapShots = Map.empty
        WakeupQueue = PriorityQueue.empty false }

    let addEvent stream (streamEvent: EventStreamEvent<'TMetadata>) (store : TestEventStore<'TMetadata, 'TAggregateType>) =
        let streamEvents = 
            match store.Events |> Map.tryFind stream with
            | Some events -> events
            | None -> Vector.empty

        let eventPosition = nextPosition store.Position
        let eventNumber = streamEvents.Length
        let streamEvents' = streamEvents |> Vector.conj (eventPosition, streamEvent)
        { store with 
            Events = store.Events |> Map.add stream streamEvents'; 
            Position = eventPosition 
            AllEventsStream = store.AllEventsStream |> Queue.conj (stream, eventNumber, streamEvent)}

    let runHandlerForEvent (context: 'TEventContext) interpreter (eventStream, eventNumber, evt) testEventStore (EventfulEventHandler (t, evtHandler)) =
        let program = evtHandler context eventStream eventNumber evt |> Async.RunSynchronously
        interpreter program testEventStore
        |> fst

    let runEventHandlers (context: 'TEventContext) interpreter (handlers : EventfulHandlers<'TCommandContext, 'TEventContext, 'TMetadata,'TBaseEvent, 'TAggregateType>) (testEventStore : TestEventStore<'TMetadata, 'TAggregateType>) (eventStream, eventNumber, eventStreamEvent) =
        match eventStreamEvent with
        | Event { Body = evt; EventType = eventType; Metadata = metadata } ->
            let handlers = 
                handlers.EventHandlers
                |> Map.tryFind (evt.GetType().Name)
                |> function
                | Some handlers -> handlers
                | None -> []

            handlers |> Seq.fold (runHandlerForEvent context interpreter (eventStream, eventNumber, { Body = evt; EventType = eventType; Metadata = metadata })) testEventStore
        | _ -> testEventStore

    let updateStateSnapShot 
        (streamId, streamNumber, evt : EventStreamEvent<'TMetadata>) 
        (testEventStore : TestEventStore<'TMetadata, 'TAggregateType>) =
        match evt with
        | Event { Body = body; EventType = eventType; Metadata = metadata } ->
            testEventStore
        | EventLink _ ->
            // todo work out what to do here
            testEventStore

    let rec processPendingEvents (context: 'TEventContext) interpreter (handlers : EventfulHandlers<'TCommandContext, 'TEventContext, 'TMetadata,'TBaseEvent,'TAggregateType>) (testEventStore : TestEventStore<'TMetadata, 'TAggregateType>) =
        match testEventStore.AllEventsStream with
        | Queue.Nil -> testEventStore
        | Queue.Cons (x, xs) ->
            let next = 
                runEventHandlers context interpreter handlers { testEventStore with AllEventsStream = xs } x
                |> updateStateSnapShot x
            processPendingEvents context interpreter handlers next

    let runCommand interpreter cmd handler testEventStore =
        let program = handler cmd 
        interpreter program testEventStore