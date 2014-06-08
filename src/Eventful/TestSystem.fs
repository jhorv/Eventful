﻿namespace Eventful.Testing

open System
open Eventful
open FSharpx.Collections

type Aggregate (commandTypes : Type list, runCommand : obj -> obj list, getId : obj -> obj, aggregateType) =
    member x.CommandTypes = commandTypes
    member x.GetId = getId
    member x.AggregateType = aggregateType
    member x.Run (cmd:obj) =
        runCommand cmd 
    
open FSharpx.Collections

type TestEventStore = {
    Events : Map<string,Vector<(obj * EventMetadata)>>
}

[<CompilationRepresentation(CompilationRepresentationFlags.ModuleSuffix)>]
module TestEventStore =
    let empty : TestEventStore = { Events = Map.empty }
    let addEvent (stream, event, metadata) (store : TestEventStore) =
        let streamEvents = 
            match store.Events |> Map.tryFind stream with
            | Some events -> events
            | None -> Vector.empty

        let streamEvents' = streamEvents |> Vector.conj (event, metadata)
        { store with Events = store.Events |> Map.add stream streamEvents' }

type Settings = {
    // tenancyId -> aggregateType -> id
    GetStreamName : obj -> obj -> obj -> string
}

type TestSystem 
    (
        aggregates : list<Aggregate>, 
        lastEvents : list<string * obj * EventMetadata>, 
        allEvents : TestEventStore, 
        settings : Settings
    ) =
    let testTenancy = Guid.Parse("A1028FC2-67D2-4F52-A69F-714A0F571D8A") :> obj
    member x.RunCommand (cmd : obj) =    
        let cmdType = cmd.GetType()
        let sourceMessageId = Guid.NewGuid()
        let aggregate = 
            aggregates
            |> Seq.filter (fun a -> a.CommandTypes |> Seq.exists (fun c -> c = cmdType))
            |> Seq.toList
            |> function
            | [aggregate] -> aggregate
            | [] -> failwith <| sprintf "Could not find aggregate to handle %A" cmdType
            | xs -> failwith <| sprintf "Found more than one aggreate %A to handle %A" xs cmdType

        let id = aggregate.GetId cmd
        let streamName = settings.GetStreamName testTenancy aggregate.AggregateType id

        let result = 
            aggregate.Run cmd
            |> Seq.map (fun evt -> 
                            let metadata = {
                                SourceMessageId = sourceMessageId
                                MessageId = Guid.NewGuid() 
                            }
                            (streamName,evt, metadata))
            |> List.ofSeq
            
        let allEvents' =
            result
            |> Seq.fold (fun s e -> s |> TestEventStore.addEvent e) allEvents

        new TestSystem(aggregates, result, allEvents',settings)

    member x.Aggregates = aggregates

    member x.LastEvents = lastEvents

    member x.AddAggregate (handlers : AggregateHandlers<'TState, 'TEvents, 'TId>) aggregateType=
        let commandTypes = 
            handlers.CommandHandlers
            |> Seq.map (fun x -> x.CmdType)
            |> Seq.toList

        let unwrapper = MagicMapper.getUnwrapper<'TEvents>()

        let getHandler cmdType =    
            handlers.CommandHandlers
            |> Seq.find (fun x -> x.CmdType = cmdType)

        let getId (cmd : obj) =
            let handler = getHandler (cmd.GetType())
            handler.GetId cmd :> obj
            
        let runCmd (cmd : obj) =
            let handler = getHandler (cmd.GetType())
            handler.Handler None cmd
            |> function
            | Choice1Of2 events -> events |> Seq.map unwrapper |> List.ofSeq
            | _ -> []

        let aggregate = new Aggregate(commandTypes, runCmd, getId, aggregateType)
        new TestSystem(aggregate::aggregates, lastEvents, allEvents, settings)

    member x.Run (cmds : obj list) =
        cmds
        |> List.fold (fun (s:TestSystem) cmd -> s.RunCommand cmd) x

    member x.EvaluateState<'TState> (stream : string) (stateBuilder : StateBuilder<'TState>) =
        let streamEvents = 
            allEvents.Events 
            |> Map.tryFind stream
            |> function
            | Some events -> 
                events
            | None -> Vector.empty

        streamEvents
        |> Vector.map fst
        |> Vector.fold stateBuilder.Run stateBuilder.InitialState

    static member Empty settings =
        new TestSystem(List.empty, List.empty, TestEventStore.empty, settings)