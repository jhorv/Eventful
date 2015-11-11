namespace Eventful

open System

open FSharpx

type internal GroupEntry<'TItem> = {
    Items : List<Int64 * 'TItem>
    Processing : List<Int64 * 'TItem>
}
  
type internal MutableOrderedGroupingBoundedQueueMessages<'TGroup, 'TItem> = 
  | AddItem of ((unit -> (seq<'TItem * 'TGroup>)) * Async<unit> option)
  | ConsumeWork of (('TGroup * seq<'TItem> -> Async<unit>) * AsyncReplyChannel<Async<unit>>)
  | GroupComplete of 'TGroup
  | NotifyWhenAllComplete of AsyncReplyChannel<unit>

type internal MutableOrderedGroupingBoundedQueueState<'TGroup, 'TItem> 
    (
        ?groupComparer : System.Collections.Generic.IComparer<'TGroup>
    ) =
    // normal .NET dictionary for performance
    // very mutable
    // TODO: We don't seem to actually require this to be a *sorted* dictionary
    let groupItems = 
        match groupComparer with
        | Some c -> new System.Collections.Generic.SortedDictionary<'TGroup, GroupEntry<'TItem>>(c)
        | None -> new System.Collections.Generic.SortedDictionary<'TGroup, GroupEntry<'TItem>>()

    let workQueue = new System.Collections.Generic.Queue<'TGroup>()

    member x.GetGroupItemsCount () =
        groupItems.Count

    member x.GetWorkQueueCount () =
        // BUG: It looks like this should really be returning the number of pending items in all queues.
        //      But, fixing this will mean the full queue logic will suddenly start getting exercised.
        workQueue.Count

    member x.AddItemToGroup item group =
        let (exists, value) = groupItems.TryGetValue(group)
        let value = 
            if exists then
                value
            else 
                workQueue.Enqueue group
                { Items = List.empty; Processing = List.empty } 
        let value' = { value with Items = item::value.Items }
        groupItems.[group] <- value'
        ()

    member x.GroupComplete group =
        let values = groupItems.Item group
        if (not (values.Items |> List.isEmpty)) then
            workQueue.Enqueue(group)
        else
            groupItems.Remove group |> ignore

    member x.ConsumeNext () =
        let nextKey = workQueue.Dequeue()
        let values = groupItems.Item nextKey
        let newValues = { values with Items = List.empty; Processing = values.Items }
        groupItems.Remove(nextKey) |> ignore
        groupItems.Add(nextKey, newValues)
        (nextKey, values)

/// Queue items, grouping them by their 'TGroup. Each consume will get all of the queued
/// items for the group which has the oldest queued item.
type MutableOrderedGroupingBoundedQueue<'TGroup, 'TItem>
    (
        ?maxItems, 
        ?name : string,
        ?groupComparer : System.Collections.Generic.IComparer<'TGroup>
    ) =
    let name = name |> Option.getOrElseF makeProbablyUniqueShortName
    let maxItems = maxItems |> Option.getOrElse 10000

    let log = createLogger <| sprintf "MutableOrderedGroupingBoundedQueue<%s,%s> %s" typeof<'TGroup>.Name typeof<'TItem>.Name name
    
    let state = new MutableOrderedGroupingBoundedQueueState<'TGroup, 'TItem>(?groupComparer = groupComparer)
    
    let activeGroupsGauge = Metrics.Metric.Gauge(sprintf "Active Groups %s" name, (fun () -> state.GetGroupItemsCount () |> float), Metrics.Unit.Items)
    let workQueueGauge = Metrics.Metric.Gauge(sprintf "Work Queue Size %s" name, (fun () -> state.GetWorkQueueCount () |> float), Metrics.Unit.Items)

    let lastCompleteTracker = new LastCompleteItemAgent<int64>(name)

    let dispatcherAgent = 
        let theAgent = Agent.Start(fun agent -> 
            let rec empty itemIndex = 
                agent.Scan(function
                | AddItem x -> Some <| enqueue x itemIndex
                | ConsumeWork _ -> None
                | GroupComplete group -> Some <| groupComplete group itemIndex
                | NotifyWhenAllComplete reply -> 
                    if itemIndex = 0L then
                        reply.Reply()
                    else 
                        lastCompleteTracker.NotifyWhenComplete(itemIndex - 1L, Some "NotifyWhenComplete empty", async { reply.Reply() } )
                    Some <| empty itemIndex)

            and hasWork itemIndex =
                agent.Scan(function
                | AddItem x -> Some <| enqueue x itemIndex
                | ConsumeWork x -> Some <| consume x itemIndex
                | GroupComplete group -> Some <| groupComplete group itemIndex
                | NotifyWhenAllComplete reply ->
                    lastCompleteTracker.NotifyWhenComplete(itemIndex - 1L, Some "NotifyWhenComplete hasWork", async { reply.Reply() } )
                    Some <| hasWork itemIndex)

            and full itemIndex = 
                agent.Scan(function
                | AddItem x -> None
                | ConsumeWork x -> Some <| consume x itemIndex
                | GroupComplete group -> Some <| groupComplete group itemIndex
                | NotifyWhenAllComplete reply -> 
                    if itemIndex = 0L then
                        reply.Reply()
                    else 
                        lastCompleteTracker.NotifyWhenComplete(itemIndex - 1L, Some "NotifyWhenComplete empty", async { reply.Reply() } )
                    Some <| empty itemIndex)

            and enqueue (itemsF :(unit -> (seq<'TItem * 'TGroup>)), onComplete) itemIndex = async {
                let indexedItems =
                    try
                        itemsF ()
                        |> Seq.mapi (fun i item -> (item, itemIndex + int64 i))
                        |> List.ofSeq  // Materialize the sequence while in the try block
                    with | e ->
                        log.ErrorWithException <| lazy("Exception thrown enqueueing item", e)
                        List.empty

                for ((item, group), index) in indexedItems do
                    state.AddItemToGroup (index, item) group
                    lastCompleteTracker.Start index

                let lastIndex =
                    indexedItems
                    |> Seq.map snd
                    |> Seq.fold (fun _ x -> Some x) None

                match onComplete, lastIndex with
                | Some action, Some lastIndex ->
                    lastCompleteTracker.NotifyWhenComplete(lastIndex, None, action)
                | Some action, None ->
                    do! action  // No items were added, notify of completion immediately
                | None, _ ->
                    ()

                let nextIndex =
                    match lastIndex with
                    | Some lastIndex -> lastIndex + 1L
                    | None -> itemIndex

                return! nextMessage nextIndex
                }

            and groupComplete group itemIndex = async {
                state.GroupComplete group
                return! nextMessage itemIndex }

            and consume (workCallback, reply) itemIndex = async {
                let (nextKey, values) = state.ConsumeNext()
                let work =
                    async {
                        try
                            do! workCallback(nextKey,values.Items |> List.rev |> List.map snd) 
                        with | e ->
                            log.ErrorWithException <| lazy("Exception thrown while processing item", e)
                        
                        for (i, _) in values.Items do
                            lastCompleteTracker.Complete i

                        agent.Post <| GroupComplete nextKey
                    }

                reply.Reply work

                return! nextMessage itemIndex }

            and nextMessage itemIndex = async {
                let currentQueueSize = state.GetWorkQueueCount()
                if(currentQueueSize = 0) then
                    return! empty itemIndex
                elif currentQueueSize >= maxItems then
                    return! full itemIndex   
                else
                    return! hasWork itemIndex
            }

            empty 0L )

        theAgent.Error.Add(fun exn -> 
            log.ErrorWithException <| lazy("Exception thrown by MutableOrderedGroupingBoundedQueueMessages", exn))

        theAgent

    let queueFullEvent = new Event<_>()    

    /// fired each time a full queue is detected
    [<CLIEvent>]
    member this.QueueFullEvent = queueFullEvent.Publish

    /// Enqueue `input`, using `group` to determine the sequence of items and corresponding grouping keys.
    member this.Add (input:'TInput, group: ('TInput -> (seq<'TItem * 'TGroup>)), ?onComplete : Async<unit>) =
        async {
            while(state.GetWorkQueueCount() + dispatcherAgent.CurrentQueueLength > maxItems) do
                queueFullEvent.Trigger()
                do! Async.Sleep(10)
            dispatcherAgent.Post <| AddItem ((fun () -> group input), onComplete) }

    /// Consume all of the queued items for the group with the oldest enqueued item.
    member this.Consume (work:(('TGroup * seq<'TItem>) -> Async<unit>)) =
        dispatcherAgent.PostAndAsyncReply(fun ch -> ConsumeWork(work, ch))

    /// Wait for all of the currently queued items to be processed.
    member this.CurrentItemsComplete () = 
        dispatcherAgent.PostAndAsyncReply(fun ch -> NotifyWhenAllComplete(ch))
