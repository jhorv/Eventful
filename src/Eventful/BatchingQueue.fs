namespace Eventful

type internal QueueItem<'TItem, 'TResult> = 'TItem * AsyncReplyChannel<'TResult>
type internal BatchWork<'TKey, 'TItem,'TResult> = 'TKey * seq<QueueItem<'TItem, 'TResult>>

type internal BatchingQueueMessage<'TKey, 'TItem, 'TResult> =
    | Enqueue of 'TKey * QueueItem<'TItem, 'TResult>
    | Consume of AsyncReplyChannel<BatchWork<'TKey, 'TItem, 'TResult>>

type internal BatchingQueueState<'TKey, 'TItem, 'TResult when 'TKey : equality> = {
    Queues : System.Collections.Generic.Dictionary<'TKey, System.Collections.Generic.Queue<QueueItem<'TItem, 'TResult>>>
    HasWork : System.Collections.Generic.HashSet<'TKey>
    mutable ItemCount : int
}
with static member Zero = 
        { 
            Queues = new System.Collections.Generic.Dictionary<'TKey, System.Collections.Generic.Queue<QueueItem<'TItem, 'TResult>>>() 
            HasWork = new System.Collections.Generic.HashSet<'TKey>()
            ItemCount = 0
        }

[<CompilationRepresentation(CompilationRepresentationFlags.ModuleSuffix)>]
module internal BatchingQueueState =
    let rnd = new System.Random()
    let enqeue (queue : BatchingQueueState<'TKey, 'TItem, 'TResult>) (key : 'TKey) (item : QueueItem<'TItem, 'TResult>) =
        if (queue.Queues.ContainsKey key) then
            (queue.Queues.Item key).Enqueue item
        else
            let q = new System.Collections.Generic.Queue<QueueItem<'TItem,'TResult>>()
            q.Enqueue item
            queue.Queues.Add(key, q)
        queue.HasWork.Add key |> ignore
        queue.ItemCount <- queue.ItemCount + 1
        queue

    let getBatch (queue: BatchingQueueState<'TKey, 'TItem, 'TResult>) size : (BatchWork<'TKey, 'TItem, 'TResult> * BatchingQueueState<'TKey, 'TItem, 'TResult>) =
        let key = queue.HasWork |> Seq.nth (rnd.Next(queue.HasWork.Count))
        let selectedQueue = queue.Queues.Item key
        let batchSize = min size selectedQueue.Count

        let unfoldAcc size =
            if (size = 0) then
                None
            else 
                (Some (selectedQueue.Dequeue(), size - 1))

        let batch = 
            Seq.unfold unfoldAcc batchSize
            |> List.ofSeq
            |> Seq.ofList

        if selectedQueue.Count = 0 then
            queue.HasWork.Remove key |> ignore
        else
            ()

        queue.ItemCount <- queue.ItemCount - batchSize
        ((key,batch), queue)

    let queueSize (queue: BatchingQueueState<'TKey, 'TItem, 'TResult>) = 
        queue.ItemCount

/// A queue which groups items by key. Consumers receive the set of items
/// under a randomly chosen key, with the maximum number of items returned limited
/// to `maxBatchSize`.
type BatchingQueue<'TKey, 'TItem, 'TResult when 'TKey : equality> 
    (
        maxBatchSize : int,
        maxQueueSize : int
    ) =

    let log = createLogger "Eventful.BatchingQueue"

    let agent = newAgent "BatchingQueue" log (fun agent -> 
        let rec empty state = agent.Scan((fun msg -> 
            match msg with
            | Enqueue (key, (item, reply)) ->
                let state' = enqueue state key item reply
                Some (next state')
            | Consume reply -> None)) 
        and hasWork state = async {
            let! msg = agent.Receive()
            match msg with
            | Enqueue (key, (item, reply)) ->
                let state' = enqueue state key item reply
                return! (next state')
            | Consume reply -> 
                let state' = consume state reply
                return! (next state') }
        and full state = agent.Scan(fun msg ->
            match msg with
            | Consume reply ->
                let state' = consume state reply
                Some(next state')
            | _ -> None) 
        and enqueue state key item reply = BatchingQueueState.enqeue state key (item, reply)
        and consume state (reply :  AsyncReplyChannel<BatchWork<'TKey,'TItem,'TResult>>) = 
            let (batch, state') = BatchingQueueState.getBatch state maxBatchSize
            reply.Reply batch
            state'
        and next state =
            let queueSize = state |> BatchingQueueState.queueSize
            if (queueSize = 0) then
                empty state
            elif (queueSize >= maxQueueSize) then
                full state
            else
                hasWork state 

        empty BatchingQueueState<'TKey,'TItem,'TResult>.Zero
    )

    /// Get a batch of work to be done. The consumer must reply with the outcome for each item on
    /// the item's reply channel once it has been processed.
    member x.Consume () : Async<BatchWork<'TKey, 'TItem, 'TResult>> =
        agent.PostAndAsyncReply(fun ch -> Consume ch)

    /// Add an item to the queue and return the processing outcome.
    member x.Work (key : 'TKey) (item : 'TItem) : Async<'TResult> =
        agent.PostAndAsyncReply(fun ch -> Enqueue (key, (item, ch)))
