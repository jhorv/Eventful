namespace BookLibrary

open System
open Eventful
open Eventful.EventStore
open EventStore.ClientAPI
open Suave
open Suave.Http
open Suave.Web

module SetupHelpers =

    let addEventTypes evtTypes handlers =
        Array.fold (fun h x -> StandardConventions.addEventType x h) handlers evtTypes

    let eventTypes =
        System.Reflection.Assembly.GetExecutingAssembly()
        |> Eventful.Utils.getLoadableTypes

    let toString x = x.ToString()

    let handlers openSession : EventfulHandlers<_,_,_,IEvent> =
        EventfulHandlers.empty (BookLibraryEventMetadata.GetAggregateType >> toString)
        |> EventfulHandlers.addAggregate (Book.handlers openSession)
        |> EventfulHandlers.addAggregate (BookCopy.handlers openSession)
        |> EventfulHandlers.addAggregate (Award.handlers ())
        |> EventfulHandlers.addAggregate (Delivery.handlers ())
        |> EventfulHandlers.addAggregate NewArrivalsNotification.handlers
        |> addEventTypes eventTypes

module Neo4jHelpers =
    open Eventful.Neo4j

    let buildGraphClient (neo4jConfig : Neo4jConfig) =
        let uri = new UriBuilder("http", neo4jConfig.Server, neo4jConfig.Port, "/db/data")
        let graphClient = new Neo4jClient.GraphClient(uri.Uri, "neo4j", "changeit")
        graphClient.Connect()
        graphClient :> Neo4jClient.ICypherGraphClient

    let initProjector (neo4jConfig : Neo4jConfig) (graphClient : Neo4jClient.ICypherGraphClient) (system : BookLibraryEventStoreSystem) =

        let graphName = neo4jConfig.GraphName

        let projector = 
            DocumentBuilderProjector.buildProjector 
                graphClient
                Book.documentBuilder
                (fun (m:EventStoreMessage) -> m.Event)
                (fun (m:EventStoreMessage) -> m.EventContext)
            :> IProjector<_,_,_>

            
        let writeQueue = new Neo4jWriteQueue(graphClient, 100, 10000, 10, Async.DefaultCancellationToken)
        //let writeQueue = new Neo4jWriteQueue(graphClient, 1, 1, 10, Async.DefaultCancellationToken)

        let executor actions =
            writeQueue.Work graphName actions


        let bulkNeo4jProjector =
            BulkNeo4jProjector.create
                (
                    graphName,
                    [projector],
                    Async.DefaultCancellationToken,
                    (fun _ -> async { () }),
                    graphClient,
                    executor,
                    100000,
                    1000,
                    Some (TimeSpan.FromSeconds(60.0)),
                    TimeSpan.FromSeconds 5.0
                )

        bulkNeo4jProjector.StartWork ()
        bulkNeo4jProjector.StartPersistingPosition ()

        let lastPosition =
            bulkNeo4jProjector.LastComplete() 
            |> Async.RunSynchronously 
            |> Option.map (fun eventPosition -> new EventStore.ClientAPI.Position(eventPosition.Commit, eventPosition.Prepare))

        (bulkNeo4jProjector, lastPosition)
        //((), ())



module RavenHelpers =
    open Eventful.Raven

    let buildDocumentStore (ravenConfig : RavenConfig) =
        let ds = new Raven.Client.Document.DocumentStore(Url = UriBuilder("http", ravenConfig.Server, ravenConfig.Port).ToString())
        ds.Initialize() |> ignore
        ds :> Raven.Client.IDocumentStore

    let initProjector (ravenConfig : RavenConfig) (documentStore : Raven.Client.IDocumentStore) (system : BookLibraryEventStoreSystem) =
        let projector = 
            DocumentBuilderProjector.buildProjector 
                documentStore 
                Book.documentBuilder
                (fun (m:EventStoreMessage) -> m.Event)
                (fun (m:EventStoreMessage) -> m.EventContext)
            :> IProjector<_,_,_>

        let aggregateStateProjector = 
            AggregateStatePersistence.buildProjector
                (EventStoreMessage.ToPersitedEvent >> Some)
                Serialization.esSerializer
                system.Handlers

        let cache = new RavenMemoryCache("myCache", documentStore)

        let writeQueue = new RavenWriteQueue(documentStore, 100, 10000, 10, Async.DefaultCancellationToken, cache)
        let readQueue = new RavenReadQueue(documentStore, 100, 1000, 10, Async.DefaultCancellationToken, cache)

        let bulkRavenProjector =
            BulkRavenProjector.create
                (
                    ravenConfig.Database,
                    [projector; aggregateStateProjector],
                    Async.DefaultCancellationToken,
                    (fun _ -> async { () }),
                    documentStore,
                    writeQueue,
                    readQueue,
                    100000, 
                    1000, 
                    Some (TimeSpan.FromSeconds(60.0)),
                    TimeSpan.FromSeconds 5.0
                )
        bulkRavenProjector.StartWork ()
        bulkRavenProjector.StartPersistingPosition ()

        let lastPosition =
            bulkRavenProjector.LastComplete() 
            |> Async.RunSynchronously 
            |> Option.map (fun eventPosition -> new EventStore.ClientAPI.Position(eventPosition.Commit, eventPosition.Prepare))

        (bulkRavenProjector, lastPosition)


type BookLibraryServiceRunner (applicationConfig : ApplicationConfig) =
    let log = createLogger "BookLibrary.BookLibraryServiceRunner"
    let webConfig = applicationConfig.WebServer
    let ravenConfig = applicationConfig.Raven
    let neo4jConfig = applicationConfig.Neo4j
    let eventStoreConfig = applicationConfig.EventStore

    let mutable client : EventStoreClient option = None
    let mutable eventStoreSystem : BookLibraryEventStoreSystem option = None

    let getIpAddress server = 
        let addresses = 
            System.Net.Dns.GetHostAddresses server
            |> Array.filter (fun x -> x.AddressFamily = Net.Sockets.AddressFamily.InterNetwork)
            |> List.ofArray

        match addresses with
        | [] -> failwith <| sprintf "Could not find IPv4 address for %s" server
        | x::xs -> x
        
    let getIpEndpoint server port =
        new System.Net.IPEndPoint(getIpAddress server, port)
        
    let getConnection (eventStoreConfig : EventStoreConfig) : Async<IEventStoreConnection> =
        async {
            let ipEndPoint = getIpEndpoint eventStoreConfig.Server eventStoreConfig.TcpPort
            let connectionSettingsBuilder = 
                ConnectionSettings
                    .Create()
                    .SetDefaultUserCredentials(new SystemData.UserCredentials(eventStoreConfig.Username, eventStoreConfig.Password))
                    .KeepReconnecting()
                    .SetHeartbeatTimeout(TimeSpan.FromMinutes 5.0)
            let connectionSettings : ConnectionSettings = ConnectionSettingsBuilder.op_Implicit(connectionSettingsBuilder)

            let connection = EventStoreConnection.Create(connectionSettings, ipEndPoint)
            connection.Connected.Add(fun _ -> log.RichDebug "EventStore Connected" [||])
            connection.ErrorOccurred.Add(fun e -> log.ErrorWithException <| lazy("EventStore Connection Error", e.Exception))
            connection.Disconnected.Add(fun _ -> log.RichDebug "EventStore Disconnected" [||])

            log.RichDebug "EventStore Connecting" [||]
            return! connection.ConnectAsync().ContinueWith(fun t -> connection) |> Async.AwaitTask
        }

    let buildWakeupMonitor documentStore onWakeups = 
        new Eventful.Raven.WakeupMonitor(documentStore, ravenConfig.Database, onWakeups) :> Eventful.IWakeupMonitor

    let buildEventStoreSystem (documentStore : Raven.Client.IDocumentStore) client =
        let getSnapshot = Eventful.Raven.AggregateStatePersistence.getStateSnapshot documentStore Serialization.esSerializer ravenConfig.Database
        let openSession () = documentStore.OpenAsyncSession(ravenConfig.Database)
        let wakeupMonitor = buildWakeupMonitor documentStore
        new BookLibraryEventStoreSystem(SetupHelpers.handlers openSession, client, Serialization.esSerializer, (fun pe -> { BookLibraryEventContext.Metadata = pe.Metadata; EventId = pe.EventId }), getSnapshot, wakeupMonitor)

    let initializedSystem documentStore eventStoreConfig = 
        async {
            let! conn = getConnection eventStoreConfig
            let client = new EventStoreClient(conn)
            let system = buildEventStoreSystem documentStore client
            return new BookLibrarySystem(system)
        } |> Async.StartAsTask

    let runAsyncAsTask f =
        async {
            try 
                do! f
            with | e -> 
                log.ErrorWithException <| lazy("Exception starting EventStoreSystem",e)
                raise ( new System.Exception("See inner exception",e)) // cannot use reraise in an async block
        } |> Async.StartAsTask

    member x.Start () =
        log.Debug <| lazy "Starting App"
        async {
            let! connection = getConnection eventStoreConfig
            let c = new EventStoreClient(connection)

            let documentStore = RavenHelpers.buildDocumentStore ravenConfig
            let graphClient = Neo4jHelpers.buildGraphClient neo4jConfig

            let system : BookLibraryEventStoreSystem = buildEventStoreSystem documentStore c
            system.Start() |> Async.StartAsTask |> ignore

            let bookLibrarySystem = new BookLibrarySystem(system)

            let dbCommands = documentStore.AsyncDatabaseCommands.ForDatabase(ravenConfig.Database)

            let webAddress = getIpAddress webConfig.Server

            let suaveLogger = new SuaveEventfulLogger(Serilog.Log.Logger.ForContext("SourceContext","Suave"))
            let suaveConfig = 
                { defaultConfig with 
                   Types.SuaveConfig.bindings = [Types.HttpBinding.mk' Types.Protocol.HTTP (webAddress.ToString()) webConfig.Port] 
                   logger = suaveLogger }

            // start web
            let (ready, listens) =
                choose 
                    [ BooksWebApi.config bookLibrarySystem
                      BooksCopiesWebApi.config bookLibrarySystem
                      AwardsWebApi.config bookLibrarySystem
                      DeliveryWebApi.config bookLibrarySystem
                      FileWebApi.config dbCommands
                      (Suave.Http.RequestErrors.NOT_FOUND "404 Not Found") ]
                |> startWebServerAsync suaveConfig
            listens |> Async.Start

            //let bulkRavenProjector, lastRavenPosition = RavenHelpers.initProjector ravenConfig documentStore system
            let bulkNeo4jProjector, lastNeo4jPosition = Neo4jHelpers.initProjector neo4jConfig graphClient system

            let handle (projector : BulkProjector<#IBulkMessage,'TMessage>) id (re : EventStore.ClientAPI.ResolvedEvent) =
                log.Debug <| lazy(sprintf "Projector received event : %s" re.Event.EventType)
                match system.EventStoreTypeToClassMap.ContainsKey re.Event.EventType with
                | true ->
                    let eventClass = system.EventStoreTypeToClassMap.Item re.Event.EventType
                    let evtObj = Serialization.esSerializer.DeserializeObj re.Event.Data eventClass
                    let metadata = Serialization.esSerializer.DeserializeObj re.Event.Metadata typeof<BookLibraryEventMetadata> :?> BookLibraryEventMetadata

                    let eventStoreMessage : EventStoreMessage = {
                        EventContext = metadata
                        Id = re.Event.EventId
                        Event = evtObj
                        StreamIndex = re.Event.EventNumber
                        EventPosition = { Commit = re.OriginalPosition.Value.CommitPosition; Prepare = re.OriginalPosition.Value.PreparePosition }
                        StreamName = re.Event.EventStreamId
                        EventType = re.Event.EventType
                    }

                    //bulkRavenProjector.Enqueue (eventStoreMessage)
                    projector.Enqueue (eventStoreMessage)
                | false -> async { () }


//            let handle id (re : EventStore.ClientAPI.ResolvedEvent) =
//                log.Debug <| lazy(sprintf "Projector received event : %s" re.Event.EventType)
//                match system.EventStoreTypeToClassMap.ContainsKey re.Event.EventType with
//                | true ->
//                    let eventClass = system.EventStoreTypeToClassMap.Item re.Event.EventType
//                    let evtObj = Serialization.esSerializer.DeserializeObj re.Event.Data eventClass
//                    let metadata = Serialization.esSerializer.DeserializeObj re.Event.Metadata typeof<BookLibraryEventMetadata> :?> BookLibraryEventMetadata
//
//                    let eventStoreMessage : EventStoreMessage = {
//                        EventContext = metadata
//                        Id = re.Event.EventId
//                        Event = evtObj
//                        StreamIndex = re.Event.EventNumber
//                        EventPosition = { Commit = re.OriginalPosition.Value.CommitPosition; Prepare = re.OriginalPosition.Value.PreparePosition }
//                        StreamName = re.Event.EventStreamId
//                        EventType = re.Event.EventType
//                    }
//
//                    //bulkRavenProjector.Enqueue (eventStoreMessage)
//                    bulkNeo4jProjector.Enqueue (eventStoreMessage)
//                | false -> async { () }

            let onLive _ = ()

            log.Debug <| lazy(sprintf "About to subscribe projector")
            //c.subscribe lastRavenPosition (handle bulkRavenProjector) onLive |> ignore
            c.subscribe lastNeo4jPosition (handle bulkNeo4jProjector) onLive |> ignore
            log.Debug <| lazy(sprintf "Subscribed projector")

            client <- Some c
            eventStoreSystem <- Some system
        } |> runAsyncAsTask

    member x.Stop () =
        log.Debug <| lazy "App Stopping"