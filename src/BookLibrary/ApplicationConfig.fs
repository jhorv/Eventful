namespace BookLibrary

open System
open EventStore.ClientAPI
open Eventful
open Eventful.EventStore
open FSharpx
open Nessos.Argu

type BookLibrarySystem (system : BookLibraryEventStoreSystem) = 
    interface IBookLibrarySystem with
        member x.RunCommand cmd =
            system.RunCommand () cmd

        member x.RunCommandTask cmd =
            system.RunCommand () cmd
            |> Async.StartAsTask

type CLIArguments =
    | RavenServer of host:string * port:int
    | RavenDatabase of string
    | Neo4jServer of host:string * port:int
    | EventStore of host:string * port:int
    | WebServer of host:string * port:int
    | Create_Raven_Database
with 
    static member Parser = ArgumentParser.Create<CLIArguments>()
    interface IArgParserTemplate with
        member s.Usage =
            match s with
            | RavenServer _ -> "Specify Raven Server (hostname : port)."
            | RavenDatabase _ -> "Specify Raven Database name."
            | Neo4jServer _ -> "Specify Neo4j Server (hostname : port)."
            | EventStore _ -> "Specify EventStore Server (hostname : port)."
            | WebServer _ -> "Specify Host and Port for Http Api (hostname : port)."
            | Create_Raven_Database -> "Create Raven database and exit"

type RavenConfig = {
    Server : string
    Port : int
    Database : string
}

type Neo4jConfig = {
    Server : string
    Port : int
    GraphName : string
}

type EventStoreConfig = {
    Server : string
    TcpPort : int
    Username : string
    Password: string
}

type WebServerConfig = {
    Server : string
    Port : int
}

type ApplicationConfig = {
    Raven: RavenConfig
    Neo4j: Neo4jConfig
    EventStore : EventStoreConfig
    WebServer : WebServerConfig
}

[<CompilationRepresentation(CompilationRepresentationFlags.ModuleSuffix)>]
module ApplicationConfig = 
    let default_eventstore_config : EventStoreConfig = {
        Server = "localhost"
        TcpPort = 1113
        Username = "admin"
        Password = "changeit" }

    let default_raven_config : RavenConfig = {
        Server = "localhost"
        Port = 8080
        Database = "BookLibrary"
    }

    let default_neo4j_config : Neo4jConfig = {
        Server = "localhost"
        Port = 7474
        GraphName = ""
    }

    let default_web_config : WebServerConfig = {
        Server = "localhost"
        Port = 8083
    }

    let default_application_config : ApplicationConfig = {
        Raven = default_raven_config
        Neo4j = default_neo4j_config
        EventStore = default_eventstore_config
        WebServer = default_web_config 
    }