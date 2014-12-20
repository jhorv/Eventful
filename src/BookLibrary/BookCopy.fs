﻿namespace BookLibrary

open Eventful
open FSharpx
open BookLibrary.Aggregates
open FSharp.Control.AsyncSeq

[<CLIMutable>]
type AddBookCopyCommand = {
    [<GeneratedIdAttribute>]BookCopyId : BookCopyId
    BookId : BookId
}

module BookCopy =
    let getStreamName () (bookCopyId : BookCopyId) =
        sprintf "BookCopy-%s" <| bookCopyId.Id.ToString("N")

    let getEventStreamName (context : BookLibraryEventContext) (bookCopyId : BookCopyId) =
        sprintf "BookCopy-%s" <| bookCopyId.Id.ToString("N")

    let inline getBookCopyId (a: ^a) _ = 
        (^a : (member BookCopyId: BookCopyId) (a))

    let buildBookCopyMetadata = 
        Aggregates.emptyMetadata AggregateType.BookCopy

    let inline bookCopyCmdHandler f = 
        cmdHandler f buildBookCopyMetadata

    let cmdHandlers = 
        seq {
           let addBookCopy (cmd : AddBookCopyCommand) =
               { 
                   BookCopyAddedEvent.BookCopyId = cmd.BookCopyId
                   BookId = cmd.BookId
               }

           yield bookCopyCmdHandler addBookCopy
        }

    let deliveryHandler openSession (evt : DeliveryAcceptedEvent, ctx) = asyncSeq {
            let! deliveryDocument = DocumentHelpers.getDeliveryDocument openSession evt.FileId

            for book in deliveryDocument.Books do
                for i in [1..book.Copies] do
                    let bookCopyId = BookCopyId.New()
                    let result = {
                       UniqueId = sprintf "%s_%d" (evt.DeliveryId.Id.ToString()) i
                       Events = 
                        {
                            BookCopyAddedEvent.BookCopyId = bookCopyId
                            BookId = book.BookId
                        } :> IEvent
                        |> Seq.singleton
                        |> Seq.map (fun evt -> (evt, buildBookCopyMetadata))
                    }
                    yield (bookCopyId, konst result) 
        }

    let eventHandlers dbCmd =
        seq {
            yield AggregateActionBuilder.onEventMultiAsync StateBuilder.nullStateBuilder (deliveryHandler dbCmd)
        }

    let handlers dbCmd =
        Eventful.Aggregate.toAggregateDefinition 
            AggregateType.BookCopy 
            BookLibraryEventMetadata.GetUniqueId
            getStreamName 
            getEventStreamName 
            cmdHandlers 
            (eventHandlers dbCmd)

open Suave.Http
open Suave.Http.Applicatives
open BookLibrary.WebHelpers

module BooksCopiesWebApi = 
    let addHandler (cmd : AddBookCopyCommand) =
        let bookCopyId = BookCopyId.New()
        let cmd = { cmd with BookCopyId = bookCopyId }
        let successResponse = 
            let responseBody = new Newtonsoft.Json.Linq.JObject();
            responseBody.Add("bookCopyId", new Newtonsoft.Json.Linq.JValue(bookCopyId))
            responseBody
        (cmd, successResponse :> obj)

    let config system =
        choose [
            url "/api/bookcopies" >>= choose
                [ 
                    POST >>= commandHandler system addHandler
                ]
        ]