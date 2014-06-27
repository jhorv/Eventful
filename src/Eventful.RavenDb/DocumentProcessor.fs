﻿namespace Eventful.Raven

open System

open Raven.Abstractions.Data
open Raven.Json.Linq

type DocumentProcessor<'TKey, 'TDocument, 'TContext> = {
    GetDocumentKey : 'TKey -> string
    EventTypes : seq<Type>
    MatchingKeys: SubscriberEvent<'TContext> -> seq<'TKey>
    Process: 'TKey -> ('TDocument * RavenJObject * Etag) -> SubscriberEvent<'TContext> -> ('TDocument * RavenJObject * Etag)
    NewDocument : 'TKey -> ('TDocument * RavenJObject * Etag)
    BeforeWrite : ('TDocument * RavenJObject * Etag) -> ('TDocument * RavenJObject * Etag)
}