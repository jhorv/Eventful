namespace Eventful

open System 

/// A statebuilder that calculates wakeup time
type WakeupFold<'TMetadata> = IStateBuilder<UtcDateTime option, 'TMetadata, unit>

module Wakeup = 
    /// A wakeup time statebuilder for something that never wakes up
    let noWakeup<'TAggregateId, 'TMetadata when 'TAggregateId : equality> = 
        AggregateStateBuilder.constant None :> WakeupFold<'TMetadata>
