# Distributed Clause Storage
This document briefly describes the current implementation of the `DistributedClauseStorage`.

## General Organization
- Each worker's `AnytimeSatClauseCommunicator` contains one instance of the `DistributedClauseStorage`.
- It handels everything regarding saving, importing and requesting clauses/progress.
- The `DistributedClauseStorage` contains a `Worker` that handels all background tasks regarding the clause database on this worker (e.g., importing clauses during clause sharing, preparing clauses for export).
- The `DistributedClauseStorage` also contains a pointer to an object of `JobTreeProgressSharer`, which provides the functionality regarding aggregating and broadcasting the progress requests and responses along the job tree.

## `DistributedClauseStorage`
- contains of a `Worker` for background tasks regarting the database
- points to a `JobTreeProgressSharer` for progress communication
- the `Worker` contains a list of `Slots` which cover a interval of epochs and contain the clauses, *currently contain no clauses*
- provides the interfaces:
    - `importSharing()`: called during every clause sharing broadcast, imports fraction of the sharing buffer into the local database, *currently only updates the intervals, no clause import*, initiates the progress request for this epoch
    - `resetProgressSharer()`: gets called in every `AnytimeSatClauseCommunicator::initiateClauseSharing()` and resets the `JobTreeProgressSharer` such that only one communication iteration is performed per clause sharing epoch
    - `handleIntervalRequestAggregation()`: is called when the worker receives a progress request aggregation message, adds the received request to the current request in the `JobTreeProgressSharer` and advances the sharer if possible
    - `handleIntervalRequestBroadcast()`: gers called when receiving a request broadcast message, asks `Worker` to prepare slots matching the request, *currently returns empty slots*,   
    - `handleIntervalResponseAggregation()`: is called when receiving a response aggregation message, adds own response to the `JobTreeProgressSharer` and advances the sharer if possible, *currently only empty slots*
    - `handleIntervalResponseBroadcast()`: is called when receiving a response broadcast message, asks worker to add import the clauses (*currently doesn't import anything*) and updates the missing intervals accordingly
    - `handleFinishedTasks()`: is called in `AnytimeSatClauseCommunicator::communicate()` and handles all finished `Worker` tasks, e.g., initiating the response when the requested slots are provided by the worker

## `JobTreeProgressSharer`
- contains all functionality regarting request and response aggregation
- provides `Request` and `Response` structures
- remembers for each worker, which requests or responses are needed in the current stage to proceed the aggregation
- provides the interfaces:
    - `initiate()`: is called every epoch to reset the sharer
    - `addOwnRequest()`: adds the request of the worker to the current request
    - `addChileRequest()`: adds the request of one child to the current request
    - `broadcastRequest()`: sends the request message to the worker's children
    - `addOwnResponse()`: adds the response of the current worker to the current response
    - `addChildResponse()`: adds the response of one child to the current response
    - `broadcastResponse()`: sends the response message to the workers children
    - `advance()`: checks if all criteria are met to advance the request or response aggregation and if so, sends message to parent node

### Request aggregation
- each node aggregated between 1 and 3 requests (if a worker doesn't need progress, it adds an empty request)
- the requests are then merged at the current node:
    - two request intervals $[a,b)$ and $[c,d)$ are merged to one interval $\big[\min(a,c), \max(b,d)\big)$ iff they overlap
    - the request keeps track of how many workers need each interval → if two intervals are merged, the number of requesting workers is simply added

- the idea behind this merge is that there are no *duplicated* claused in the response → it can therefore happen that the response interval is technically *too big*, however, this is ignored and imported as if it was the *perfect fit*
- the big difference to HCS is that the received clauses are not imported into the database, because they are alredy placed somewhere else (one could thing about rebalancing operations later but it is technically not necessary because it does not reduce imbalance)

### Response aggregation
- each worker receives the same request containing a (sorted) list of intervals and the number of requesting workers for each interval
- the background worker prepares all clauses that match the requested intervals (i.g., each local slot that overlaps the requested slots)
- the response then contains of one slot per requested interval where each slot gets a maximum capacity according to a global maximum and the number of requesting workers (to give more budget to the slots that dominate the request)
- the merge of the slots is then easy because each response contains the same slots (even though some of them are empty)
- when the response is broadcasted, each worker imports the clauses from the slots they actually requested


## Some things to think about
The idea of the HCS intervals is to give newer clauses more space in the database than old clauses. 
This is why slots are merged if there are $n$ consecutive slots of the same size which leads to sorted slots based on their size.
For a given $n$ however, one could pre-calculate the slot sizes and then merge slots accordingly.
This would allow the DCS to always request the *perfect* slots and make the complicated request merge obsolete because either two workers request the same slot or different slots.
The consequence of this would be that the layout of the slots would be identical among all workers.
Because the progress requests are in sync with the clause sharing, it is also impossible that one worker does not have the right slot ready for a request.
However, this would lead to a very different implementation of the `Worker` class of the `DistributedClauseStorage`.

HCS currently uses the `AdaptiveClauseStore` with the `ExactClauseFilter` to store the clauses in a slot and enforce a fixed literal budget for each slot.
For the DCS slots, it makes sense to also utlize this mechanic.
For response aggregation, a store-merge is also needed.
However, the store has to be serialized and sent along the job tree.
One has to think about how these clause stores can be merged on each worker with respect to the literal budget and prefering *better* clauses over *worse* clauses.
