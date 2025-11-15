# Microlsm

Very small and simple LSM KV Database implementation, not yet tested in any real project.

The goal of this project is to provide a starting point for creating your own LSM database.

## So, how do I use it?

First, learn how LSM databases work. They are very easy to understand and can be customized in a lot of different ways.

Then, clone this project and start adding and changing parts, tinkering the perfect version for your use case.

This implementation does bare minimum of writing, reading and deleting a single key value pair
and the only LSM optimization there is WAL batching.

## Alright, what can I hack?

* Do you want to have range reads? Implement it by searching left and right keys' offsets in index and then read all the chunks between them, instantiating keys and values.

* Do you want to have lowest latency possible? Remove WAL batching

* Is it okay to lose recent writes in case of malfunction? Sync WAL lazily with periodic timers

* Do you want to have a special SST merging strategy? Go ahead, reimplement the 50 lines of code which do them

* Do you want the most performant reads? Sure, add bloom filters and encoding for it in blocks.

* Do you want to use all RAM available for indices? Change 50 lines of the indexing algorithm.

* Do you want to have minimum RAM usage? Make indices as small as possible. Add search trees or just key arrays to sst blocks.

* Do you want to reduce disk usage? Sure, add stream-compress the blocks, use `term_to_binary/2` with high compression levels.
