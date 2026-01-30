## TODO

[ ] Test range reads

[ ] Investigate non-log2 structure of SST heirarchy
    That means some logN

[ ] README

[ ] Improve descriptor pool cleanup process to load the system less

[ ] Make a search index of disktable ranges.
    In theory it should make the search for disktable a bit faster
    But in practice I doubt it, cause lineary checking all disktables
    is not much slower

[ ] Improve restart to not reset the persistent_term

## WONT DO

[?] Split descriptor pool into different ets tables
    Right now it is just a one big ets table
    But it can be a ets of ets
    Not sure about performance benefit from this one

[W] traverse ets instead of tab2list (after layered)
    This may introduce problems when 1 read takes
    more time than two dumps

[W] Layered

## DONE

[x] Configuration of parameters at start_link (including spawn_opt)
[x] Cleanup and drop table
[x] Compile-time configuration of stats
[x] Investigate ets heir for descriptor pool (not suitable cause of the prim_file closing when caller dies, not the owner)
[x] Implement cell checkin when caller is killed during checkout
[x] Fix descriptor pool linking and implement a supervisor
[x] Implement a periodic descriptor pool cleanup process
[x] Implement write no_sync

Need to write to memtable on call, add to batch and return
Then need to sync

