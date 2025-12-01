# Garden Sched

A distributed task scheduler based on JGroups.

## Features

 * Implements `ScheduledExecutorService` presenting a familiar API.
 * Share tasks between all active nodes in the cluster.
 * Submitted tasks can run on any individual node or all nodes.
 * If a node goes down, another node will take over any of its jobs.
 * Supports additional `TaskTrigger` scheduling, to match the similar Spring capability. Allows custom scheduling.
 * Customisable serialization of tasks and triggers and other hook points  such as `TaskFilter` to allow Spring integration.
 * Optional Spring helpers.
 
## Limitations

 * No serialization to external storage yet. A job will exists as long as the cluster does.
 * All tasks must be `Serializable`. 


 
