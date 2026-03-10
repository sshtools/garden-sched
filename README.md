# Garden Sched

A distributed task scheduler, shared cache and distributed lock based on JGroups.

## Features

 * Scheduler Implements `ScheduledExecutorService` presenting a familiar API.
 * Share tasks between all active nodes in the cluster.
 * Submitted tasks can run on any individual node or all nodes.
 * If a node goes down, another node will take over any of its jobs.
 * Supports additional `TaskTrigger` scheduling, to match the similar Spring capability. Allows custom scheduling.
 * Customisable serialization of tasks and triggers and other hook points  such as `TaskFilter` to allow Spring integration.
 * Optional Spring integration.
 * Hooks for optional long term peristence.
 * Job progress callback.
 
## Extras

There are some additional features that are not strictly relating to scheduling.

 * Events. Uses the same serialization mechanism, and allows an event (effectively just a serializable object) to be broadcast to all nodes. 
 * Shared Object Store. Again taking advantage of the same serialization mechanism, arbitrary objects may be placed in a key/value store. Other nodes can request the object by its key, and whatever node has it will return it. Can be used  as the basis of a clustered cache.
 * Simple distributed locks. 
 
## Limitations

 * All tasks must be `Serializable`. 
 * Distributed locks are simplistic and don't yet deal with nodes going down.


## Installation

Available on Maven Central, so just add the following dependency to your project's `pom.xml`.

```xml
<dependency>
    <groupId>com.sshtools</groupId>
    <artifactId>garden-sched-lib</artifactId>
    <version>0.9.3</version>
</dependency>
```

_See badge above for version available on Maven Central. Snapshot versions are in the [Sonatype OSS Snapshot Repository](https://central.sonatype.com/repository/maven-snapshots)._

```xml
<repository>
    <id>oss-snapshots</id>
    <url>https://central.sonatype.com/repository/maven-snapshots</url>
    <snapshots />
    <releases>
        <enabled>false</enabled>
    </releases>
</repository>
```
### JPMS

If you are using [JPMS](https://en.wikipedia.org/wiki/Java_Platform_Module_System), add `com.sshtools.gardensched` to your `module-info.java`.

### Build From Source

Using [Apache Maven](maven.apache.org/) is recommended.

 * Clone this module
 * Change directory to where you cloned to
 * Run `mvn package`
 * Jar Artifacts will be in the `target` directory.
 
## Usage
 
First create a `DistributedMachine`. You will need one of these regardless of the type of distributed component you will be using.

```java
try(var machine = new DistributedMachine.Builder().build()) {

	// TODO create distributed component here
}
```

### Distributed Storage

With distributed storage you have access to a unlimited number of limited `Map`-like structure `ObjectStore` that allow primitive operations such as `get`, `put`, `size`, `has`, `remove` and `keySet`.

Each value in the store can contain an unlimited number of key / value pairs in a specified namespace (its "path"). The path must be `String`, and the key and value must be be anything `Serializable`.

`DistributedObjectStore` provides the distributed store functionality and requires a  `ObjectStore` that actually stores the objects locally.

 * When a item is placed in the distributed store (`put`), it is added to its local store and broadcast to all nodes.
 * When an item is retrieved from the distributed source (`get`), it may (depending configuration look in its local store first, before broadcasting a request to all other nodes for the item with the given path and key. When the item is received, it may then be placed in the ocal store.
 * When an item is removed with `remove`, the removal happens locally first, then is broadcast to all nodes.
 * The `size` of the store is the largest reported size by any node.
 * The `keySet` of the store is a unique amalgamation of all keys reported by all nodes.
 
#### Limitations

 * If a node restarts, it stores are not currently synchronized with other nodes. This will only happen if the node itself requests items from the cluster and receives them. This may be addressed in future versions.
 * There is currently no locking, one node may place an item in the map at the same time as another. 
 

```java

var localStore = ObjectStore.basicMap();
var distributedStore = new DistributedObjectStore.Builder(machine, objectStore);

distributedStore.put("myMap", "Key1", "Some value");
Assertions.asserEquals("Some value", distributedStore.get("myMap", "Key1");


```

### Distributed Events

TODO `DistributedEvents`

### Distributed Lock

TODO `DistributedLock`

### Distributed Scheduler

TODO `DistributedScheduledExecutor`