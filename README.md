# Garden Sched

A distributed task scheduler based on JGroups.

## Features

 * Implements `ScheduledExecutorService` presenting a familiar API.
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
 
## Limitations

 * All tasks must be `Serializable`. 


## Installation

Available on Maven Central, so just add the following dependency to your project's `pom.xml`.

```xml
<dependency>
    <groupId>com.sshtools</groupId>
    <artifactId>garden-sched-lib</artifactId>
    <version>0.0.3</version>
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
 
TODO