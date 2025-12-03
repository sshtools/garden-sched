package com.sshtools.gardensched;

import java.util.Set;
import java.util.stream.Stream;

public interface TaskStore {
	
	default void initialise(DistributedScheduledExecutor executor) {}

	void store(TaskEntry entry);
	
	Stream<TaskEntry> entries();
	
	void remove(ClusterID id, Set<String> classifiers);
}
