package com.sshtools.gardensched;

import java.util.concurrent.atomic.AtomicInteger;

import org.jgroups.Address;
import org.jgroups.util.Promise;

public class TaskEntry {
	
	final DistributedTask<?> task;
	final Address submitter;
	final Promise<Object> promise = new Promise<>();
	final AtomicInteger results;
	final TaskSpec spec;
	final ClusterID id;

	public TaskEntry(ClusterID id, DistributedTask<?> task, Address submitter, int expectedResults, TaskSpec spec) {
		this.task = task;
		this.id = id;
		this.submitter = submitter;
		this.results = new AtomicInteger(expectedResults);
		this.spec = spec;
	}
	
	public ClusterID id() {
		return id;
	}

	public DistributedTask<?> task() {
		return task;
	}

	public Address submitter() {
		return submitter;
	}

	public Promise<Object> promise() {
		return promise;
	}

	public AtomicInteger results() {
		return results;
	}

	public TaskSpec spec() {
		return spec;
	}
}