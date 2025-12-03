package com.sshtools.gardensched;

import org.jgroups.Address;

public abstract class TaskContext {

	final static ThreadLocal<TaskContext> ctx = new ThreadLocal<>();
	
	public static TaskContext get() {
		var c = ctx.get();
		if(c == null)
			throw new IllegalStateException("Not running in context of a task.");
		return c;
	}
	
	public abstract ClusterID id();
	
	public abstract Address address();

	public abstract TaskProgress progress();
	
	public abstract void cancel();
}
