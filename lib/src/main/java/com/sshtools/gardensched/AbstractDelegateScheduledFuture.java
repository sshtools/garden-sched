package com.sshtools.gardensched;

import java.util.Set;
import java.util.concurrent.Future;
import java.util.concurrent.ScheduledFuture;

public abstract class AbstractDelegateScheduledFuture<V> extends AbstractDelegateFuture<V> implements ScheduledFuture<V> {

	public AbstractDelegateScheduledFuture(ClusterID id, Future<V> delegate, Set<String> classifiers) {
		super(id, delegate, classifiers);
	}

}
