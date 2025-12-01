package com.sshtools.gardensched;

import java.util.Set;
import java.util.concurrent.Delayed;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;

public class DelegatedScheduledFuture<V> extends AbstractDelegateScheduledFuture<V> {

	public DelegatedScheduledFuture(ClusterID id, ScheduledFuture<V> delegate, Set<String> classifiers) {
		super(id, delegate, classifiers);
	}

	@Override
	public long getDelay(TimeUnit unit) {
		return ((ScheduledFuture<V>)delegate).getDelay(unit);
	}

	@Override
	public int compareTo(Delayed o) {
		return ((ScheduledFuture<V>)delegate).compareTo(o);
	}

}
