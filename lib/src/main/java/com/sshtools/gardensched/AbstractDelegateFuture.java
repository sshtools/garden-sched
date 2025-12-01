package com.sshtools.gardensched;

import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

public abstract class AbstractDelegateFuture<V> implements Future<V>, IdentifiableFuture<V> {
	
	protected final Future<V> delegate;
	private final ClusterID id;
	private final Set<String> classifiers;

	public AbstractDelegateFuture(ClusterID id, Future<V> delegate, Set<String> classifiers) {
		this.delegate = delegate;
		this.id = id;
		this.classifiers = classifiers;
	}

	@Override
	public Set<String> classifiers() {
		return classifiers;
	}

	@Override
	public boolean cancel(boolean mayInterrupt) {
		return delegate.cancel(mayInterrupt);
	}

	@Override
	public V get() throws InterruptedException, ExecutionException {
		return delegate.get();
	}

	@Override
	public V get(long timeout, TimeUnit unit) throws InterruptedException, ExecutionException, TimeoutException {
		return delegate.get(timeout, unit);
	}

	@Override
	public boolean isCancelled() {
		return delegate.isCancelled();
	}

	@Override
	public boolean isDone() {
		return delegate.isDone();
	}

	@Override
	public ClusterID clusterID() {
		return id;
	}

}
