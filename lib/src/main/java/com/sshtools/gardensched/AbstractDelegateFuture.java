/**
 * Copyright © 2025 JAdaptive Limited (support@jadaptive.com)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.sshtools.gardensched;

import java.io.Serializable;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

public abstract class AbstractDelegateFuture<V> implements Future<V>, IdentifiableFuture<V> {
	
	protected final Future<V> delegate;
	private final ClusterID id;
	private final Set<String> classifiers;
	private final Map<String, Serializable> attributes;

	public AbstractDelegateFuture(ClusterID id, Future<V> delegate, Set<String> classifiers, Map<String, Serializable> attributes) {
		this.delegate = delegate;
		this.id = id;
		this.classifiers = classifiers;
		this.attributes = attributes;
	}

	@Override
	public Map<String, Serializable> attributes() {
		return attributes;
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
