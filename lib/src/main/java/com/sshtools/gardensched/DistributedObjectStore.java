/*
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
import java.util.Collection;
import java.util.Set;
import java.util.UUID;
import java.util.stream.Collectors;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.sshtools.gardensched.Request.StorePayload;
import com.sshtools.gardensched.Request.Type;

public class DistributedObjectStore implements ObjectStore {

	private final static Logger LOG = LoggerFactory.getLogger(DistributedObjectStore.class);
	private final ObjectStore os;
	private final DistributedScheduledExecutor distributedScheduledExecutor;
	private final boolean checkLocalObjectStorageFirst;
	private final boolean storeToLocalUponRemoteRetrieval;
	private final Ack acks;

	DistributedObjectStore(Ack acks, ObjectStore os, DistributedScheduledExecutor distributedScheduledExecutor,
			boolean checkLocalObjectStorageFirst, boolean storeToLocalUponRemoteRetrieval) {
		this.os = os;
		this.acks = acks;
		this.distributedScheduledExecutor = distributedScheduledExecutor;
		this.checkLocalObjectStorageFirst = checkLocalObjectStorageFirst;
		this.storeToLocalUponRemoteRetrieval = storeToLocalUponRemoteRetrieval;
	}

	@Override
	public void put(String path, Serializable key, Serializable value) {
		os.put(path, key, value);
		distributedScheduledExecutor.sendRequest(null, new Request(Type.STORED_OBJECT, new StorePayload(path, key)));
	}

	@SuppressWarnings("unchecked")
	@Override
	public <T extends Serializable> Set<T> keySet(String path) {
		var id = ClusterID.createNext(UUID.randomUUID().toString());
		try {
			return acks.runWithAck(Request.Type.KEYS, id, distributedScheduledExecutor.clusterSize(), () -> {
				distributedScheduledExecutor.sendRequest(null, new Request(Type.KEYS, id, new StorePayload(path, "")));
			}).stream()
				.filter(f -> f != null)
				.map(s -> (Collection<T>)s)
				.flatMap(f-> f.stream())
				.distinct()
				.collect(Collectors.toSet());
		}
		catch(RuntimeException re) {
			throw re;
		}
		catch(Exception e) {
			throw new IllegalStateException("Failed to get from object store.", e);
		}
	}

	@SuppressWarnings("unchecked")
	public <T extends Serializable> T get(String path, Serializable key) {

		if(checkLocalObjectStorageFirst && os.has(path, key)) {

			if (LOG.isDebugEnabled()) {
				LOG.debug("Get called for {} in {} and it exists in the local store, using  that.", path, key);
			}

			return (T)os.get(path, key);
		}

		var id = ClusterID.createNext(UUID.randomUUID().toString());
		try {
			T res = (T)acks.runWithAck(Request.Type.GET_OBJECT, id, distributedScheduledExecutor.clusterSize(), () -> {
				distributedScheduledExecutor.sendRequest(null, new Request(Type.GET_OBJECT, id, new StorePayload(path, key)));
			}).stream().filter(f -> f != null).findFirst().orElse(null);
			if(storeToLocalUponRemoteRetrieval) {
				if(res == null) {
					if (LOG.isDebugEnabled()) {
						LOG.debug("null returned, assuming key does not exist, so not caching locall for {} / {}.", path, key);
					}
				}
				else {
					/*
					 * TODO make this better. It is ambiguous whether null means a null value
					 * or if the key does not exist. For now we have to take it to mean it
					 * doesn't exist.
					 */
					os.put(path, key, res);
				}
			}
			return res;
		}
		catch(RuntimeException re) {
			throw re;
		}
		catch(Exception e) {
			throw new IllegalStateException("Failed to get from object store.", e);
		}
	}

	@Override
	public boolean has(String path, Serializable key) {
		if(checkLocalObjectStorageFirst && os.has(path, key)) {
			return true;
		}

		var id = ClusterID.createNext(UUID.randomUUID().toString());
		try {
			return acks.runWithAck(Request.Type.HAS_OBJECT, id, distributedScheduledExecutor.clusterSize(), () -> {
				distributedScheduledExecutor.sendRequest(null, new Request(Type.HAS_OBJECT, id, new StorePayload(path, key)));
			}).stream().map(s -> (Boolean)s).filter(t -> t).findFirst().orElse(false);
		}
		catch(RuntimeException re) {
			throw re;
		}
		catch(Exception e) {
			throw new IllegalStateException("Failed to check object store.", e);
		}
	}

	@Override
	public boolean remove(String path, Serializable key) {
		var id = ClusterID.createNext(UUID.randomUUID().toString());
		try {
			return acks.runWithAck(Request.Type.REMOVE_OBJECT, id, distributedScheduledExecutor.clusterSize(), () -> {
				distributedScheduledExecutor.sendRequest(null, new Request(Type.REMOVE_OBJECT, id, new StorePayload(path, key)));
			}).stream().map(s -> (Boolean)s).filter(t -> t).findFirst().orElse(false);
		}
		catch(RuntimeException re) {
			throw re;
		}
		catch(Exception e) {
			throw new IllegalStateException("Failed to remove from object store.", e);
		}
	}

	@Override
	public int size(String path) {
		var id = ClusterID.createNext(UUID.randomUUID().toString());
		try {
			return acks.runWithAck(Request.Type.OBJECT_COUNT, id, distributedScheduledExecutor.clusterSize(), () -> {
				distributedScheduledExecutor.sendRequest(null, new Request(Type.OBJECT_COUNT, id, new StorePayload(path, "")));
			}).stream().mapToInt(s -> (Integer)s).max().orElse(0);
		}
		catch(RuntimeException re) {
			throw re;
		}
		catch(Exception e) {
			throw new IllegalStateException("Failed to check object store.", e);
		}
	}
}
