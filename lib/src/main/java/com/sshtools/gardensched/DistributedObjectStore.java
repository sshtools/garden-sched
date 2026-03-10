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
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import org.jgroups.Address;
import org.jgroups.Message;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.sshtools.gardensched.Request.AckPayload;
import com.sshtools.gardensched.Request.StorePayload;
import com.sshtools.gardensched.Request.Type;

public class DistributedObjectStore implements ObjectStore, DistributedComponent, MessageReceiver {
	
	public final static class Builder {
		private final ObjectStore objectStore;
		private boolean checkLocalObjectStorageFirst = true;
		private boolean storeToLocalUponRemoteRetrieval = true;
		private DistributedMachine machine;
		
		public Builder(DistributedMachine machine, ObjectStore objectStore) {
			this.objectStore = objectStore;
			this.machine = machine;
		}

		public DistributedObjectStore build() throws Exception {
			return new DistributedObjectStore(this);
		}
		
		public Builder withoutCheckLocalObjectStorageFirst() {
			return withCheckLocalObjectStorageFirst(false);
		}
		
		public Builder withCheckLocalObjectStorageFirst(boolean checkLocalObjectStorageFirst) {
			this.checkLocalObjectStorageFirst = checkLocalObjectStorageFirst;
			return this;
		}

		public Builder withStoreToLocalUponRemoteRetrieval(boolean storeToLocalUponRemoteRetrieval) {
			this.storeToLocalUponRemoteRetrieval = storeToLocalUponRemoteRetrieval;
			return this;
		}

	}

	private final static Logger LOG = LoggerFactory.getLogger(DistributedObjectStore.class);
	private final ObjectStore objectStore;
	private final boolean checkLocalObjectStorageFirst;
	private final boolean storeToLocalUponRemoteRetrieval;
	private final Ack acks;
	private final DistributedMachine machine;

	private DistributedObjectStore(Builder builder) {
		this.objectStore = builder.objectStore;
		this.machine = builder.machine;
		this.acks = machine.acks();
		this.checkLocalObjectStorageFirst = builder.checkLocalObjectStorageFirst;
		this.storeToLocalUponRemoteRetrieval = builder.storeToLocalUponRemoteRetrieval;
		
		machine.addComponent(this);
		machine.addReceiver(this);
	}

	@Override
	public void put(String path, Serializable key, Serializable value) {
		objectStore.put(path, key, value);
		machine.sendRequest(null, new Request(Type.STORED_OBJECT, new StorePayload(path, key)));
	}

	@SuppressWarnings("unchecked")
	@Override
	public <T extends Serializable> Set<T> keySet(String path) {
		var id = ClusterID.createNext(UUID.randomUUID().toString());
		try {
			return acks.runWithAck(Request.Type.KEYS, id, machine.clusterSize(), () -> {
				machine.sendRequest(null, new Request(Type.KEYS, id, new StorePayload(path, "")));
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

		if(checkLocalObjectStorageFirst && objectStore.has(path, key)) {

			if (LOG.isDebugEnabled()) {
				LOG.debug("Get called for {} in {} and it exists in the local store, using  that.", path, key);
			}

			return (T)objectStore.get(path, key);
		}

		var id = ClusterID.createNext(UUID.randomUUID().toString());
		try {
			T res = (T)acks.runWithAck(Request.Type.GET_OBJECT, id, machine.clusterSize(), () -> {
				machine.sendRequest(null, new Request(Type.GET_OBJECT, id, new StorePayload(path, key)));
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
					objectStore.put(path, key, res);
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
		if(checkLocalObjectStorageFirst && objectStore.has(path, key)) {
			return true;
		}

		var id = ClusterID.createNext(UUID.randomUUID().toString());
		try {
			return acks.runWithAck(Request.Type.HAS_OBJECT, id, machine.clusterSize(), () -> {
				machine.sendRequest(null, new Request(Type.HAS_OBJECT, id, new StorePayload(path, key)));
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
			return acks.runWithAck(Request.Type.REMOVE_OBJECT, id, machine.clusterSize(), () -> {
				machine.sendRequest(null, new Request(Type.REMOVE_OBJECT, id, new StorePayload(path, key)));
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
			return acks.runWithAck(Request.Type.OBJECT_COUNT, id, machine.clusterSize(), () -> {
				machine.sendRequest(null, new Request(Type.OBJECT_COUNT, id, new StorePayload(path, "")));
			}).stream().mapToInt(s -> (Integer)s).max().orElse(0);
		}
		catch(RuntimeException re) {
			throw re;
		}
		catch(Exception e) {
			throw new IllegalStateException("Failed to check object store.", e);
		}
	}

	@Override
	public List<Type> types() {
		return List.of(
				Type.KEYS,
				Type.GET_OBJECT,
				Type.HAS_OBJECT,
				Type.REMOVE_OBJECT,
				Type.OBJECT_COUNT,
				Type.STORED_OBJECT
			);
	}

	@Override
	public void receive(Message msg, Request req) {
		switch(req.type()) {
		case KEYS:
			handleKeys(msg.getSrc(), req);
			break;
		case GET_OBJECT:
			handleGetObject(msg.getSrc(), req);
			break;
		case HAS_OBJECT:
			handleHasObject(msg.getSrc(), req);
			break;
		case OBJECT_COUNT:
			handleObjectCount(msg.getSrc(), req);
			break;
		case REMOVE_OBJECT:
			handleRemoveObject(msg.getSrc(), req);
			break;
		case STORED_OBJECT:
			handleStore(msg.getSrc(), req);
			break;
		default:
			throw new IllegalStateException("Unsupported request type: " + req.type());
		}
	}

	@Override
	public void prepareClose() {
	}

	@Override
	public void close() {
		machine.removeComponent(this);
		machine.removeReceiver(this);
	}

	@Override
	public void forceClose() {
	}

	@Override
	public boolean awaitTermination(long millis, TimeUnit milliseconds) throws InterruptedException {
		return true;
	}

	@Override
	public void handleLeftMember(Address mbr) {
	}

	@Override
	public void handleNewMember(int wasRank, Address mbr) {
	}

	private void handleHasObject(Address sender, Request req) {
		StorePayload store = req.payload();
		if(LOG.isDebugEnabled()) {
			LOG.debug("Received HAS_OBJECT from {} for {} / {}", sender, store.path(), store.key());
		}
		machine.sendRequest(sender, new Request(Request.Type.ACK, req.id(), new AckPayload(Request.Type.HAS_OBJECT, objectStore.has(store.path(), store.key()))));
	}

	private void handleObjectCount(Address sender, Request req) {
		StorePayload store = req.payload();
		if(LOG.isDebugEnabled()) {
			LOG.debug("Received OBJECT_COUNT from {} for {}", sender, store.path());
		}
		machine.sendRequest(sender, new Request(Request.Type.ACK, req.id(), new AckPayload(Request.Type.OBJECT_COUNT, objectStore.size(store.path()))));
	}

	private void handleGetObject(Address sender, Request req) {
		StorePayload store = req.payload();
		if(LOG.isDebugEnabled()) {
			LOG.debug("Received GET_OBJECT from {} for {}", sender, req.id());
		}
		machine.sendRequest(sender, new Request(
				Request.Type.ACK, 
				req.id(), 
				new AckPayload(Request.Type.GET_OBJECT, objectStore.get(store.path(), store.key()))
			)
		);
	}

	private void handleKeys(Address sender, Request req) {
		StorePayload store = req.payload();
		if(LOG.isDebugEnabled()) {
			LOG.debug("Received KEYS from {} for {}", sender, req.id());
		}
		machine.sendRequest(sender, new Request(
				Request.Type.ACK, 
				req.id(), 
				new AckPayload(Request.Type.KEYS, (Serializable)new LinkedHashSet<>(objectStore.keySet(store.path())))
			)
		);
	}

	private void handleRemoveObject(Address sender, Request req) {
		StorePayload store = req.payload();
		if(LOG.isDebugEnabled()) {
			LOG.debug("Received REMOVE_OBJECT from {} for {}", sender, req.id());
		}
		var removed = objectStore.remove(store.path(), store.key()); 
		if(removed) {
			if(LOG.isDebugEnabled()) {
				LOG.debug("Removed object {} from store at path {}", store.key(), store.path());
			}		
		}
		machine.sendRequest(sender, new Request(Request.Type.ACK, req.id(), new AckPayload(Request.Type.REMOVE_OBJECT, removed)));
	}

	private void handleStore(Address sender, Request req) {
		if(sender.equals(machine.address()))
			return;
			
		StorePayload store = req.payload();
		if(LOG.isDebugEnabled()) {
			LOG.debug("Received STORED_OBJECT from {} for {}", sender, req.id());
		}
		
		if(objectStore.remove(store.path(), store.key())) {
			if(LOG.isDebugEnabled()) {
				LOG.debug("Removed object {} from store at path {}", store.key(), store.path());
			}		
		}
	}
}
