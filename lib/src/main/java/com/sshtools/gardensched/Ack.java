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
import java.time.Duration;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class Ack {
	
	private record AckEntry(Semaphore semaphore, List<Serializable> results) {}
	
	public interface ThrowingRunnable {
		void run() throws Exception;
	}
	
	private final static Logger LOG = LoggerFactory.getLogger(Ack.class);

	private final Map<Request.Type, Map<ClusterID, AckEntry>> acks = new HashMap<>();

	private final Duration acknowledgeTimeout;
	
	public Ack(Duration acknowledgeTimeout) {
		this.acknowledgeTimeout = acknowledgeTimeout;
	}
	
	public void ack(Request.Type type, ClusterID id, Serializable response) {
		if(LOG.isDebugEnabled()) {
			LOG.debug("Received {} ack for {}", type, id);
		}
		
		AckEntry entry;
		
		synchronized(acks) {
			var ackmap = acks.get(type);
			if(ackmap == null) {
				LOG.warn("Not expecting any {} acknowledgements.", type);
				return;
			}
			else {
				entry = ackmap.get(id);
			}
			
		}
		
		if(entry == null) {
			LOG.warn("Timed out waiting for {} ack for {}.", type, id);
		}
		else {
			if(response != null)
				entry.results.add(response);
			entry.semaphore.release();
			if(LOG.isDebugEnabled()) {
				LOG.debug("Released 1 semaphore {} for {}", type, id);
			}
		}
	}
	
	public List<Serializable> runWithAck(Request.Type type, ClusterID id, int size, ThrowingRunnable task) throws Exception {
		var sem = new Semaphore(size);
		try {
			sem.acquire(size);
			var entry = putAck(type, id, sem);
			task.run();
			
			/* Wait for everyone to acknowledge via REMOVED */
			if(LOG.isDebugEnabled()) {
				LOG.debug("Waiting for {} nodes to acknowledge {} for {}", size, type, id);
			}
			if (sem.tryAcquire(size, acknowledgeTimeout.toMillis(), TimeUnit.MILLISECONDS)) {
				if(LOG.isDebugEnabled()) {
					LOG.debug("All {} nodes acknowledged {} for {}", size, type, id);
				}
			}
			else {
				LOG.warn("{} nodes did not reply, perhaps a node went down, ignoring.",  size - sem.availablePermits());
			}
			
			return entry.results;
		} finally {
			removeAck(type, id);
		}
	}

	private void  removeAck(Request.Type type, ClusterID id) {

		synchronized(acks) {
			if(LOG.isDebugEnabled()) {
				LOG.debug("Removing {} Ack for {}", type, id);
			}
			var ackmap = acks.get(type);
			if(ackmap != null) {
				ackmap.remove(id);
				if(ackmap.isEmpty()) {
					acks.remove(type);
				}
			}
		}
	}
	
	private AckEntry  putAck(Request.Type type, ClusterID id, Semaphore sem) {
		synchronized(acks) {
			var ackmap = acks.get(type);
			if(ackmap == null) {
				ackmap = new ConcurrentHashMap<>();
				acks.put(type, ackmap);
			}
			var entry = new AckEntry(sem, new ArrayList<>());
			ackmap.put(id, entry);
			return entry;
		}
	}
}
