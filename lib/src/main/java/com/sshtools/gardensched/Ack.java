package com.sshtools.gardensched;

import java.time.Duration;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class Ack {
	
	public interface ThrowingRunnable {
		void run() throws Exception;
	}
	
	private final static Logger LOG = LoggerFactory.getLogger(Ack.class);

	private final Map<Request.Type, Map<ClusterID, Semaphore>> acks = new HashMap<>();

	private final Duration acknowledgeTimeout;
	
	public Ack(Duration acknowledgeTimeout) {
		this.acknowledgeTimeout = acknowledgeTimeout;
	}
	
	public void ack(Request.Type type, ClusterID id) {
		if(LOG.isDebugEnabled()) {
			LOG.debug("Received {} ack for {}", type, id);
		}
		
		Semaphore sem;
		
		synchronized(acks) {
			var ackmap = acks.get(type);
			if(ackmap == null) {
				LOG.warn("Not expecting any {} acknowledgements.", type);
				return;
			}
			else {
				sem = ackmap.get(id);
			}
			
		}
		
		if(sem == null) {
			LOG.warn("Timed out waiting for {} ack for {}.", type, id);
		}
		else {
			sem.release();
			if(LOG.isDebugEnabled()) {
				LOG.debug("Released 1 semaphore {} for {}", type, id);
			}
		}
	}
	
	public void runWithAck(Request.Type type, ClusterID id, int size, ThrowingRunnable task) throws Exception {
		var sem = new Semaphore(size);
		try {
			sem.acquire(size);
			putAck(type, id, sem);
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
		} finally {
			removeAck(type, id);
		}
	}

	private void  removeAck(Request.Type type, ClusterID id) {

		synchronized(acks) {
			var ackmap = acks.get(type);
			if(ackmap != null) {
				ackmap.remove(id);
				if(ackmap.isEmpty()) {
					acks.remove(type);
				}
			}
		}
	}
	
	private void  putAck(Request.Type type, ClusterID id, Semaphore sem) {
		synchronized(acks) {
			var ackmap = acks.get(type);
			if(ackmap == null) {
				ackmap = new ConcurrentHashMap<>();
				acks.put(type, ackmap);
			}
			ackmap.put(id, sem);
		}
	}
}
