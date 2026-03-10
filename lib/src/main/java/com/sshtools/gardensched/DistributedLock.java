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

import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import org.jgroups.Address;
import org.jgroups.Message;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.sshtools.gardensched.Request.LockPayload;
import com.sshtools.gardensched.Request.Type;

/**
 * Provides a simplistic distributed lock implementation. It is not reentrant, and does not support
 * any kind of fairness. It is also not designed to be performant, but rather to be correct and simple.
 */
public class DistributedLock implements LockProvider, DistributedComponent, MessageReceiver {

	private final static Logger LOG = LoggerFactory.getLogger(DistributedLock.class);

	private final DistributedMachine machine;
	private final Map<String, Address> locks = new ConcurrentHashMap<>();
	private final Map<String, Semaphore> acks = new ConcurrentHashMap<>();
	private final Map<String, AtomicInteger> ackCounts = new ConcurrentHashMap<>();

	public DistributedLock(DistributedMachine machine) {
		this.machine = machine;
		machine.addComponent(this);
		machine.addReceiver(this);
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
	public List<Type> types() {
		return List.of(Type.LOCK, Type.UNLOCK, Type.LOCKED, Type.UNLOCKED);
	}

	@Override
	public void receive(Message msg, Request req) {
		switch (req.type()) {
		case LOCK:
			handleLock(req);
			break;
		case LOCKED:
			handleLocked(req);
			break;
		case UNLOCK:
			handleUnlock(req);
			break;
		case UNLOCKED:
			handleUnlocked(req);
			break;
		default:
			throw new IllegalArgumentException("Type " + req.type() + " is not recognized");
		}

	}

	@Override
	public Lock acquireLock(String name) throws InterruptedException, IllegalStateException {

		var sem = new Semaphore(machine.clusterSize());
		var ackCount = new AtomicInteger();

		try {

			synchronized (locks) {
				if (locks.containsKey(name))
					throw new IllegalStateException(name + " is locked.");

				locks.put(name, machine.address());

				sem.acquire(machine.clusterSize());
				acks.put(name, sem);
				ackCounts.put(name, ackCount);
				machine.sendRequest(null, new Request(Request.Type.LOCK, new LockPayload(name, machine.address())));
			}

			/* Wait for everyone to acknowledge via LOCKED */
			var requireAck = machine.clusterSize();
			if (!sem.tryAcquire(machine.clusterSize(), machine.acknowledgeTimeout().toMillis(),
					TimeUnit.MILLISECONDS)) {
				requireAck = machine.clusterSize() - sem.availablePermits();
				LOG.warn("{} nodes did not reply, perhaps a node went down, ignoring.", requireAck);
			}
			if (ackCount.get() != requireAck) {
				throw new IllegalStateException(name + " is locked.");
			}
		} finally {
			acks.remove(name);
			ackCounts.remove(name);
		}

		return new Lock() {
			@Override
			public void close() {
				try {
					LOG.debug("{} releasing lock {}", machine.address(), name);
					var sem = new Semaphore(machine.clusterSize());
					sem.acquire(machine.clusterSize());
					acks.put(name, sem);

					machine.sendRequest(null,
							new Request(Request.Type.UNLOCK, new LockPayload(name, machine.address())));

					/* Wait for everyone to acknowledge via UNLOCKED */
					sem.tryAcquire(machine.clusterSize(), machine.acknowledgeTimeout().toMillis(),
							TimeUnit.MILLISECONDS);
				} catch (RuntimeException re) {
					throw re;
				} catch (InterruptedException e) {
					throw new IllegalStateException(e);
				} finally {
					acks.remove(name);
				}
			}
		};
	}

	private void handleUnlock(Request req) {
		LockPayload lock = req.payload();

		if (LOG.isDebugEnabled()) {
			LOG.debug("Removing lock {}", lock.lockName());
		}
		locks.remove(lock.lockName());
		machine.sendRequest(lock.locker(),
				new Request(Request.Type.UNLOCKED, new LockPayload(lock.lockName(), lock.locker())));
	}

	private void handleUnlocked(Request req) {
		LockPayload lock = req.payload();
		if (LOG.isDebugEnabled()) {
			LOG.debug("Received UNLOCK ack for {} as {}", lock.lockName(), lock.locker());
		}
		acks.get(lock.lockName()).release();
	}

	private void handleLock(Request req) {
		LockPayload lock = req.payload();
		if (LOG.isDebugEnabled()) {
			LOG.debug("Adding lock {} as {}", lock.lockName(), lock.locker());
		}
		synchronized (locks) {
			var locker = lock.locker();
			if (locks.containsKey(lock.lockName())) {
				if (LOG.isDebugEnabled()) {
					LOG.debug("I ({}) already have lock {}", machine.address(), lock.lockName());
				}
				locker = machine.address();
			} else {
				locks.put(lock.lockName(), lock.locker());
			}
			machine.sendRequest(lock.locker(),
					new Request(Request.Type.LOCKED, new LockPayload(lock.lockName(), locker)));
		}
	}

	private void handleLocked(Request req) {
		LockPayload lock = req.payload();
		if (LOG.isDebugEnabled()) {
			LOG.debug("Received LOCKED ack for {} as {}", lock.lockName(), lock.locker());
		}
		if (lock.locker().equals(machine.address())) {
			if (LOG.isDebugEnabled()) {
				LOG.debug("It was agreed I can lock {}", lock.lockName());
			}
			ackCounts.get(lock.lockName()).addAndGet(1);
		} else {
			if (LOG.isDebugEnabled()) {
				LOG.debug("{} claimed they own the lock {}", lock.locker(), lock.lockName());
			}
		}
		acks.get(lock.lockName()).release();
	}

	@Override
	public void handleLeftMember(Address mbr) {
		// TODO handle the case where a member leaves while holding a lock, or while we are waiting for an ack from them. For now, we just ignore it and hope the timeout handles it.
		
	}

	@Override
	public void handleNewMember(int rank, Address mbr) {
		// TODO handle the case where a new member joins while we are waiting for acks, or while they are waiting for acks from us. For now, we just ignore it and hope the timeout handles it.
		
	}
}
