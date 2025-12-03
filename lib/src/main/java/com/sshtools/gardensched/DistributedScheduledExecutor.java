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

import java.io.IOException;
import java.io.Serializable;
import java.io.UncheckedIOException;
import java.text.MessageFormat;
import java.time.Duration;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.Delayed;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Stream;

import org.jgroups.Address;
import org.jgroups.BytesMessage;
import org.jgroups.JChannel;
import org.jgroups.Message;
import org.jgroups.Receiver;
import org.jgroups.View;
import org.jgroups.util.Util;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public final class DistributedScheduledExecutor implements ScheduledExecutorService {
	
	public final static class Builder {
		private int schedulerThreads = 1;
		private String props = "udp.xml";
		private String clusterName = "garden-sched";
		private String groupName = null;
		private Optional<LockProvider> lockProvider = Optional.empty();
		private Optional<PayloadSerializer> payloadSerializer = Optional.empty();
		private Optional<PayloadFilter> payloadFilter = Optional.empty();
		private boolean alwaysDistribute = false;
		private boolean startPaused = false;
		private Duration acknowledgeTimeout = Duration.ofSeconds(10);
		private Optional<TaskErrorHandler> taskErrorHandler = Optional.empty();
		private Optional<TaskSuccessHandler> taskSuccessHandler = Optional.empty();
		private Duration closeTimeout = Duration.of(1, ChronoUnit.DAYS);
		private Optional<TaskStore> taskStore = Optional.empty();
		private boolean persistentByDefault = false;
		private boolean restoreTasksAtStartup = true;
		private boolean deferStorageUntilStarted = false;

		public DistributedScheduledExecutor build() throws Exception {
			return new DistributedScheduledExecutor(this);
		}

		public Builder withAcknowledgeTimeout(Duration acknowledgeTimeout) {
			this.acknowledgeTimeout = acknowledgeTimeout;
			return this;
		}
		
		public Builder withoutRestoreTasksAtStartup() {
			return withRestoreTasksAtStartup(false);
		}
		
		public Builder withRestoreTasksAtStartup(boolean restoreTasksAtStartup) {
			this.restoreTasksAtStartup = restoreTasksAtStartup;
			return this;
		}
		
		public Builder withDeferStorageUntilStarted() {
			return withDeferStorageUntilStarted(true);
		}

		
		public Builder withDeferStorageUntilStarted(boolean deferStorageUntilStarted) {
			this.deferStorageUntilStarted = deferStorageUntilStarted;
			return this;
		}
		
		public Builder withAlwaysDistribute() {
			return withAlwaysDistribute(true);
		}

		
		public Builder withAlwaysDistribute(boolean alwaysDistribute) {
			this.alwaysDistribute = alwaysDistribute;
			return this;
		}
		
		public Builder withCloseTimeout(Duration closeTimeout) {
			this.closeTimeout = closeTimeout;
			return this;
		}
		
		public Builder withClusterName(String clusterName) {
			this.clusterName = clusterName;
			return this;
		}
		
		public Builder withGroupName(String groupName) {
			this.groupName = groupName;
			return this;
		}
		
		public Builder withJGroupsProps(LockProvider lockProvider) {
			this.lockProvider = Optional.of(lockProvider);
			return this;
		}
		
		public Builder withJGroupsProps(String props) {
			this.props = props;
			return this;
		}
		
		public Builder withPayloadFilter(PayloadFilter payloadFilter) {
			this.payloadFilter = Optional.of(payloadFilter);
			return this;
		}

		public Builder withPayloadSerializer(PayloadSerializer payloadSerializer) {
			this.payloadSerializer = Optional.of(payloadSerializer);
			return this;
		}

		public Builder withPersistentByDefault() {
			return withPersistentByDefault(true);
		}

		public Builder withPersistentByDefault(boolean persistentByDefault) {
			this.persistentByDefault = persistentByDefault;
			return this;
		}

		public Builder withSchedulerThreads(int schedulerThreads) {
			this.schedulerThreads = schedulerThreads;
			return this;
		}

		public Builder withStartPaused() {
			return withStartPaused(true);
		}

		public Builder withStartPaused(boolean startPaused) {
			this.startPaused = startPaused;
			return this;
		}

		public Builder withTaskErrorHandler(TaskErrorHandler  taskErrorHandler) {
			this.taskErrorHandler = Optional.of(taskErrorHandler);
			return this;
		}

		public Builder withTaskStore(TaskStore taskStore) {
			this.taskStore = Optional.of(taskStore);
			return this;
		}

		public Builder withTaskSuccessHandler(TaskSuccessHandler  taskSuccessHandler) {
			this.taskSuccessHandler = Optional.of(taskSuccessHandler);
			return this;
		}

	}

	private abstract class AbstractHandler<TSK> implements Runnable {
		final ClusterID id;
		final TSK task;
		final TaskSpec taskSpec;

		public AbstractHandler(ClusterID id, TSK task, TaskSpec taskSpec) {
			this.id = id;
			this.task = task;
			this.taskSpec = taskSpec;
		}

		@Override
		public void run() {
			synchronized(taskInfo) {
				taskInfo.get(id).lastExecuted = Optional.of(Instant.now());
			}
			
			var retry = new AtomicLong(Integer.MIN_VALUE);
			var cancel = new AtomicBoolean();
			try {
				
				TaskContext.ctx.set(new TaskContext() {
					
					private final TaskProgress progress = new TaskProgress() {
						
						@Override
						public void start(long max) {
							// TODO Auto-generated method stub
							
						}
						
						@Override
						public void progress(long value) {
							// TODO Auto-generated method stub
						}
						
						@Override
						public void message(String bundle, String key, Object... args) {
							// TODO Auto-generated method stub
						}
						
						@Override
						public void message(String text) {
							// TODO Auto-generated method stub
						}
					};
					
					@Override
					public TaskProgress progress() {
						return progress;
					}
					
					@Override
					public ClusterID id() {
						return id;
					}
					
					@Override
					public void cancel() {
						/* If this is TRIGGER type, only cancels this run */
						DistributedScheduledExecutor.this.future(id).cancel(true);
					}
					
					@Override
					public Address address() {
						return DistributedScheduledExecutor.this.address();
					}
				});
				
				doTask(new TaskCompletionContext() {
					
					@Override
					public void cancel() {
						cancel.set(true);
					}
					
					@Override
					public void retry(Duration delay) {
						retry.set(delay.toMillis());
					}
				});
			}
			finally {
				TaskContext.ctx.remove();
				synchronized(taskInfo) {
					taskInfo.get(id).lastCompleted = Optional.of(Instant.now());
				}
				
				if(cancel.get()) {
					LOG.info("Cancelling {} because error handling requested it.", id);
					noRetrigger();
					future(id).cancel(false);
				}
				else if(retry.get() != Integer.MIN_VALUE) {
					LOG.info("Retrying {} in {} ms because error handling requested it.", id, retry.get());
					
					throw new UnsupportedOperationException("Retrying is not yet supported.");
				}
				else {
					if(taskSpec.schedule() == Schedule.TRIGGER) {
						LOG.debug("{} is a TRIGGER task, so scheduling again.",  id);
						retrigger();
					}
					else {
						noRetrigger();
					}
				}
			}

		}
		
		protected Object doTask(TaskCompletionContext taskCompletionContext) {
			Object result = null;
			try {
				if(LOG.isDebugEnabled()) {
					LOG.debug("Executing {}", id);
				}
				if(task instanceof Callable dcl)
					result = dcl.call();
				else if(task instanceof Runnable dcr) {
					dcr.run();
				}
				else {
					throw new IllegalArgumentException("Unknown task type.");
				}

				synchronized(taskInfo) {
					Optional.ofNullable(taskInfo.get(id)).ifPresent(ti -> ti.lastError = Optional.empty());
				} 
				taskSuccessHandler.ifPresent(te -> {
					te.handleSuccess(id, taskSpec, (Serializable) task, taskCompletionContext);
				});
			} catch (Throwable t) {
				LOG.error("Failed executing {}", id, t);
				result = t;

				synchronized(taskInfo) {
					Optional.ofNullable(taskInfo.get(id)).ifPresent(ti -> ti.lastError = Optional.of(t));
				}
				
				taskErrorHandler.ifPresent(te -> {
					te.handleError(id, taskSpec, (Serializable) task, taskCompletionContext, t);
				});
			}
			return result;
		}
		
		protected void noRetrigger() {
		}

		abstract void retrigger();
	}

	private final class DefaultLockProvider implements LockProvider {

		private Map<String, Address> locks = new ConcurrentHashMap<>();
		private Map<String, Semaphore> acks = new ConcurrentHashMap<>();
		private Map<String, AtomicInteger> ackCounts = new ConcurrentHashMap<>();

		@Override
		public Lock acquireLock(String name) throws InterruptedException, IllegalStateException {

			var sem = new Semaphore(clusterSize);
			var ackCount = new AtomicInteger();
			
			try {

				synchronized (locks) {
					if (locks.containsKey(name))
						throw new IllegalStateException(name + " is locked.");
	
					locks.put(name, ch.getAddress());
	
					sem.acquire(clusterSize);
					acks.put(name, sem);
					ackCounts.put(name, ackCount);
					sendRequest(null, new Request(Request.Type.LOCK, null, null, null, null, ch.getAddress(), name, null));
				}
	
				/* Wait for everyone to acknowledge via LOCKED */
				var requireAck = clusterSize;
				if (!sem.tryAcquire(clusterSize, acknowledgeTimeout.toMillis(), TimeUnit.MILLISECONDS)) {
					requireAck = clusterSize - sem.availablePermits();
					LOG.warn("{} nodes did not reply, perhaps a node went down, ignoring.", requireAck);
				}
				if (ackCount.get() != requireAck) {
					throw new IllegalStateException(name + " is locked.");
				}
			}
			finally {
				acks.remove(name);
				ackCounts.remove(name);
			}

			return new Lock() {
				@Override
				public void close() {
					try {
						LOG.debug("{} releasing lock {}", ch.getAddress(), name);
						var sem = new Semaphore(clusterSize);
						sem.acquire(clusterSize);
						acks.put(name, sem);

						sendRequest(null, new Request(Request.Type.UNLOCK, null, null, null, null, ch.getAddress(), name, null));

						/* Wait for everyone to acknowledge via UNLOCKED */
						sem.tryAcquire(clusterSize, acknowledgeTimeout.toMillis(), TimeUnit.MILLISECONDS);
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

	}
	
	private final class EntryFuture<T> implements Future<T> {
		private final ClusterID id;
		private final TaskEntry entry;
		private boolean cancelled;

		private EntryFuture(ClusterID id, TaskEntry entry) {
			this.id = id;
			this.entry = entry;
		}

		@Override
		public boolean cancel(boolean mayInterruptIfRunning) {
			if (!cancelled) {
				LOG.info("Cancelling future {}", id);
				try {
					removeRequest(id);
					return true;
				} finally {
					cancelled = true;
				}
			}
			return false;
		}

		@SuppressWarnings("unchecked")
		@Override
		public T get() throws InterruptedException, ExecutionException {
			return (T) entry.promise.getResult();
		}

		@SuppressWarnings("unchecked")
		@Override
		public T get(long timeout, TimeUnit unit) throws InterruptedException, ExecutionException, TimeoutException {
			return (T) entry.promise.getResult(timeout);
		}

		@Override
		public boolean isCancelled() {
			return cancelled;
		}

		@Override
		public boolean isDone() {
			return entry.promise.hasResult();
		}
	}
	
	private class LocalHandler<RESULT> extends AbstractHandler<Serializable> implements Runnable {

		public LocalHandler(ClusterID id, Serializable task, TaskSpec taskSpec) {
			super(id, task, taskSpec);
		}

		@Override
		protected void noRetrigger() {
			taskInfo.remove(id);
		}

		@Override
		void retrigger() {
			submitLocal(id, taskSpec, this);
		}
	}
	
	private class RemoteHandler  extends AbstractHandler<DistributedTask<?>> implements Runnable {
		final TaskEntry entry;

		public RemoteHandler(TaskEntry entry) {
			super(entry.id, entry.task, entry.spec);
			this.entry = entry;
		}

		@Override
		public void run() {
			if(LOG.isDebugEnabled()) {
				LOG.debug("Handling {} [{}]", id, task.affinity());
			}
			super.run();
		}

		@Override
		protected Object doTask(TaskCompletionContext taskErrorContext) {
			Object result =  null;
			switch (task.affinity()) {
			case ALL:
			case MEMBER:
				result = super.doTask(taskErrorContext);
				break;
			case THIS:
				if (entry.submitter.equals(ch.getAddress())) {
					if(LOG.isDebugEnabled()) {
						LOG.debug(
								"Running task {} [{}] on this node because its affinity is {} and it was scheduled on this node.",
								id, task.id(), task.affinity());
					}
					result = super.doTask(taskErrorContext);
				}
				else {
					return null;
				}
				break;
			case ANY:
				if (id.getId() % clusterSize == rank) {
					if(LOG.isDebugEnabled()) {
						LOG.debug(
								"Running task {} [{}] on this node because its affinity is {} and it's rank of {} matches.",
								id, task.id(), task.affinity(), rank);
					}
					result = super.doTask(taskErrorContext);
				} else {
					if(LOG.isDebugEnabled()) {
						LOG.debug(
								"Not running task {} [{}] on this node because its affinity is {} and it's rank of {} does not match.",
								id, task.id(), task.affinity(), rank);
					}
					return null;
				}
				break;
			default:
				throw new IllegalStateException("Unexpected affinity " + task.affinity());
			}
			sendRequest(entry.submitter, new Request(Request.Type.RESULT, null, id, result, DEFAULT_TASK, null, null, null));
			return result;
		}

		@Override
		void retrigger() {
			execute(entry);
		}
	}

	private class SchedReceiver implements Receiver {

		@Override
		public void receive(Message msg) {
			srlzr.set(payloadSerializer);
			fltr.set(payloadFilter);
			try {
				Request req = Util.streamableFromByteBuffer(Request.class, ((BytesMessage) msg).getBytes(),
						msg.getOffset(), msg.getLength());
				switch (req.type()) {
				case EVENT:
					handleEvent(msg.getSrc(), req);
					break;
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
				case REMOVED:
					handleRemoved(msg.getSrc(), req);
					break;
				case EXECUTE:
					handleExecute(req.id(), msg.getSrc(), req.task(), req.spec());
					break;
				case EXECUTED:
					handleExecuted(req);
					break;
				case RESULT:
					var entry = tasks.get(req.id());

					if (entry == null) {
						LOG.error("Found no entry for request {}", req.id());
						queue.execute(() -> removeRequest(req.id()));
					} else {
						var responsesLeft = entry.results.addAndGet(-1);
						if (responsesLeft == 0) {
							if (LOG.isDebugEnabled()) {
								LOG.debug("No more responses to {} expected, completing.", req.id());
							}
							entry.promise.setResult(req.result());

							queue.execute(() -> removeRequest(req.id()));
						} else {
							if (LOG.isDebugEnabled()) {
								LOG.debug("Waiting for {} more responses to {}.", responsesLeft, req.id());
							}
						}
					}
					break;
				case REMOVE:
					handleRemove(msg.getSrc(), req);
					break;
				default:
					throw new IllegalArgumentException("Type " + req.type() + " is not recognized");
				}
			} catch (Exception e) {
				LOG.error("Exception receiving message from {}", msg.getSrc(), e);
			} finally {
				srlzr.remove();
				fltr.remove();
			}
		}

		@Override
		public void viewAccepted(View view) {
			acceptView(view);
		}
	}

	private final static Logger LOG = LoggerFactory.getLogger(DistributedScheduledExecutor.class);
	
	private static TaskSpec DEFAULT_TASK = new TaskSpec(Instant.EPOCH, Schedule.NOW, 0, 0, TimeUnit.MILLISECONDS, null);

	private static final ThreadLocal<PayloadSerializer> srlzr = new ThreadLocal<>();
	private static final ThreadLocal<PayloadFilter> fltr = new ThreadLocal<>();
	
	public static PayloadFilter currentFilter() {
		return fltr.get();
	}
	
	public static PayloadSerializer currentSerializer() {
		return srlzr.get();
	}
	
	private final PayloadSerializer payloadSerializer;
	private final JChannel ch;
	private final ScheduledExecutorService delegate;
	private final ConcurrentMap<ClusterID, TaskEntry> tasks = new ConcurrentHashMap<>();
	private final LockProvider lockProvider;
	private final Map<ClusterID, Semaphore> removeAcks = new ConcurrentHashMap<>();
	private final Map<ClusterID, Semaphore> executeAcks = new ConcurrentHashMap<>();
	private final ConcurrentMap<ClusterID, TaskInfo> taskInfo = new ConcurrentHashMap<>();
	private final ConcurrentMap<ClusterID, IdentifiableFuture<?>> returnedFutures = new ConcurrentHashMap<>();
	private final ExecutorService queue;
	private final boolean alwaysDistribute;
	private final Duration acknowledgeTimeout;
	private final Optional<TaskErrorHandler> taskErrorHandler;
	private final Optional<TaskSuccessHandler> taskSuccessHandler;
	private final Optional<TaskStore> taskStore;
	private final List<NodeListener> listeners = new ArrayList<>();
	private final List<BroadcastEventListener> broadcastListeners = new ArrayList<>();
	private final Duration closeTimeout;
	private final boolean persistentByDefault;
	private final boolean deferStorageUntilStarted;
	private final List<TaskEntry> deferredStorage = new ArrayList<>();
	private final AtomicBoolean started = new AtomicBoolean();
	
	private View view;
	private int rank = -1;
	private int clusterSize = -1;
	private int threads;
	private final PayloadFilter payloadFilter;
	private boolean startPaused;
	private Semaphore sem;
	private boolean restoreTasksAtStartup;
	
	private DistributedScheduledExecutor(Builder bldr) throws Exception {
		payloadSerializer = bldr.payloadSerializer.orElseGet(PayloadSerializer::defaultSerializer);
		lockProvider = bldr.lockProvider.orElseGet(() -> new DefaultLockProvider());
		threads = bldr.schedulerThreads;
		delegate = Executors.newScheduledThreadPool(threads);
		startPaused = bldr.startPaused;
		acknowledgeTimeout = bldr.acknowledgeTimeout;
		taskErrorHandler = bldr.taskErrorHandler;
		taskSuccessHandler = bldr.taskSuccessHandler;
		closeTimeout = bldr.closeTimeout;
		taskStore = bldr.taskStore;
		persistentByDefault = bldr.persistentByDefault;
		queue = Executors.newSingleThreadExecutor();
		payloadFilter = bldr.payloadFilter.orElseGet(() -> PayloadFilter.nullFilter());
		alwaysDistribute = bldr.alwaysDistribute;
		restoreTasksAtStartup = bldr.restoreTasksAtStartup;
		taskStore.ifPresent(ts -> ts.initialise(this));
		deferStorageUntilStarted = bldr.deferStorageUntilStarted;
		
		ch = new JChannel(bldr.props);
		ch.name(bldr.groupName);
		ch.setReceiver(new SchedReceiver());
		ch.connect(bldr.clusterName);
		
		if(startPaused) {
			LOG.info("Starting paused (all {} threads locked).", threads);
			sem = new Semaphore(threads);
			sem.acquire(threads);
			for(var i = 0 ; i < threads; i++) {
				delegate.submit(() -> {
					try {
						sem.acquire();
					} catch (InterruptedException e) {
					}
					finally {
						sem.release();
					}
				});
			}
		}
		else {
			started.set(true);
			LOG.info("Starting with {} threads", threads);
			if(restoreTasksAtStartup) {
				taskStore.ifPresent(ts -> {
					LOG.info("Loading from task store");
					ts.entries().forEach(entry -> {
						if (shouldRun(entry)) {
							execute(entry);
						}
					});
				});
			}
		}
	}

	public void restore(TaskEntry entry) {
		if (!tasks.containsKey(entry.id) && shouldRun(entry)) {
			execute(entry);
		}
	}
	
	public void addBroadcastListener(BroadcastEventListener listener) {
		this.broadcastListeners.add(listener);
	}
	
	public void addListener(NodeListener listener) {
		this.listeners.add(listener);
	}
	
	public Address address() {
		return ch.getAddress();
	}
	
	@Override
	public boolean awaitTermination(long timeout, TimeUnit timeUnit) throws InterruptedException {
		return delegate.awaitTermination(timeout, timeUnit);
	}
	
	/**
	 * Note this is  in Java 19+ only, we want a shorter timeout
	 */
//	@Override
	public void close() {
		LOG.info("Closing distributed executor.");
        boolean terminated = isTerminated();
        if (!terminated) {

    		/* Cancel tasks on shutdown so only actually running tasks will remain subject
    		 * to the timeout */ 
    		LOG.info("Cancelling locally scheduled tasks.");
    		taskInfo.values().forEach(tsk -> tsk.future.cancel(false));
    		
            shutdown();
            boolean interrupted = false;
            try {
        		LOG.info("Awaiting termination ....");
                terminated = awaitTermination(closeTimeout.toMillis(), TimeUnit.MILLISECONDS);
            } catch (InterruptedException e) {
                if (!interrupted) {
            		LOG.info("Forcibly terminating");
                    shutdownNow();
                    interrupted = true;
                }
            }
            if (interrupted) {
                Thread.currentThread().interrupt();
            }
        }
	}
	
	public void event(Serializable event) {
		queue.execute(() -> {
			sendRequest(null, new Request(Request.Type.EVENT, null, ClusterID.createNext(ch.getAddress()), null, null, null, null, event));
		});
	}
	
	@Override
	public void execute(Runnable runnable) {
		submit(runnable);
	}
	
	@SuppressWarnings("unchecked")
	public <F extends IdentifiableFuture<?>> F future(ClusterID task) {
		return (F)returnedFutures.get(task);
	}

	public <F extends IdentifiableFuture<?>> Optional<F> futureOr(ClusterID task) {
		return Optional.ofNullable(future(task));
	}

	public Collection<IdentifiableFuture<?>> futures() {
		return Collections.unmodifiableCollection(returnedFutures.values());
	}

	@Override
	public <T> List<Future<T>> invokeAll(Collection<? extends Callable<T>> arg0) throws InterruptedException {
		throw new UnsupportedOperationException();
	}

	@Override
	public <T> List<Future<T>> invokeAll(Collection<? extends Callable<T>> arg0, long arg1, TimeUnit arg2)
			throws InterruptedException {
		throw new UnsupportedOperationException();
	}

	@Override
	public <T> T invokeAny(Collection<? extends Callable<T>> arg0) throws InterruptedException, ExecutionException {
		throw new UnsupportedOperationException();
	}

	@Override
	public <T> T invokeAny(Collection<? extends Callable<T>> arg0, long arg1, TimeUnit arg2)
			throws InterruptedException, ExecutionException, TimeoutException {
		throw new UnsupportedOperationException();
	}

	@Override
	public boolean isShutdown() {
		return delegate.isShutdown();
	}

	@Override
	public boolean isTerminated() {
		return delegate.isTerminated();
	}
	
	public LockProvider lockProvider() {
		return lockProvider;
	}
	
	public int rank() {
		return rank;
	}

	public void removeBroadcastListener(BroadcastEventListener listener) {
		this.broadcastListeners.remove(listener);
	}
	
	public void removeListener(NodeListener listener) {
		this.listeners.remove(listener);
	}

	@SuppressWarnings("unchecked")
	@Override
	public <V> ScheduledFuture<V> schedule(Callable<V> callable, long delay, TimeUnit unit) {
		
		if (checkTask(callable)) {

			var dtask = (DistributedCallable<?>) callable;
			
			var id = dtask.id().
					map(i -> ClusterID.createNext(i)).
					orElseGet(() -> ClusterID.createNext(ch.getAddress()));

			TaskEntry dfut = checkTaskId(id);
			if (dfut != null) {
				var afut = taskInfo.get(id).future;
				var rfut = new AbstractDelegateScheduledFuture<>(id, new EntryFuture<>(id, dfut), dtask.classifiers()) {

					@Override
					public int compareTo(Delayed other) {
						return ((ScheduledFuture<?>)afut).compareTo(other);
					}

					@Override
					public long getDelay(TimeUnit unit) {
						return ((ScheduledFuture<?>)afut).getDelay(unit);
					}
				};
				returnedFutures.put(id, rfut);
				return (ScheduledFuture<V>) rfut;
			}

			var ftr = doSubmit(id, dtask, new TaskSpec(Instant.now(), Schedule.ONE_SHOT, delay, delay, unit, null));
			var afut = taskInfo.get(id).future;
			var rfut = new AbstractDelegateScheduledFuture<>(id, (Future<V>)ftr, dtask.classifiers()) {

				@Override
				public int compareTo(Delayed other) {
					return ((ScheduledFuture<?>)afut).compareTo(other);
				}

				@Override
				public long getDelay(TimeUnit unit) {
					return ((ScheduledFuture<?>)afut).getDelay(unit);
				}

			};
			returnedFutures.put(id, rfut);
			return rfut;
		} else {
			if(alwaysDistribute) {
				if(callable instanceof SerializableCallable cr)
					return schedule(DistributedCallable.of(cr), delay, unit);
				else
					return (ScheduledFuture<V>) schedule(DistributedCallable.of(new SerializableCallable<Serializable>() {
						@Override
						public Serializable call() throws Exception {
							return (Serializable)callable.call();
						}
					}), delay, unit);
			}
			else {
				if (LOG.isDebugEnabled()) {
					LOG.debug("Not a {}, scheduling locally only.", DistributedCallable.class.getName());
				}
				
				var id = ClusterID.createNext(address());
				
				var sfut = delegate.schedule(() -> {
					try {
						return payloadFilter.filter(callable).call();
					}
					finally {
						returnedFutures.remove(id);
					}
				}, delay, unit);
				
				var dfut = new DelegatedScheduledFuture<>(id, sfut, Collections.emptySet()){

					@Override
					public boolean cancel(boolean mayInterrupt) {
						returnedFutures.remove(id);
						return super.cancel(mayInterrupt);
					}
					
				};
				returnedFutures.put(id, dfut);
				return dfut;
			}
		}

	}

	@Override
	public ScheduledFuture<?> schedule(Runnable command, long delay, TimeUnit unit) {
		return doRunnable(Schedule.ONE_SHOT, command, delay, delay, unit, null);
	}

	public ScheduledFuture<?> schedule(Runnable command, TaskTrigger trigger) {
		return doRunnable(Schedule.TRIGGER, command, 0, 0, TimeUnit.MILLISECONDS, trigger);
	}

	@Override
	public ScheduledFuture<?> scheduleAtFixedRate(Runnable command, long initialDelay, long period, TimeUnit unit) {
		return doRunnable(Schedule.FIXED_RATE, command, initialDelay, period, unit, null);
	}

	@Override
	public ScheduledFuture<?> scheduleWithFixedDelay(Runnable command, long initialDelay, long delay, TimeUnit unit) {
		return doRunnable(Schedule.FIXED_DELAY, command, initialDelay, delay, unit, null);
	}

	@Override
	public void shutdown() {
		try {
			LOG.info("Closing channel.");
			ch.close();
		}
		finally {
			LOG.info("Shutting down executor.");
			delegate.shutdown();
			LOG.info("Shutting down internal operations queue.");
			queue.shutdown();
		}
	}
	
	@Override
	public List<Runnable> shutdownNow() {
		LOG.info("Requested shutdown now.");
		try {
			return Stream.concat(delegate.shutdownNow().stream(), queue.shutdownNow().stream()).toList();
		} finally {
			ch.close();
		}
	}
	
	public void start() {
		if(startPaused) {
			startPaused = false;
			LOG.info("Unpausing {} threads", threads);
			sem.release(threads);
			
			if(restoreTasksAtStartup) {
				taskStore.ifPresent(ts -> {
					LOG.info("Loading from task store");
					ts.entries().forEach(entry -> {
						if (!tasks.containsKey(entry.id) && shouldRun(entry)) {
							execute(entry);
						}
					});
				});
			}
		
			synchronized(started) {
				
				started.set(true);
				
				deferredStorage.forEach(entry -> taskStore.get().store(entry));
				deferredStorage.clear();
			}
		}
		else {
			throw new IllegalStateException("Not paused.");
		}
	}

	@Override
	public <T> Future<T> submit(Callable<T> task) {
		if (checkTask(task)) {

			@SuppressWarnings("unchecked")
			var dtask = (DistributedCallable<Serializable>) task;

			var id = dtask.id().map(i -> ClusterID.createNext(i))
					.orElseGet(() -> ClusterID.createNext(ch.getAddress()));

			TaskEntry dfut = checkTaskId(id);
			if (dfut != null) {
				return new EntryFuture<T>(id, dfut);
			}

			return doSubmit(id, dtask, DEFAULT_TASK);
		} else {
			if (LOG.isDebugEnabled()) {
				LOG.debug("Not a {}, submitting locally only.", DistributedCallable.class.getName());
			}
			var id = ClusterID.createNext(address());
			
			var sfut = delegate.submit(() -> {
				try {
					return payloadFilter.filter(task).call();
				}
				finally {
					returnedFutures.remove(id);
				}
			});
			
			var dfut = new AbstractDelegateFuture<>(id, sfut, Collections.emptySet()) {

				@Override
				public boolean cancel(boolean mayInterrupt) {
					returnedFutures.remove(id);
					return super.cancel(mayInterrupt);
				}
				
			};
			returnedFutures.put(id, dfut);
			return dfut;
		}
	}

	@Override
	public Future<?> submit(Runnable task) {
		return doRunnable(Schedule.ONE_SHOT,  task, 0, 0, TimeUnit.SECONDS, null);
	}

	@SuppressWarnings("unchecked")
	@Override
	public <T> Future<T> submit(Runnable task, T result) {
		if(result == null)
			return (Future<T>)submit(task);
		else
			/* TODO */
			throw new UnsupportedOperationException();
	}

	public View view() {
		return ch.getView();
	}

	private void acceptView(View view) {
		
		queue.execute(() -> {
		
			List<Address> leftMembers = DistributedScheduledExecutor.this.view != null && view != null
					? Util.leftMembers(this.view.getMembers(), view.getMembers())
					: Collections.emptyList();
	
			List<Address> newMembers = DistributedScheduledExecutor.this.view != null && view != null
					? Util.newMembers(this.view.getMembers(), view.getMembers())
					: Collections.emptyList();
	
			this.view = view;
			
			LOG.info("View: " + view);
			clusterSize = view.size();
			
			var mbrs = view.getMembers();
			var oldRank = rank;
			for (var i = 0; i < mbrs.size(); i++) {
				var tmp = mbrs.get(i);
				if (tmp.equals(ch.getAddress())) {
					rank = i;
					break;
				}
			}
			
			if (oldRank == -1 || oldRank != rank) {
				LOG.info("My rank is {}", rank);
			}
	
			for (Address mbr : leftMembers) {
				handleLeftMember(mbr);
			}
			
			for (Address mbr : newMembers) {
				handleNewMember(oldRank, mbr);
			}
			
			for(var i = listeners.size() - 1 ; i >= 0 ; i--) {
				listeners.get(i).nodesChanged(leftMembers, newMembers);
			}
		});
	}

	private boolean checkTask(Callable<?> task) {
		return task instanceof DistributedTask;
	}

	private boolean checkTask(Runnable task) {
		return task instanceof DistributedRunnable;
	}

	private  TaskEntry checkTaskId(ClusterID id) {
		var existingTask = tasks.get(id);
		if (existingTask != null) {
			switch (existingTask.task.onConflict()) {
			case IGNORE:
				return existingTask;
			case THROW:
				throw new IllegalArgumentException("There is already a task with the ID of " + id);
			default:
				removeRequest(id);
				break;
			}
		}
		return null;
	}

	private TaskTriggerContext createTriggerContext(ClusterID id) {
		return new TaskTriggerContext() {
			
			@Override
			public Instant lastCompleted() {
				return Optional.ofNullable(taskInfo.get(id)).map(ti -> ti.lastCompleted.orElse(null)).orElse(null);
			}
			
			@Override
			public Instant lastExecuted() {
				return Optional.ofNullable(taskInfo.get(id)).map(ti -> ti.lastExecuted.orElse(null)).orElse(null);
			}
			
			@Override
			public Instant lastScheduled() {
				return Optional.ofNullable(taskInfo.get(id)).map(ti -> ti.lastScheduled).orElse(null);
			}
		};
	}

	@SuppressWarnings("unchecked")
	private <V> ScheduledFuture<V> doRunnable(Schedule schedule, Runnable command, long initialDelay, long period, TimeUnit unit, TaskTrigger taskTrigger) {
		if (checkTask(command)) {

			var dtask = (DistributedRunnable) command;
			var id = dtask.id().map(i -> ClusterID.createNext(i))
					.orElseGet(() -> ClusterID.createNext(ch.getAddress()));

			TaskEntry dfut = checkTaskId(id);
			if (dfut != null) {
				var afut = taskInfo.get(id).future;
				return new AbstractDelegateScheduledFuture<>(id, new EntryFuture<>(id, dfut), dtask.classifiers()) {

					@Override
					public int compareTo(Delayed other) {
						return ((ScheduledFuture<?>)afut).compareTo(other);
					}

					@Override
					public long getDelay(TimeUnit unit) {
						return ((ScheduledFuture<?>)afut).getDelay(unit);
					}
				};
			}

			var ftr = doSubmit(id, dtask, new TaskSpec(Instant.now(), schedule, initialDelay, period, unit, taskTrigger));
			var afut = taskInfo.get(id).future;
			
			var rfut = new AbstractDelegateScheduledFuture<>(id, ftr, dtask.classifiers()) {

				@Override
				public int compareTo(Delayed other) {
					return ((ScheduledFuture<?>)afut).compareTo(other);
				}

				@Override
				public long getDelay(TimeUnit unit) {
					return ((ScheduledFuture<?>)afut).getDelay(unit);
				}

			};
			returnedFutures.put(id, rfut);
			return (ScheduledFuture<V>) rfut;
		} else {
			if(alwaysDistribute) {
				if(command instanceof SerializableRunnable sr)
					return doRunnable(schedule, DistributedRunnable.of(sr), initialDelay, period, unit, null);
				else
					return doRunnable(schedule, DistributedRunnable.of(() -> command.run()), initialDelay, period, unit, null);
			}
			else {
				
				/* Just handle locally. We generate a unique ID and wrap the returned future so it
				 * can be cancelled by it's ID and so it is include in the list of tasks if this is queried.
				 * We must capture cancelling so the future can be  removed from the local map too, and also
				 * when the task naturally completes in the case of a ONE_SHOT submission.
				 */
				
				if (LOG.isDebugEnabled()) {
					LOG.debug("Not a {}, scheduling locally only.", DistributedCallable.class.getName());
				}
				
				ScheduledFuture<V> sfut;
				var id = ClusterID.createNext(ch.getAddress());
				
				switch(schedule) {
				case ONE_SHOT:
					sfut = (ScheduledFuture<V>) delegate.schedule(() -> {
						try {
							payloadFilter.filter(command).run();
						}
						finally {
							returnedFutures.remove(id);
						}
					}, initialDelay, unit);
					break;
				case FIXED_DELAY:
					sfut = (ScheduledFuture<V>) delegate.scheduleWithFixedDelay(payloadFilter.filter(command), initialDelay, period, unit);
					break;
				case FIXED_RATE:
					sfut = (ScheduledFuture<V>) delegate.scheduleAtFixedRate(payloadFilter.filter(command), initialDelay, period, unit);
				default:
					throw new IllegalStateException();
				}
				
				var rfut = new DelegatedScheduledFuture<>(id, sfut, Collections.emptySet()) {
					@Override
					public boolean cancel(boolean mayInterrupt) {
						returnedFutures.remove(id);
						return super.cancel(mayInterrupt);
					}
				};

				returnedFutures.put(id, rfut);
				return sfut;
			}
		}
	}

	private <T> Future<T> doSubmit(ClusterID id, DistributedTask<?> task, TaskSpec spec) {

		if(task.affinity() == Affinity.LOCAL) {
			if(LOG.isDebugEnabled()) {
				LOG.debug("Task {} is a LOCAL task for LOCAL people", id);
			}
			return submitLocal(id, spec, new LocalHandler<>(id, payloadFilter.filter(task.task()), spec));
		}

		try {
			var prevTask = tasks.get(id);
			try {

				if (LOG.isDebugEnabled()) {
					LOG.debug("Is a {}, submitting {} [{}] to cluster.", DistributedCallable.class.getName(), id, spec);
				}
				
				var expectResults = task.affinity() == Affinity.ALL ? clusterSize : 1;
				var entry = new TaskEntry(id, task, ch.getAddress(), expectResults, spec);
				tasks.put(id, entry);
				
				if(task.persistent().orElse(persistentByDefault)) {
					taskStore.ifPresentOrElse(str -> {
							if(deferStorageUntilStarted)
								deferredStorage.add(entry);
							else
								str.store(entry); 
						}, () -> {
						throw new IllegalStateException(MessageFormat.format("Task {0} was persistent, but there is no task store configured.", id));
					});
				}
				
				var req = new Request(Request.Type.EXECUTE, (DistributedTask<?>) task, id, null, spec, null, null, null);
				sendExecuteRequest(null, req);
				return new EntryFuture<T>(id, entry);
			} catch (Exception ex) {
				if(prevTask == null)
					tasks.remove(id);
				else
					tasks.put(id, prevTask);
				throw ex;
			}
		} catch (RuntimeException re) {
			throw re;
		} catch (IOException ioe) {
			throw new UncheckedIOException(ioe);
		} catch (Exception e) {
			throw new IllegalStateException(e);
		}
	}

	private void execute(TaskEntry entry) {
		synchronized(taskInfo) {
			var handler = new RemoteHandler(entry);
			var now = Instant.now();
			ScheduledFuture<?> future = null;
			switch (entry.spec.schedule()) {
			case TRIGGER:
				var triggerContext = createTriggerContext(entry.id);
				var nextFire = entry.spec.trigger().nextFire(triggerContext);
				if(LOG.isDebugEnabled()) {
					LOG.debug("Next Fire for {} is {}", entry.id, nextFire);
				}
				future = delegate.schedule(
						handler, 
						Math.max(0, nextFire.toEpochMilli() - now.toEpochMilli()),
						TimeUnit.MILLISECONDS
					);
				break;
			case NOW:
				if(LOG.isDebugEnabled()) {
					LOG.debug("Execute {} {} [] with {} from {}", entry.spec.schedule(), entry.id, entry.task.id(), entry.task.affinity(), entry.submitter);
				}
				delegate.execute(handler);
				break;
			case ONE_SHOT:
				if(LOG.isDebugEnabled()) {
					LOG.debug("Scheduling {} {} [] with {} from {} in {} {}", entry.spec.schedule(), entry.id, entry.task.id(), entry.task.affinity(),
							entry.submitter, entry.spec.initialDelay(), entry.spec.unit());
				}
				future = delegate.schedule(handler, entry.spec.initialDelay(), entry.spec.unit());
				break;
			case FIXED_DELAY:
				if(LOG.isDebugEnabled()) {
					LOG.debug("Scheduling {} {} [] with {} from {} in {} {} after {} {}", entry.spec.schedule(), entry.id, entry.task.id(),
							entry.task.affinity(), entry.submitter, entry.spec.period(), entry.spec.unit(), entry.spec.initialDelay(), entry.spec.unit());
				}
				future = delegate.scheduleWithFixedDelay(handler, entry.spec.initialDelay(), entry.spec.period(), entry.spec.unit());
				break;
			case FIXED_RATE:
				if(LOG.isDebugEnabled()) {
					LOG.debug("Scheduling {} {} [] with {} from {} in {} {} after {} {}", entry.spec.schedule(), entry.id, entry.task.id(),
							entry.task.affinity(), entry.submitter, entry.spec.period(), entry.spec.unit(), entry.spec.initialDelay(), entry.spec.unit());
				}
				future = delegate.scheduleAtFixedRate(handler, entry.spec.initialDelay(), entry.spec.period(), entry.spec.unit());
				break;
			default:
				throw new IllegalArgumentException();
			}

			if(LOG.isDebugEnabled()) {
				LOG.debug("Scheduled {}", entry.id);
			}
			taskInfo.put(entry.id, new TaskInfo(now, future, taskInfo.get(entry.id)));
		}
	}

	private void handleEvent(Address sender, Request req) {
		for(var i = broadcastListeners.size() - 1 ; i >= 0 ; i--) {
			broadcastListeners.get(i).accept(sender, req.event());
		}
	}

	private void handleExecute(ClusterID id, Address sender, DistributedTask<?> task, TaskSpec spec) {
		var entry = tasks.computeIfAbsent(id, iid -> new TaskEntry(id, task, sender, 1, spec));

		if (shouldRun(entry)) {
			execute(entry);
		}

		if(LOG.isDebugEnabled()) {
			LOG.debug("Sending EXECUTED for {}", id);
		}
		sendRequest(sender, new Request(Request.Type.EXECUTED, null, id, null, null, null, null, null));
	}

	private void handleExecuted(Request req) {
		if(LOG.isDebugEnabled()) {
			LOG.debug("Received EXECUTED ack for {}", req.id());
		}
		var ack = executeAcks.get(req.id());
		if(ack == null)
			LOG.warn("Timed out waiting for EXECUTED ack.");
		else
			ack.release();

		if(LOG.isDebugEnabled()) {
			LOG.debug("Released 1 semaphore {}", req.id());
		}
	}

	private void handleLeftMember(Address mbr) {
		/*
		 * Take over the tasks previously assigned to this member *if* the ID matches my
		 * (new rank)
		 */
		
//		LOG.info("Handling left member {}. I know about {} tasks", mbr, tasks.size());
//		for (Map.Entry<ClusterID, Entry<?>> entry : tasks.entrySet()) {
//			ClusterID id = entry.getKey();
//			if (!shouldRun(id, entry.getValue().submitter, entry.getValue().task, entry.getValue().spec)) {
//				continue;
//			}
//
//			var val = entry.getValue();
//			if (mbr.equals(val.submitter)) {
//				LOG.info("Will not take over tasks submitted by {} because it left the cluster", mbr);
//				continue;
//			}
//			LOG.info("Taking over task {} from {} (submitted by {})", id, mbr, val.submitter);
//			execute(id, val.submitter, val.task, val.spec);
//
//			// XXXXXXXXXXXXXXXXXXXXXXXXXX
//			// TODO
//			// XXXXXXXXXXXXXXXXXXXXXXXXXXX
//			// Pretty sure something similar to handleExecute is needed here
//
//		}
	}


	private void handleLock(Request req) {
		if (lockProvider instanceof DefaultLockProvider dlp) {
			if(LOG.isDebugEnabled()) {
				LOG.debug("Adding lock {} as {}", req.lockName(), req.locker());
			}
			synchronized (dlp.locks) {
				var locker = req.locker();
				if (dlp.locks.containsKey(req.lockName())) {
					if(LOG.isDebugEnabled()) {
						LOG.debug("I ({}) already have lock {}", ch.getAddress(), req.lockName());
					}
					locker = ch.getAddress();
				} else {
					dlp.locks.put(req.lockName(), req.locker());
				}
				sendRequest(req.locker(), new Request(Request.Type.LOCKED, null, null, null, null, locker,
							req.lockName(), null));
			}
		} else {
			LOG.warn(
					"Received LOCK command, but this scheduler is not using {}. This suggests different configuration between nodes and is unsupported.",
					DefaultLockProvider.class.getName());
		}
	}

	private void handleLocked(Request req) {
		if (lockProvider instanceof DefaultLockProvider dlp) {
			if(LOG.isDebugEnabled()) {
				LOG.debug("Received LOCKED ack for {} as {}", req.lockName(), req.locker());
			}
			if (req.locker().equals(ch.getAddress())) {
				if(LOG.isDebugEnabled()) {
					LOG.debug("It was agreed I can lock {}", req.lockName());
				}
				dlp.ackCounts.get(req.lockName()).addAndGet(1);
			} else {
				if(LOG.isDebugEnabled()) {
					LOG.debug("{} claimed they own the lock {}", req.locker(), req.lockName());
				}
			}
			dlp.acks.get(req.lockName()).release();
		} else {
			LOG.warn(
					"Received LOCKED command, but this scheduler is not using {}. This suggests different configuration between nodes and is unsupported.",
					DefaultLockProvider.class.getName());
		}
	}

	private void handleNewMember(int wasRank, Address mbr) {
		/* We only we the new member to be sent tasks once, so only send if ..
		 * if we WERE the leader before this new member
		 * 
		 *  None of this happens if there is shared task storage
		 */
		if(taskStore.isPresent() && wasRank == 0) {
			LOG.info("I was leader, handling new member {}. I know about {} tasks", mbr, tasks.size());
			for (Map.Entry<ClusterID, TaskEntry> entry : tasks.entrySet()) {
				var task = (DistributedTask<?>) entry.getValue().task;
				var spec = entry.getValue().spec.adjustTimes();
				var req = new Request(Request.Type.EXECUTE, task, entry.getKey(), null, spec, null, null, null);
				try {
					sendExecuteRequest(mbr, req);
				} catch (Exception ioe) {
					LOG.warn("Couldn't update new member with task {}", task.id());
				}
			}
		}
		
	}

	private void handleRemove(Address sender, Request req) {
		if(LOG.isDebugEnabled()) {
			LOG.debug("Received REMOVE for {} from {}", req.id(), sender);
		}
		tasks.remove(req.id());
		returnedFutures.remove(req.id());
		var tinfo = taskInfo.remove(req.id());
		if(tinfo != null && tinfo.future != null) {
			tinfo.future.cancel(false);
		}
		
		if(LOG.isDebugEnabled()) {
			LOG.debug("Sending REMOVED to {}", sender);
		}
		sendRequest(sender, new Request(Request.Type.REMOVED, null, req.id(), null, null, null, null, null));
	}

	private void handleRemoved(Address sender, Request req) {
		if(LOG.isDebugEnabled()) {
			LOG.debug("Received REMOVED ack for {} from {}", req.id(), sender);
		}
		var ack = removeAcks.get(req.id());
		if(ack == null)
			LOG.warn("Timed out waiting for REMOVED ack.");
		else
			ack.release();
		
		if(LOG.isDebugEnabled()) {
			LOG.debug("Released 1 semaphore {}", req.id());
		}
	}

	private void handleUnlock(Request req) {
		if (lockProvider instanceof DefaultLockProvider dlp) {
			if(LOG.isDebugEnabled()) {
				LOG.debug("Removing lock {}", req.lockName());
			}
			dlp.locks.remove(req.lockName());
			sendRequest(req.locker(), new Request(Request.Type.UNLOCKED, null, null, null, null, req.locker(),
					req.lockName(), null));
		} else {
			LOG.warn(
					"Received UNLOCK command, but this scheduler is not using {}. This suggests different configuration between nodes and is unsupported.",
					DefaultLockProvider.class.getName());
		}
	}

	private void handleUnlocked(Request req) {
		if (lockProvider instanceof DefaultLockProvider dlp) {
			if(LOG.isDebugEnabled()) {
				LOG.debug("Received UNLOCK ack for {} as {}", req.lockName(), req.locker());
			}
			dlp.acks.get(req.lockName()).release();
		} else {
			LOG.warn(
					"Received UNLOCK command, but this scheduler is not using {}. This suggests different configuration between nodes and is unsupported.",
					DefaultLockProvider.class.getName());
		}
	}

	private void removeRequest(ClusterID id) {
		var tsk = tasks.get(id);
		if(tsk == null) {
			throw new IllegalArgumentException("No task with ID " + id);
		}
		if(LOG.isDebugEnabled()) {
			LOG.debug("Sending remove request {} ({} in cluster right now)", id, clusterSize);
		}
		var sem = new Semaphore(clusterSize);
		try {
			sem.acquire(clusterSize);
			removeAcks.put(id, sem);
			
			sendRequest(null, new Request(Request.Type.REMOVE, null, id, null, DEFAULT_TASK, null, null, null));
			
			/* Wait for everyone to acknowledge via REMOVED */
			if(LOG.isDebugEnabled()) {
				LOG.debug("Waiting for {} nodes to acknowledge removal", clusterSize);
			}
			if (sem.tryAcquire(clusterSize, acknowledgeTimeout.toMillis(), TimeUnit.MILLISECONDS)) {
				if(LOG.isDebugEnabled()) {
					LOG.debug("All {} nodes acknowledged removal", clusterSize);
				}
			}
			else {
				LOG.warn("{} nodes did not reply, perhaps a node went down, ignoring.",  clusterSize - sem.availablePermits());
			}
		} catch (Exception e) {
			LOG.error("Failed multicasting REMOVE request", e);
		} finally {
			removeAcks.remove(id);
			taskStore.ifPresent(ts -> ts.remove(id, tsk.task.classifiers()));
		}
	}

	private void sendExecuteRequest(Address mbr, Request req)
			throws IOException, Exception {
		var acksReqd = mbr == null ? clusterSize : 1;
		var sem = new Semaphore(acksReqd);
		sem.acquire(acksReqd);
		try {
			executeAcks.put(req.id(), sem);
			sendRequest(mbr, req);
			
			/* Wait for everyone to acknowledge via EXECUTED */
			if (!sem.tryAcquire(acksReqd, acknowledgeTimeout.toMillis(), TimeUnit.MILLISECONDS)) {
				LOG.warn("{} nodes did not reply, perhaps a node went down, ignoring.",
						acksReqd - sem.availablePermits());
			}
			else { 
				if(LOG.isDebugEnabled()) {
					LOG.debug("Acknowledged {}", req.id());
				}
			}
		}
		finally {
			executeAcks.remove(req.id());
		}
	}

	private void sendRequest(Address sender, Request repreq) {
		try {
			if(LOG.isDebugEnabled()) {
				LOG.debug("Sending request: {} to {}", repreq.type(), sender == null ? "ALL" : sender);
			}
			
			srlzr.set(payloadSerializer);
			var buf = Util.streamableToByteBuffer(repreq);
			ch.send(new BytesMessage(sender, buf));
		} catch (RuntimeException re) {
			throw re;
		} catch (IOException ioe) {
			throw new UncheckedIOException(ioe);
		} catch (Exception e) {
			throw new IllegalStateException(e);
		} finally {
			srlzr.remove();
		}
	}
	
	private boolean shouldRun(TaskEntry entry) {
		var run = false;
		if (entry.spec.schedule() == Schedule.NOW) {

			/*
			 * Request to execute NOW. Whether the task runs on this node will be based on
			 * its affinity.
			 */

			switch (entry.task.affinity()) {
			case ALL:
				if(LOG.isDebugEnabled()) {
					LOG.debug("Running immediate task {} [{}] on this node because its affinity is {}", entry.id, entry.task.id(),
							entry.task.affinity());
				}
				run = true;
				break;
			case ANY:
				int index = entry.id.getId() % clusterSize;
				run = index == rank;
				if (run) {
					if(LOG.isDebugEnabled()) {
						LOG.debug(
								"Running immediate task {} [{}] on this node because its affinity is {} and it's rank of {} matches.",
								entry.id, entry.task.id(), entry.task.affinity(), rank);
					}
				} else {
					if(LOG.isDebugEnabled()) {
						LOG.debug(
								"Not running immediate task {} [{}] on this node because its affinity is {} and it's rank of {} does not match.",
								entry.id, entry.task.id(), entry.task.affinity(), rank);
					}
				}
				break;
			case MEMBER:
				if(LOG.isDebugEnabled()) {
					LOG.debug(
							"Running immediate task {} [{}] on this node because its affinity is {} and this is the node it was directed at",
							entry.id, entry.task.id(), entry.task.affinity());
				}
				run = true;
				break;
			case THIS:
				if (entry.submitter.equals(ch.getAddress())) {
					if(LOG.isDebugEnabled()) {
						LOG.debug(
								"Running immediate task {} [{}] on this node because its affinity is {} and this is the node it was scheduled on",
								entry.id, entry.task.id(), entry.task.affinity());
					}
					run = true;
				} else {
					if(LOG.isDebugEnabled()) {
						LOG.debug(
								"Not running immediate task {} [{}] on this node because its affinity is {} and this is not the node it was scheduled on",
								entry.id, entry.task.id(), entry.task.affinity());
					}
				}
				break;
			default:
				throw new UnsupportedOperationException("Invalid affinity for a remote task.");
			}
		} else {
			/*
			 * Request to execute at some point in the future. The task will actually be
			 * scheduled on ALL nodes, and similar checks to above are made when the task actually
			 * executes.
			 */
			if(LOG.isDebugEnabled()) {
				LOG.debug("Running {} [{}] on this node because it has a schedule.", entry.id, entry.task.id());
			}
			run = true;
		}
		return run;
	}
	@SuppressWarnings("unchecked")
	private <T> Future<T> submitLocal(ClusterID id, TaskSpec spec, LocalHandler<?> handler) {
		synchronized(taskInfo) {
			var now = Instant.now();
			Future<T> fut;
			switch (spec.schedule()) {
			case TRIGGER:
				fut = (Future<T>) delegate.schedule(
					handler, 
					Math.max(0,  
						spec.trigger().nextFire(createTriggerContext(id)).toEpochMilli() - now.toEpochMilli()),
					TimeUnit.MILLISECONDS
				);
				break;
			case ONE_SHOT:
				fut = (Future<T>) delegate.schedule(handler, spec.initialDelay(), spec.unit());
				break;
			case FIXED_DELAY:
				fut = (Future<T>) delegate.scheduleWithFixedDelay(handler, spec.initialDelay(), spec.period(), spec.unit());
				break;
			case FIXED_RATE:
				fut = (Future<T>) delegate.scheduleAtFixedRate(handler, spec.initialDelay(), spec.period(), spec.unit());
				break;
			default:
				throw new IllegalStateException();
			}
			
			taskInfo.put(id, new TaskInfo(now, fut, taskInfo.get(id)));
			
			return fut;
		}
	}
}
