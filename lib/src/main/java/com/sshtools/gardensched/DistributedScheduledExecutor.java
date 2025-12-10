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
import java.util.UUID;
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

import com.sshtools.gardensched.Request.AckPayload;
import com.sshtools.gardensched.Request.EventPayload;
import com.sshtools.gardensched.Request.LockPayload;
import com.sshtools.gardensched.Request.ProgressMessagePayload;
import com.sshtools.gardensched.Request.ProgressPayload;
import com.sshtools.gardensched.Request.ResultPayload;
import com.sshtools.gardensched.Request.StartProgressPayload;
import com.sshtools.gardensched.Request.StorePayload;
import com.sshtools.gardensched.Request.SubmitPayload;
import com.sshtools.gardensched.Request.Type;

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
		private Optional<ObjectStore> objectStore = Optional.empty();
		private boolean persistentByDefault = false;
		private boolean restoreTasksAtStartup = true;
		private boolean deferStorageUntilStarted = false;
		private boolean checkLocalObjectStorageFirst = true;

		public DistributedScheduledExecutor build() throws Exception {
			return new DistributedScheduledExecutor(this);
		}
		
		public Builder withoutCheckLocalObjectStorageFirst() {
			return withCheckLocalObjectStorageFirst(false);
		}
		
		public Builder withCheckLocalObjectStorageFirst(boolean checkLocalObjectStorageFirst) {
			this.checkLocalObjectStorageFirst = checkLocalObjectStorageFirst;
			return this;
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

		public Builder withObjectStore(ObjectStore objectStore) {
			this.objectStore = Optional.of(objectStore);
			return this;
		}

		public Builder withTaskSuccessHandler(TaskSuccessHandler  taskSuccessHandler) {
			this.taskSuccessHandler = Optional.of(taskSuccessHandler);
			return this;
		}

	}

	private abstract class AbstractHandler<TSK extends DistributedTask<RESULT>, RESULT extends Serializable> implements Runnable {
		final ClusterID id;
		final TSK task;
		final TaskSpec taskSpec;

		public AbstractHandler(ClusterID id, TSK task, TaskSpec taskSpec) {
			this.id = id;
			this.task = task;
			this.taskSpec = taskSpec;
		}

		@Override
		public final void run() {

			if(LOG.isDebugEnabled()) {
				LOG.debug("Handling {} [{}]", id, task.affinity());
			}
			
			TaskInfo tinfo = taskInfo.get(id);
			
			var retry = new AtomicLong(Integer.MIN_VALUE);
			var cancel = new AtomicBoolean();
			try {
				
				TaskContext.ctx.set(new TaskContext() {
					
					private final TaskProgress progress = createProgress(tinfo);

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
				
				doTask(tinfo, new TaskCompletionContext() {
					
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
		
		protected abstract TaskProgress createProgress(TaskInfo tinfo);
		
		protected Object doTask(TaskInfo taskInfo, TaskCompletionContext taskCompletionContext) {
			Object result = null;
			taskInfo.lastExecuted = Optional.of(Instant.now());
			taskInfo.active = true;
			try {
				if(LOG.isDebugEnabled()) {
					LOG.debug("Executing {}", id);
				}
				result = doRunTask();
				taskInfo.lastError = Optional.empty();
				taskSuccessHandler.ifPresent(te -> {
					te.handleSuccess(id, taskSpec, task, taskCompletionContext);
				});
			} catch (Throwable t) {
				LOG.error("Failed executing {}", id, t);
				result = t;
				taskInfo.lastError = Optional.of(t);
				taskErrorHandler.ifPresent(te -> {
					te.handleError(id, taskSpec, task, taskCompletionContext, t);
				});
			} finally {
				taskInfo.lastCompleted = Optional.of(Instant.now());
				taskInfo.progress = Optional.empty();
				taskInfo.active = false;
			}
			return result;
		}

		protected Object doRunTask() throws Exception {
			if(task instanceof Callable dcl)
				return dcl.call();
			else if(task instanceof Runnable dcr) {
				dcr.run();
			}
			else {
				throw new IllegalArgumentException("Unknown task type.");
			}
			return null;
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
					sendRequest(null, new Request(Request.Type.LOCK, new LockPayload(name, ch.getAddress())));
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

						sendRequest(null, new Request(Request.Type.UNLOCK, new LockPayload(name, ch.getAddress())));

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
		private final TaskEntry entry;
		private boolean cancelled;

		private EntryFuture(TaskEntry entry) {
			this.entry = entry;
		}

		@Override
		public boolean cancel(boolean mayInterruptIfRunning) {
			if (!cancelled) {
				LOG.info("Cancelling future {}", entry.id);
				try {
					removeRequest(entry.id);
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
	
	private class LocalHandler<RESULT extends Serializable> extends AbstractHandler<DistributedTask<RESULT>, RESULT> implements Runnable {

		public LocalHandler(ClusterID id, DistributedTask<RESULT> task, TaskSpec taskSpec) {
			super(id, task, taskSpec);
		}

		@Override
		protected TaskProgress createProgress(TaskInfo tinfo) {
			return new TaskProgress() {
				
				@Override
				public void start(long max) {
					tinfo.maxProgress = Optional.of(max);
				}
				
				@Override
				public void progress(long value) {
					tinfo.progress = Optional.of(value);
				}
				
				@Override
				public void message(String bundle, String key, String... args) {
					tinfo.message(bundle, key, args);
				}
				
				@Override
				public void message(String text) {
					tinfo.message(text);
				}
			};
		}

		@Override
		protected void noRetrigger() {
			taskInfo.remove(id);
		}

		@Override
		void retrigger() {
			submitLocal(id, taskSpec, this);
		}

		@SuppressWarnings("unchecked")
		@Override
		protected Object doRunTask() throws Exception {
			/* Bit hacky, but this is to save having to manually autowire
			 * AFFINITY.LOCAL tasks 
			 */
			if(task instanceof DistributedCallable dcl)
				return ((Callable<Object>)payloadFilter.filter(dcl.task())).call();
			else if(task instanceof DistributedRunnable dcr) {
				((Runnable)payloadFilter.filter(dcr.task())).run();
			}
			else {
				throw new IllegalArgumentException("Unknown task type.");
			}
			return null;
		}
	}
	
	private class RemoteHandler  extends AbstractHandler<DistributedTask<Serializable>, Serializable> implements Runnable {
		final TaskEntry entry;

		@SuppressWarnings("unchecked")
		public RemoteHandler(TaskEntry entry) {
			super(entry.id, (DistributedTask<Serializable>) entry.task(), entry.spec);
			this.entry = entry;
		}

		@Override
		protected TaskProgress createProgress(TaskInfo tinfo) {
			return new TaskProgress() {
				
				@Override
				public void start(long max) {
					tinfo.maxProgress = Optional.of(max);
					queue.submit(() -> {
						sendRequest(null, new Request(Type.START_PROGRESS, id, new StartProgressPayload(max)));
					});
				}
				
				@Override
				public void progress(long value) {
					tinfo.progress = Optional.of(value);
					queue.submit(() -> {
						sendRequest(null, new Request(Type.PROGRESS, id, new ProgressPayload(value)));
					});
				}
				
				@Override
				public void message(String bundle, String key, String... args) {
					tinfo.message(bundle, key, args);
					queue.submit(() -> {
						sendRequest(null, new Request(Type.PROGRESS_MESSAGE, id, new ProgressMessagePayload(bundle, key, args)));
					});
				}
				
				@Override
				public void message(String text) {
					tinfo.message(text);
					queue.submit(() -> {
						sendRequest(null, new Request(Type.PROGRESS_MESSAGE, id, new ProgressMessagePayload(text)));
					});
				}
			};
		}
		
		@Override
		protected Object doTask(TaskInfo taskInfo, TaskCompletionContext taskErrorContext) {
			Serializable result =  null;
			switch (task.affinity()) {
			case ALL:
			case MEMBER:
				queue.submit(() -> {
					sendRequest(null, new Request(Type.EXECUTING, id));
				});
				result = (Serializable) super.doTask(taskInfo, taskErrorContext);
				break;
			case ONLY_THIS:
				if (entry.submitter.equals(ch.getAddress())) {
					if(LOG.isDebugEnabled()) {
						LOG.debug(
								"Running task {} [{}] on this node because its affinity is {} and it was scheduled on this node.",
								id, task.id(), task.affinity());
					}
					queue.submit(() -> {
						sendRequest(null, new Request(Type.EXECUTING, id));
					});
					result = (Serializable) super.doTask(taskInfo, taskErrorContext);
				}
				else {
					return null;
				}
				break;
			case THIS:
				if (!ch.getView().getMembers().contains(entry.submitter) && leader()) {
					if(LOG.isDebugEnabled()) {
						LOG.debug(
								"Running task {} [{}] on this node because its affinity is {} and the node it was scheduled on is not active and we are leader.",
								id, task.id(), task.affinity());
					}
					queue.submit(() -> {
						sendRequest(null, new Request(Type.EXECUTING, id));
					});
					result = (Serializable) super.doTask(taskInfo, taskErrorContext);
				}
				else if (entry.submitter.equals(ch.getAddress())) {
					if(LOG.isDebugEnabled()) {
						LOG.debug(
								"Running task {} [{}] on this node because its affinity is {} and it was scheduled on this node.",
								id, task.id(), task.affinity());
					}
					queue.submit(() -> {
						sendRequest(null, new Request(Type.EXECUTING, id));
					});
					result = (Serializable) super.doTask(taskInfo, taskErrorContext);
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
					queue.submit(() -> {
						sendRequest(null, new Request(Type.EXECUTING, id));
					});
					result = (Serializable) super.doTask(taskInfo, taskErrorContext);
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
			
			if(result instanceof Throwable thr) {
				result = new TaskError(thr);
			}

			var rp = new ResultPayload(result);
			
			try {
				acks.runWithAck(Request.Type.RESULT, id, clusterSize, () -> {
					sendRequest(null, new Request(Request.Type.RESULT, id, rp));
				});
			} catch (Exception e) {
				LOG.error("Failed to broadcast result for {}.", id, e);
			}
			return result;
		}

		@Override
		void retrigger() {
			execute(entry, false);
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
				case GET_OBJECT:
					handleGetObject(msg.getSrc(), req);
					break;
				case HAS_OBJECT:
					handleHasObject(msg.getSrc(), req);
					break;
				case REMOVE_OBJECT:
					handleRemoveObject(msg.getSrc(), req);
					break;
				case STORED_OBJECT:
					handleStore(msg.getSrc(), req);
					break;
				case START_PROGRESS:
					handleStartProgress(msg.getSrc(), req);
					break;
				case PROGRESS:
					handleProgress(msg.getSrc(), req);
					break;
				case PROGRESS_MESSAGE:
					handleProgressMessage(msg.getSrc(), req);
					break;
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
				case ACK:
					AckPayload ack = req.payload(); 
					acks.ack(ack.type(), req.id(), ack.result());
					break;
				case SUBMIT:{
					SubmitPayload submission = req.payload();
					handleSubmit(req.id(), msg.getSrc(), submission.task(), submission.spec(), submission.runNow());
					break;
				}
				case EXECUTING:
					handleExecuting(msg.getSrc(), req);
					break;
				case RESULT:
					var id = req.id();
					var entry = tasks.get(id);
					ResultPayload res = req.payload(); 
					if (entry == null) {
						LOG.error("Received result for task I don't know about, {}", id);
					} else {
						taskInfo.get(id).active = false;
						entryResults(id, entry, res.result());
					}
					sendRequest(msg.getSrc(), new Request(Request.Type.ACK, req.id(), new AckPayload(Request.Type.RESULT)));
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
	private final ConcurrentMap<ClusterID, TaskInfo> taskInfo = new ConcurrentHashMap<>();
	private final ConcurrentMap<ClusterID, Future<?>> localFutures = new ConcurrentHashMap<>();
	private final ExecutorService queue;
	private final boolean alwaysDistribute;
	private final Duration acknowledgeTimeout;
	private final Optional<TaskErrorHandler> taskErrorHandler;
	private final Optional<TaskSuccessHandler> taskSuccessHandler;
	private final Optional<TaskStore> taskStore;
	private final Optional<ObjectStore> objectStore;
	private final List<NodeListener> listeners = new ArrayList<>();
	private final List<BroadcastEventListener> broadcastListeners = new ArrayList<>();
	private final Duration closeTimeout;
	private final boolean persistentByDefault;
	private final boolean deferStorageUntilStarted;
	private final List<TaskEntry> deferredStorage = new ArrayList<>();
	private final AtomicBoolean started = new AtomicBoolean();
	private final Ack acks;
	private final boolean checkLocalObjectStorageFirst;
	
	private View view;
	private int rank = -1;
	private int clusterSize = -1;
	private int threads;
	private final PayloadFilter payloadFilter;
	private boolean startPaused;
	private Semaphore sem;
	private boolean restoreTasksAtStartup;
	
	private DistributedScheduledExecutor(Builder bldr) throws Exception {
		objectStore = bldr.objectStore;
		checkLocalObjectStorageFirst = bldr.checkLocalObjectStorageFirst;
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
		acks = new Ack(acknowledgeTimeout);
		
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
							execute(entry, false);
						}
					});
				});
			}
		}
	}

	public void restore(TaskEntry entry) {
		if (!tasks.containsKey(entry.id) && shouldRun(entry)) {
			execute(entry, false);
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
    		taskInfo.entrySet().forEach(tsk -> {
    			LOG.info("Cancelling {}", tsk.getKey());
    			tsk.getValue().underlyingFuture.cancel(false); 
    		});
    		
    		if(!started.get()) {
   				sem.release(threads);
    		}
    		
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
			sendRequest(null, new Request(Request.Type.EVENT, ClusterID.createNext(ch.getAddress()), new  EventPayload(event)));
		});
	}
	
	@Override
	public void execute(Runnable runnable) {
		submit(runnable);
	}
	
	@SuppressWarnings("unchecked")
	public <F extends IdentifiableFuture<?>> F future(ClusterID task) {
		var tinfo = taskInfo.get(task);
		return tinfo == null ? null : (F)tinfo.userFuture;
	}

	public <F extends IdentifiableFuture<?>> Optional<F> futureOr(ClusterID task) {
		return Optional.ofNullable(future(task));
	}

	@SuppressWarnings("unchecked")
	public <R> List<IdentifiableFuture<R>> futures() {
		return taskInfo.values().stream().map(ti ->  (IdentifiableFuture<R>)ti.userFuture).filter(f -> f != null).toList();
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
	
	public boolean leader() {
		return rank == 0;
	}
	
	public int rank() {
		return rank;
	}
	
	public void put(String path, Serializable key, Serializable value) {
		var os = checkObjectStore();
		os.put(path, key, value);
		sendRequest(null, new Request(Type.STORED_OBJECT, new StorePayload(path, key)));
	}

	@SuppressWarnings("unchecked")
	public <T extends Serializable> T get(String path, Serializable key) {
		var os = checkObjectStore();

		if(checkLocalObjectStorageFirst && os.has(path, key)) {
			return (T)os.get(path, key);
		}
		
		var id = ClusterID.createNext(UUID.randomUUID().toString());
		try {
			return (T)acks.runWithAck(Request.Type.GET_OBJECT, id, clusterSize, () -> {
				sendRequest(null, new Request(Type.GET_OBJECT, id, new StorePayload(path, key)));
			}).stream().findFirst().orElse(null);
		}
		catch(RuntimeException re) {
			throw re;
		}
		catch(Exception e) {
			throw new IllegalStateException("Failed to get from object store.", e);
		}
	}

	public boolean has(String path, Serializable key) {
		var os = checkObjectStore();
		if(checkLocalObjectStorageFirst && os.has(path, key)) {
			return true;
		}
		
		var id = ClusterID.createNext(UUID.randomUUID().toString());
		try {
			return acks.runWithAck(Request.Type.HAS_OBJECT, id, clusterSize, () -> {
				sendRequest(null, new Request(Type.HAS_OBJECT, id, new StorePayload(path, key)));
			}).stream().map(s -> (Boolean)s).filter(t -> t).findFirst().orElse(false);
		}
		catch(RuntimeException re) {
			throw re;
		}
		catch(Exception e) {
			throw new IllegalStateException("Failed to check object store.", e);
		}
	}
	
	public boolean remove(String path, Serializable key) {
		var id = ClusterID.createNext(UUID.randomUUID().toString());
		try {
			return acks.runWithAck(Request.Type.REMOVE_OBJECT, id, clusterSize, () -> {
				sendRequest(null, new Request(Type.REMOVE_OBJECT, id, new StorePayload(path, key)));
			}).stream().map(s -> (Boolean)s).filter(t -> t).findFirst().orElse(false);
		}
		catch(RuntimeException re) {
			throw re;
		}
		catch(Exception e) {
			throw new IllegalStateException("Failed to remove from object store.", e);
		}
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

			var taskEntry = checkTaskId(id);
			
			if (taskEntry != null) {
				return (ScheduledFuture<V>) taskInfo.get(id).userFuture;
			}

			return (ScheduledFuture<V>) doSubmit(id, dtask, new TaskSpec(Instant.now(), Schedule.ONE_SHOT, delay, delay, unit, null));
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
						localFutures.remove(id);
					}
				}, delay, unit);
				
				var dfut = new DelegatedScheduledFuture<>(id, sfut, Collections.emptySet(), Collections.emptyMap()){

					@Override
					public boolean cancel(boolean mayInterrupt) {
						localFutures.remove(id);
						return super.cancel(mayInterrupt);
					}

					@Override
					public TaskInfo info() {
						return taskInfo.get(id);
					}

					@Override
					public void runNow() {
						cancel(false);
						schedule(callable, 0, unit);
					}
					
				};
				localFutures.put(id, dfut);
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
							execute(entry, false);
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

	@SuppressWarnings("unchecked")
	@Override
	public <T> Future<T> submit(Callable<T> task) {
		if (checkTask(task)) {

			var dtask = (DistributedCallable<Serializable>) task;

			var id = dtask.id().map(i -> ClusterID.createNext(i))
					.orElseGet(() -> ClusterID.createNext(ch.getAddress()));

			TaskEntry dfut = checkTaskId(id);
			if (dfut != null) {
				return (Future<T>)taskInfo.get(id).userFuture;
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
					localFutures.remove(id);
				}
			});
			
			var dfut = new AbstractDelegateFuture<>(id, sfut, Collections.emptySet(), Collections.emptyMap()) {

				@Override
				public boolean cancel(boolean mayInterrupt) {
					localFutures.remove(id);
					return super.cancel(mayInterrupt);
				}

				@Override
				public TaskInfo info() {
					return taskInfo.get(id);
				}

				@Override
				public void runNow() {
					throw new UnsupportedOperationException();
				}
				
			};
			localFutures.put(id, dfut);
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
		return task instanceof DistributedCallable;
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
				return (ScheduledFuture<V>) taskInfo.get(id).userFuture;
			}

			return (ScheduledFuture<V>)doSubmit(id, dtask, new TaskSpec(Instant.now(), schedule, initialDelay, period, unit, taskTrigger));
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
							localFutures.remove(id);
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
				
				var rfut = new DelegatedScheduledFuture<>(id, sfut, Collections.emptySet(), Collections.emptyMap()) {
					@Override
					public boolean cancel(boolean mayInterrupt) {
						localFutures.remove(id);
						return super.cancel(mayInterrupt);
					}

					@Override
					public TaskInfo info() {
						return taskInfo.get(id);
					}

					@Override
					public void runNow() {
						if(info().active())
							throw new IllegalStateException("Already running.");
						else {
							cancel(false);
							doRunnable(schedule, command, 0, period, unit, taskTrigger);
						}
					}
				};

				localFutures.put(id, rfut);
				return sfut;
			}
		}
	}

	@SuppressWarnings("unchecked")
	private <T> Future<T> doSubmit(ClusterID id, DistributedTask<?> task, TaskSpec spec) {

		if(task.affinity() == Affinity.LOCAL) {
			if(LOG.isDebugEnabled()) {
				LOG.debug("Task {} is a LOCAL task for LOCAL people", id);
			}
			return submitLocal(id, spec, new LocalHandler<>(id, task, spec));
		}

		try {
			var prevTask = tasks.get(id);
			try {

				if (LOG.isDebugEnabled()) {
					LOG.debug("Is a {}, submitting {} [{}] to cluster with classifiers `{}`.", DistributedCallable.class.getName(), id, spec, String.join(", ", task.classifiers()));
				}
				
				var entry = new TaskEntry(id, task, ch.getAddress(), spec);
				tasks.put(id, entry);
				
				if(task.persistent().orElse(persistentByDefault)) {
					taskStore.ifPresentOrElse(str -> {
						
						synchronized(started) {
							if(deferStorageUntilStarted && !started.get())
								deferredStorage.add(entry);
							else
								str.store(entry);
						}
						
						}, () -> {
						throw new IllegalStateException(MessageFormat.format("Task {0} was persistent, but there is no task store configured.", id));
					});
				}
				
				var req = new Request(Request.Type.SUBMIT, id, new SubmitPayload((DistributedTask<?>) task, spec));
				sendSubmitRequest(null, req);
				return (Future<T>) taskInfo.get(id).userFuture;
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

	private void entryResults(ClusterID id, TaskEntry entry, Serializable result) {
		var responsesReceived = entry.results.addAndGet(1);
		var expectedResults = expectedResults(entry.task.affinity());
		
		var tinfo = taskInfo.get(id);
		if(result instanceof TaskError taskError) {
			result = taskError.toException();
			tinfo.lastError = Optional.of((Throwable)result);
		}
		else {
			tinfo.lastError = Optional.empty();
		}
		tinfo.lastCompleted = Optional.of(Instant.now());
		
		if (responsesReceived >= expectedResults) {
			if (LOG.isDebugEnabled()) {
				LOG.debug("No more responses to {} expected, completing.", id);
			}
			entry.promise.setResult(result);

			if(entry.spec().schedule().repeats()) {
				if (LOG.isDebugEnabled()) {
					LOG.debug("Leaving task {} in place as {} REPEATS and it has completed.", id, entry.spec().schedule());
				}
				entry.results.set(0);
			}
			else {
				if (LOG.isDebugEnabled()) {
					LOG.debug("Removing task {} as {} does not REPEAT and it has completed.", id, entry.spec().schedule());
				}
				queue.execute(() -> removeRequest(id));
			}
		} else {
			if (LOG.isDebugEnabled()) {
				LOG.debug("Received response {} of {}. Waiting for {} more responses to {}.", responsesReceived,  expectedResults,  expectedResults - responsesReceived, id);
			}
		}
	}

	private void execute(TaskEntry entry, boolean runNow) {
		synchronized(taskInfo) {
			var handler = new RemoteHandler(entry);
			var now = Instant.now();
			var offset = Instant.now().toEpochMilli() - entry.spec.submitted().toEpochMilli();
			ScheduledFuture<?> future = null;
			switch (entry.spec.schedule()) {
			case TRIGGER:
				var nextFire = runNow ? Instant.now() : entry.spec.trigger().nextFire(createTriggerContext(entry.id));
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
			{
				if(LOG.isDebugEnabled()) {
					LOG.debug("Scheduling {} {} [] with {} from {} in {} {} (Offset is {}ms)", entry.spec.schedule(), entry.id, entry.task.id(), entry.task.affinity(),
							entry.submitter, entry.spec.initialDelay(), entry.spec.unit(), offset);
				}
				var msdely = runNow ? 0 : Math.max(0l,  entry.spec.unit().toMillis(entry.spec.initialDelay()) - offset);
				future = delegate.schedule(handler, msdely, TimeUnit.MILLISECONDS);
				break;
			}
			case FIXED_DELAY:
			{
				if(LOG.isDebugEnabled()) {
					LOG.debug("Scheduling {} {} [] with {} from {} in {} {} after {} {} (Offset is {}ms)", entry.spec.schedule(), entry.id, entry.task.id(),
							entry.task.affinity(), entry.submitter, entry.spec.period(), entry.spec.unit(), entry.spec.initialDelay(), entry.spec.unit(), offset);
				}
			
				var msdely = runNow ? 0 : Math.max(0l,  entry.spec.unit().toMillis(entry.spec.initialDelay()) - offset);
				var msdur = entry.spec.unit().toMillis(entry.spec.period()); 
				future = delegate.scheduleWithFixedDelay(handler, msdely, msdur, TimeUnit.MILLISECONDS);
				break;
			}
			case FIXED_RATE:
			{
				if(LOG.isDebugEnabled()) {
					LOG.debug("Scheduling {} {} [] with {} from {} in {} {} after {} {} (Offset is {}ms)", entry.spec.schedule(), entry.id, entry.task.id(),
							entry.task.affinity(), entry.submitter, entry.spec.period(), entry.spec.unit(), entry.spec.initialDelay(), entry.spec.unit(), offset);
				}
				var msdely = runNow ? 0 : Math.max(0l,  entry.spec.unit().toMillis(entry.spec.initialDelay()) - offset);
				var msdur = entry.spec.unit().toMillis(entry.spec.period()); 
				future = delegate.scheduleAtFixedRate(handler, msdely, msdur, TimeUnit.MILLISECONDS);
				break;
			}
			default:
				throw new IllegalArgumentException();
			}

			if(LOG.isDebugEnabled()) {
				LOG.debug("Scheduled {}", entry.id);
			}
			
			var tinfo = new TaskInfo(entry.spec, now, future, taskInfo.get(entry.id));
			var efuture = new EntryFuture<Object>(entry);
			var ffuture = future;
			var foffset = offset;
			tinfo.userFuture = new AbstractDelegateScheduledFuture<>(entry.id, (Future<Object>)efuture, entry.task.classifiers(), entry.task.attributes()) {
				@Override
				public TaskInfo info() {
					return tinfo;
				}

				@Override
				public long getDelay(TimeUnit unit) {
					return Math.max(0l, ffuture.getDelay(unit) - foffset);
				}

				@Override
				public int compareTo(Delayed o) {
					return ffuture.compareTo(o);
				}

				@Override
				public void runNow() {
					switch (entry.spec.schedule()) {
					case NOW:
						throw new UnsupportedOperationException();
					default:
						if(tinfo.active) {
							throw new IllegalStateException("Already running.");
						}
						else {
							/* Broadcast to everyone the same task, but marked to fire immediately */
							var req = new Request(Request.Type.SUBMIT, entry.id, new SubmitPayload((DistributedTask<?>) entry.task, entry.spec(), true));
							try {
								sendSubmitRequest(null, req);
							} catch (Exception e) {
								LOG.error("Failed to re-submit task {}", entry.id, e);
							}
						}
						break;
					}
				}
			};
			
			taskInfo.put(entry.id, tinfo);
		}
	}

	private void handleEvent(Address sender, Request req) {
		for(var i = broadcastListeners.size() - 1 ; i >= 0 ; i--) {
			broadcastListeners.get(i).accept(sender, ((EventPayload)req.payload()).event());
		}
	}

	private void handleSubmit(ClusterID id, Address sender, DistributedTask<?> task, TaskSpec spec, boolean runNow) {
		var entry = new TaskEntry(id, task, sender, spec);
		synchronized(tasks) {
			var was = tasks.get(id);
			if(was != null) {
				var tinfo = taskInfo.get(id);
				if(tinfo != null) {
					tinfo.underlyingFuture.cancel(true);
				}
			}
			tasks.put(id, entry);
		}

		if (shouldRun(entry)) {
			execute(entry, runNow);
		}

		if(LOG.isDebugEnabled()) {
			LOG.debug("Sending SUBMIT ACK for {}", id);
		}
		sendRequest(sender, new Request(Request.Type.ACK, id, new AckPayload(Request.Type.SUBMIT)));
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
		LockPayload lock = req.payload();
		if (lockProvider instanceof DefaultLockProvider dlp) {
			if(LOG.isDebugEnabled()) {
				LOG.debug("Adding lock {} as {}", lock.lockName(), lock.locker());
			}
			synchronized (dlp.locks) {
				var locker = lock.locker();
				if (dlp.locks.containsKey(lock.lockName())) {
					if(LOG.isDebugEnabled()) {
						LOG.debug("I ({}) already have lock {}", ch.getAddress(), lock.lockName());
					}
					locker = ch.getAddress();
				} else {
					dlp.locks.put(lock.lockName(), lock.locker());
				}
				sendRequest(lock.locker(), new Request(Request.Type.LOCKED, new LockPayload(
						lock.lockName(), locker)));
			}
		} else {
			LOG.warn(
					"Received LOCK command, but this scheduler is not using {}. This suggests different configuration between nodes and is unsupported.",
					DefaultLockProvider.class.getName());
		}
	}

	private void handleLocked(Request req) {
		LockPayload lock = req.payload();
		if (lockProvider instanceof DefaultLockProvider dlp) {
			if(LOG.isDebugEnabled()) {
				LOG.debug("Received LOCKED ack for {} as {}", lock.lockName(), lock.locker());
			}
			if (lock.locker().equals(ch.getAddress())) {
				if(LOG.isDebugEnabled()) {
					LOG.debug("It was agreed I can lock {}", lock.lockName());
				}
				dlp.ackCounts.get(lock.lockName()).addAndGet(1);
			} else {
				if(LOG.isDebugEnabled()) {
					LOG.debug("{} claimed they own the lock {}", lock.locker(), lock.lockName());
				}
			}
			dlp.acks.get(lock.lockName()).release();
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
		if(!taskStore.isPresent() && wasRank == 0) {
			LOG.info("I was leader, handling new member {}. I know about {} tasks", mbr, tasks.size());
			for (Map.Entry<ClusterID, TaskEntry> entry : tasks.entrySet()) {
				var value = entry.getValue();
				var task = (DistributedTask<?>) value.task;
				LOG.info("   Sending {} [{}]", value.id(), entry.getKey());
				var req = new Request(Request.Type.SUBMIT, value.id(), new SubmitPayload(task, value.spec));
				try {
					sendSubmitRequest(mbr, req);
				} catch (Exception ioe) {
					LOG.warn("Couldn't update new member with task {}", task.id());
				}
			}
		}
		
	}

	private void handleHasObject(Address sender, Request req) {
		StorePayload store = req.payload();
		if(LOG.isDebugEnabled()) {
			LOG.debug("Received HAS_OBJECT from {} for {}", sender);
		}
		objectStore.ifPresentOrElse(os -> {
			sendRequest(sender, new Request(Request.Type.ACK, req.id(), new AckPayload(Request.Type.REMOVE_OBJECT, os.has(store.path(), store.key()))));
		}, () -> {
			throw new IllegalStateException("No object store configured.");
		});
	}

	private void handleGetObject(Address sender, Request req) {
		StorePayload store = req.payload();
		if(LOG.isDebugEnabled()) {
			LOG.debug("Received GET_OBJECT from {} for {}", sender, req.id());
		}
		objectStore.ifPresentOrElse(os -> {
			sendRequest(sender, new Request(Request.Type.ACK, req.id(), new AckPayload(Request.Type.GET_OBJECT, os.get(store.path(), store.key()))));
		}, () -> {
			throw new IllegalStateException("No object store configured.");
		});
	}

	private void handleRemoveObject(Address sender, Request req) {
		StorePayload store = req.payload();
		if(LOG.isDebugEnabled()) {
			LOG.debug("Received STORE_OBJECT from {} for {}", sender);
		}
		objectStore.ifPresentOrElse(os -> {
			var removed = os.remove(store.path(), store.key()); 
			if(removed) {
				if(LOG.isDebugEnabled()) {
					LOG.debug("Removed object {} from store at path {}", store.key(), store.path());
				}		
			}
			sendRequest(sender, new Request(Request.Type.ACK, req.id(), new AckPayload(Request.Type.REMOVE_OBJECT, removed)));
		}, () -> {
			throw new IllegalStateException("No object store configured.");
		});
	}

	private void handleStore(Address sender, Request req) {
		if(sender.equals(address()))
			return;
			
		StorePayload store = req.payload();
		if(LOG.isDebugEnabled()) {
			LOG.debug("Received STORE from {} for {}", sender);
		}
		objectStore.ifPresentOrElse(os -> {
			if(os.remove(store.path(), store.key())) {
				if(LOG.isDebugEnabled()) {
					LOG.debug("Removed object {} from store at path {}", store.key(), store.path());
				}		
			}
		}, () -> {
			throw new IllegalStateException("No object store configured.");
		});
	}

	private void handleStartProgress(Address sender, Request req) {
		if(sender.equals(address()))
			return;
			
		if(LOG.isDebugEnabled()) {
			LOG.debug("Received START_PROGRESS for {}", req.id());
		}
		StartProgressPayload sp = req.payload(); 
		var tinfo = taskInfo.get(req.id());
		if(tinfo != null) {
			tinfo.maxProgress = sp.max() == TaskProgress.INDETERMINATED ? Optional.empty() : Optional.of(sp.max());
		}
	}

	private void handleProgressMessage(Address sender, Request req) {
		if(sender.equals(address()))
			return;
		
		if(LOG.isDebugEnabled()) {
			LOG.debug("Received PROGRESS_MESSAGE for {}", req.id());
		}
		ProgressMessagePayload sp = req.payload(); 
		var tinfo = taskInfo.get(req.id());
		if(tinfo != null) {
			if(sp.message() == null)
				tinfo.message(sp.bundle(), sp.key(), sp.args());
			else
				tinfo.message(sp.message());
		}
	}

	private void handleExecuting(Address sender, Request req) {
		if(sender.equals(address()))
			return;
		
		if(LOG.isDebugEnabled()) {
			LOG.debug("Received EXECUTING for {}", req.id());
		}
		
		var tinfo = taskInfo.get(req.id());
		if(tinfo != null) {
			tinfo.lastExecuted = Optional.of(Instant.now());
			tinfo.active = true;
		}
	}

	private void handleProgress(Address sender, Request req) {
		if(sender.equals(address()))
			return;
		
		if(LOG.isDebugEnabled()) {
			LOG.debug("Received PROGRESS for {}", req.id());
		}
		
		ProgressPayload sp = req.payload(); 
		var tinfo = taskInfo.get(req.id());
		if(tinfo != null) {
			tinfo.progress = Optional.of(sp.progress());
		}
	}

	private void handleRemove(Address sender, Request req) {
		if(LOG.isDebugEnabled()) {
			LOG.debug("Received REMOVE for {} from {}", req.id(), sender);
		}
		tasks.remove(req.id());
		var tinfo = taskInfo.remove(req.id());
		if(tinfo != null && tinfo.underlyingFuture != null) {
			tinfo.underlyingFuture.cancel(false);
		}
		
		if(LOG.isDebugEnabled()) {
			LOG.debug("Sending REMOVED to {}", sender);
		}
		sendRequest(sender, new Request(Request.Type.ACK, req.id(), new AckPayload(Request.Type.REMOVE)));
	}

	private void handleUnlock(Request req) {
		LockPayload lock = req.payload();
				
		if (lockProvider instanceof DefaultLockProvider dlp) {
			if(LOG.isDebugEnabled()) {
				LOG.debug("Removing lock {}", lock.lockName());
			}
			dlp.locks.remove(lock.lockName());
			sendRequest(lock.locker(), new Request(Request.Type.UNLOCKED, new LockPayload(
					lock.lockName(), lock.locker())));
		} else {
			LOG.warn(
					"Received UNLOCK command, but this scheduler is not using {}. This suggests different configuration between nodes and is unsupported.",
					DefaultLockProvider.class.getName());
		}
	}

	private void handleUnlocked(Request req) {
		LockPayload lock = req.payload();
		if (lockProvider instanceof DefaultLockProvider dlp) {
			if(LOG.isDebugEnabled()) {
				LOG.debug("Received UNLOCK ack for {} as {}", lock.lockName(), lock.locker());
			}
			dlp.acks.get(lock.lockName()).release();
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
		
		try {
			acks.runWithAck(Request.Type.REMOVE, id, clusterSize, () -> {
				sendRequest(null, new Request(Request.Type.REMOVE, id));
			});
		} catch (Exception e) {
			LOG.error("Failed multicasting REMOVE request", e);
		} finally {
			if(tsk.task().persistent().orElse(persistentByDefault)) {
				taskStore.ifPresent(ts -> ts.remove(id, tsk.task.classifiers()));
			}
		}
	}

	private void sendSubmitRequest(Address mbr, Request req)
			throws IOException, Exception {
		var acksReqd = mbr == null ? clusterSize : 1;
		acks.runWithAck(Request.Type.SUBMIT, req.id(), acksReqd, () -> {
			sendRequest(mbr, req);
		});
	}

	private void sendRequest(Address recipient, Request repreq) {
		try {
			if(LOG.isDebugEnabled()) {
				LOG.debug("Sending request: {} to {}", repreq.type(), recipient == null ? "ALL" : recipient);
			}
			
			srlzr.set(payloadSerializer);
			var buf = Util.streamableToByteBuffer(repreq);
			ch.send(new BytesMessage(recipient, buf));
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
				if (!ch.getView().getMembers().contains(entry.submitter) && leader()) {
					if(LOG.isDebugEnabled()) {
						LOG.debug(
								"Running immediate task {} [{}] on this node because its affinity is {} and the node it was scheduled on is not active and we are leader",
								entry.id, entry.task.id(), entry.task.affinity());
					}
					run = true;
				}
				else if (entry.submitter.equals(ch.getAddress())) {
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
			case ONLY_THIS:
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
			
			taskInfo.put(id, new TaskInfo(spec, now, fut, taskInfo.get(id)));
			
			return fut;
		}
	}
	
	private int expectedResults(Affinity affinity) {
		switch(affinity) {
		case ALL:
			return clusterSize;
		default:
			return 1;
		}
	}
	
	private ObjectStore checkObjectStore() {
		return objectStore.orElseThrow(() -> new IllegalStateException("No object store has been configured.") );
	}
}
