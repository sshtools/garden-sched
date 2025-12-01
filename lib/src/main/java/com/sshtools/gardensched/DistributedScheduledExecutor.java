package com.sshtools.gardensched;

import java.io.IOException;
import java.io.Serializable;
import java.io.UncheckedIOException;
import java.time.Duration;
import java.time.Instant;
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

import org.jgroups.Address;
import org.jgroups.BytesMessage;
import org.jgroups.JChannel;
import org.jgroups.Message;
import org.jgroups.Receiver;
import org.jgroups.View;
import org.jgroups.util.Promise;
import org.jgroups.util.Util;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.sshtools.gardensched.TaskErrorHandler.TaskErrorContext;

public final class DistributedScheduledExecutor implements ScheduledExecutorService {
	
	public final static class Builder {
		private int schedulerThreads = 1;
		private String props = "udp.xml";
		private String clusterName = "garden-sched";
		private String groupName = null;
		private Optional<LockProvider> lockProvider = Optional.empty();
		private Optional<TaskSerializer> taskSerializer = Optional.empty();
		private Optional<TaskFilter> taskFilter = Optional.empty();
		private boolean alwaysCluster = false;
		private boolean startPaused = false;
		private Duration acknowledgeTimeout = Duration.ofSeconds(10);
		private Optional<TaskErrorHandler> taskErrorHandler = Optional.empty();

		public DistributedScheduledExecutor build() throws Exception {
			return new DistributedScheduledExecutor(this);
		}
		
		public Builder withTaskSerializer(TaskSerializer taskSerializer) {
			this.taskSerializer = Optional.of(taskSerializer);
			return this;
		}
		
		public Builder withTaskFilter(TaskFilter taskFilter) {
			this.taskFilter = Optional.of(taskFilter);
			return this;
		}
		
		public Builder withTaskErrorHandler(TaskErrorHandler  taskErrorHandler) {
			this.taskErrorHandler = Optional.of(taskErrorHandler);
			return this;
		}
		
		public Builder withAcknowledgeTimeout(Duration acknowledgeTimeout) {
			this.acknowledgeTimeout = acknowledgeTimeout;
			return this;
		}

		public Builder withStartPaused() {
			return withStartPaused(true);
		}

		public Builder withStartPaused(boolean startPaused) {
			this.startPaused = startPaused;
			return this;
		}

		public Builder withAlwaysCluster() {
			return withAlwaysCluster(true);
		}

		public Builder withAlwaysCluster(boolean alwaysCluster) {
			this.alwaysCluster = alwaysCluster;
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

		public Builder withSchedulerThreads(int schedulerThreads) {
			this.schedulerThreads = schedulerThreads;
			return this;
		}

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
					sendRequest(null, new Request<>(Request.Type.LOCK, null, null, null, null, ch.getAddress(), name));
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

						sendRequest(null, new Request<>(Request.Type.UNLOCK, null, null, null, null, ch.getAddress(), name));

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

	private static class Entry<RESULT> {
		private final DistributedTask<RESULT, ?> task;
		private final Address submitter;
		private final Promise<RESULT> promise = new Promise<>();
		private final AtomicInteger results;
		private final TaskSpec spec;

		public Entry(DistributedTask<RESULT, ?> task, Address submitter, int expectedResults, TaskSpec spec) {
			this.task = task;
			this.submitter = submitter;
			this.results = new AtomicInteger(expectedResults);
			this.spec = spec;
		}
	}

	private final class EntryFuture<T> implements Future<T> {
		private final ClusterID id;
		private final Entry<T> entry;
		private boolean cancelled;

		private EntryFuture(ClusterID id, Entry<T> entry) {
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

		@Override
		public T get() throws InterruptedException, ExecutionException {
			return entry.promise.getResult();
		}

		@Override
		public T get(long timeout, TimeUnit unit) throws InterruptedException, ExecutionException, TimeoutException {
			return entry.promise.getResult(timeout);
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
				currentTaskScheduler.set(DistributedScheduledExecutor.this);
				doTask(new TaskErrorContext() {
					
					@Override
					public void retry(Duration delay) {
						retry.set(delay.toMillis());
					}
					
					@Override
					public void cancel() {
						cancel.set(true);
					}
				});
			}
			finally {
				currentTaskScheduler.remove();
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
		
		protected void noRetrigger() {
		}
		
		abstract void retrigger();

		protected Object doTask(TaskErrorContext taskErrorContext) {
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
			} catch (Throwable t) {
				LOG.error("Failed executing {}", id, t);
				result = t;

				synchronized(taskInfo) {
					Optional.ofNullable(taskInfo.get(id)).ifPresent(ti -> ti.lastError = Optional.of(t));
				}
				
				taskErrorHandler.ifPresent(te -> {
					te.handleError(id, taskSpec, (Serializable) task, taskErrorContext, t);
				});
			}
			return result;
		}
	}
	
	private class LocalHandler<RESULT> extends AbstractHandler<Serializable> implements Runnable {

		public LocalHandler(ClusterID id, Serializable task, TaskSpec taskSpec) {
			super(id, task, taskSpec);
		}

		@Override
		void retrigger() {
			submitLocal(id, taskSpec, this);
		}

		@Override
		protected void noRetrigger() {
			taskInfo.remove(id);
		}
	}
	
	private class RemoteHandler<RESULT> extends AbstractHandler<DistributedTask<RESULT, ?>> implements Runnable {
		final Address sender;

		public RemoteHandler(ClusterID id, Address sender, DistributedTask<RESULT, ?> task, TaskSpec taskSpec) {
			super(id, task, taskSpec);
			this.sender = sender;
		}

		@Override
		public void run() {
			if(LOG.isDebugEnabled()) {
				LOG.debug("Handling {} [{}]", id, task.affinity());
			}
			super.run();
		}

		@Override
		protected Object doTask(TaskErrorContext taskErrorContext) {
			Object result =  null;
			switch (task.affinity()) {
			case ALL:
			case MEMBER:
				result = super.doTask(taskErrorContext);
				break;
			case THIS:
				if (sender.equals(ch.getAddress())) {
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
			sendRequest(sender, new Request<>(Request.Type.RESULT, null, id, result, DEFAULT_TASK, null, null));
			return result;
		}

		@Override
		void retrigger() {
			execute(id, sender, task, taskSpec);
		}
	}

	private class SchedReceiver implements Receiver {

		@Override
		public void receive(Message msg) {
			srlzr.set(taskSerializer);
			fltr.set(taskFilter);
			try {
				Request<Object> req = Util.streamableFromByteBuffer(Request.class, ((BytesMessage) msg).getBytes(),
						msg.getOffset(), msg.getLength());
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
					@SuppressWarnings("unchecked")
					var entry = (Entry<Object>) tasks.get(req.id());

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

	private final TaskSerializer taskSerializer;
	private final JChannel ch;
	private final ScheduledExecutorService delegate;
	private final ConcurrentMap<ClusterID, Entry<?>> tasks = new ConcurrentHashMap<>();
	private final LockProvider lockProvider;
	private final Map<ClusterID, Semaphore> removeAcks = new ConcurrentHashMap<>();
	private final Map<ClusterID, Semaphore> executeAcks = new ConcurrentHashMap<>();
	private final ConcurrentMap<ClusterID, TaskInfo> taskInfo = new ConcurrentHashMap<>();
	private final ConcurrentMap<ClusterID, IdentifiableFuture<?>> returnedFutures = new ConcurrentHashMap<>();
	private final ExecutorService queue;
	private final boolean alwaysCluster;
	private final Duration acknowledgeTimeout;
	private final Optional<TaskErrorHandler> taskErrorHandler;
	private final List<NodeListener> listeners = new ArrayList<>();
	
	private final static ThreadLocal<DistributedScheduledExecutor> currentTaskScheduler = new ThreadLocal<>();
	
	public static DistributedScheduledExecutor get() {
		var ts = currentTaskScheduler.get();
		if(ts == null)
			throw new IllegalStateException("Not in a distributed task scheduler thread.");
		return ts;
	}
	
	private View view;
	private int rank = -1;
	private int clusterSize = -1;
	private int threads;
	private final TaskFilter taskFilter;
	private boolean startPaused;
	private Semaphore sem;

	private DistributedScheduledExecutor(Builder bldr) throws Exception {
		taskSerializer = bldr.taskSerializer.orElseGet(TaskSerializer::defaultSerializer);
		lockProvider = bldr.lockProvider.orElseGet(() -> new DefaultLockProvider());
		threads = bldr.schedulerThreads;
		delegate = Executors.newScheduledThreadPool(threads);
		startPaused = bldr.startPaused;
		acknowledgeTimeout = bldr.acknowledgeTimeout;
		taskErrorHandler = bldr.taskErrorHandler;
		
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
			LOG.info("Starting with {} threads", threads);
		}
		
		queue = Executors.newSingleThreadExecutor();
		taskFilter = bldr.taskFilter.orElseGet(() -> TaskFilter.nullFilter());
		alwaysCluster = bldr.alwaysCluster;
		ch = new JChannel(bldr.props);
		ch.name(bldr.groupName);
		ch.setReceiver(new SchedReceiver());
		ch.connect(bldr.clusterName);
	}
	
	public void addListener(NodeListener listener) {
		this.listeners.add(listener);
	}
	
	public void removeListener(NodeListener listener) {
		this.listeners.remove(listener);
	}
	
	public static TaskFilter currentFilter() {
		return fltr.get();
	}
	
	public void start() {
		if(startPaused) {
			startPaused = false;
			LOG.info("Unpausing {} threads", threads);
			sem.release(threads);
		}
		else {
			throw new IllegalStateException("Not paused.");
		}
	}
	
	public int rank() {
		return rank;
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
	public boolean awaitTermination(long timeout, TimeUnit timeUnit) throws InterruptedException {
		return delegate.awaitTermination(timeout, timeUnit);
	}

	@Override
	public void execute(Runnable runnable) {
		submit(runnable);
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
	
	public Address address() {
		return ch.getAddress();
	}
	
	public View view() {
		return ch.getView();
	}

	public LockProvider lockProvider() {
		return lockProvider;
	}

	@SuppressWarnings("unchecked")
	@Override
	public <V> ScheduledFuture<V> schedule(Callable<V> callable, long delay, TimeUnit unit) {
		
		if (checkTask(callable)) {

			var dtask = (DistributedCallable<?>) callable;
			
			var id = dtask.id().
					map(i -> ClusterID.createNext(i)).
					orElseGet(() -> ClusterID.createNext(ch.getAddress()));

			Entry<V> dfut = checkTaskId(id);
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
				return rfut;
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
			if(alwaysCluster) {
				if(callable instanceof SerializableCallable cr)
					return schedule(DistributedCallable.of(cr), delay, unit);
				else
					return schedule(DistributedCallable.of(() -> callable.call()), delay, unit);
			}
			else {
				if (LOG.isDebugEnabled()) {
					LOG.debug("Not a {}, scheduling locally only.", DistributedCallable.class.getName());
				}
				
				var id = ClusterID.createNext(address());
				
				var sfut = delegate.schedule(() -> {
					try {
						return taskFilter.filter(callable).call();
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

	public ScheduledFuture<?> schedule(Runnable command, TaskTrigger trigger) {
		return doRunnable(Schedule.TRIGGER, command, 0, 0, TimeUnit.MILLISECONDS, trigger);
	}

	@Override
	public ScheduledFuture<?> schedule(Runnable command, long delay, TimeUnit unit) {
		return doRunnable(Schedule.ONE_SHOT, command, delay, delay, unit, null);
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
		delegate.shutdown();
		queue.shutdown();
		ch.close();
	}

	@Override
	public List<Runnable> shutdownNow() {
		try {
			return delegate.shutdownNow();
		} finally {
			ch.close();
		}
	}

	@Override
	public <T> Future<T> submit(Callable<T> task) {
		if (checkTask(task)) {

			var dtask = (DistributedCallable<T>) task;

			var id = dtask.id().map(i -> ClusterID.createNext(i))
					.orElseGet(() -> ClusterID.createNext(ch.getAddress()));

			Entry<T> dfut = checkTaskId(id);
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
					return taskFilter.filter(task).call();
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
				handleNewMember(mbr);
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

	@SuppressWarnings("unchecked")
	private <T> Entry<T> checkTaskId(ClusterID id) {
		var existingTask = tasks.get(id);
		if (existingTask != null) {
			switch (existingTask.task.onConflict()) {
			case IGNORE:
				return (Entry<T>) existingTask;
			case THROW:
				throw new IllegalArgumentException("There is already a task with the ID of " + id);
			default:
				removeRequest(id);
				break;
			}
		}
		return null;
	}

	@SuppressWarnings("unchecked")
	private <V> ScheduledFuture<V> doRunnable(Schedule schedule, Runnable command, long initialDelay, long period, TimeUnit unit, TaskTrigger taskTrigger) {
		if (checkTask(command)) {

			var dtask = (DistributedRunnable) command;
			var id = dtask.id().map(i -> ClusterID.createNext(i))
					.orElseGet(() -> ClusterID.createNext(ch.getAddress()));

			Entry<V> dfut = checkTaskId(id);
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
			if(alwaysCluster) {
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
							taskFilter.filter(command).run();
						}
						finally {
							returnedFutures.remove(id);
						}
					}, initialDelay, unit);
					break;
				case FIXED_DELAY:
					sfut = (ScheduledFuture<V>) delegate.scheduleWithFixedDelay(taskFilter.filter(command), initialDelay, period, unit);
					break;
				case FIXED_RATE:
					sfut = (ScheduledFuture<V>) delegate.scheduleAtFixedRate(taskFilter.filter(command), initialDelay, period, unit);
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

	private void handleNewMember(Address mbr) {

		if(rank == 0) {
			LOG.info("I am leader, handling new member {}. I know about {} tasks", mbr, tasks.size());
			for (Map.Entry<ClusterID, Entry<?>> entry : tasks.entrySet()) {
				@SuppressWarnings("unchecked")
				DistributedTask<Object, ?> task = (DistributedTask<Object, ?>) entry.getValue().task;
				var spec = entry.getValue().spec.adjustTimes();
				var req = new Request<>(Request.Type.EXECUTE, task, entry.getKey(), null, spec, null, null);
				try {
					sendExecuteRequest(mbr, req);
				} catch (Exception ioe) {
					LOG.warn("Couldn't update new member with task {}", task.id());
				}
			}
		}
		
	}

	private void sendExecuteRequest(Address mbr, Request<?> req)
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

	private <T> Future<T> doSubmit(ClusterID id, DistributedTask<T, ?> task, TaskSpec spec) {

		if(task.affinity() == Affinity.LOCAL) {
			if(LOG.isDebugEnabled()) {
				LOG.debug("Task {} is a LOCAL task for LOCAL people", id);
			}
			return submitLocal(id, spec, new LocalHandler<>(id, taskFilter.filter(task.task()), spec));
		}

		try {
			try {

				if (LOG.isDebugEnabled()) {
					LOG.debug("Is a {}, submitting {} [{}] to cluster.", DistributedCallable.class.getName(), id, spec);
				}
				
				var expectResults = task.affinity() == Affinity.ALL ? clusterSize : 1;
				var entry = new Entry<>(task, ch.getAddress(), expectResults, spec);
				tasks.put(id, entry);
				var req = new Request<>(Request.Type.EXECUTE, (DistributedTask<T, ?>) task, id, null, spec, null, null);
				sendExecuteRequest(null, req);
				return new EntryFuture<T>(id, entry);
			} catch (Exception ex) {
				tasks.remove(id);
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

	private TaskTriggerContext createTriggerContext(ClusterID id) {
		return new TaskTriggerContext() {
			
			@Override
			public Instant lastScheduled() {
				return Optional.ofNullable(taskInfo.get(id)).map(ti -> ti.lastScheduled).orElse(null);
			}
			
			@Override
			public Instant lastExecuted() {
				return Optional.ofNullable(taskInfo.get(id)).map(ti -> ti.lastExecuted.orElse(null)).orElse(null);
			}
			
			@Override
			public Instant lastCompleted() {
				return Optional.ofNullable(taskInfo.get(id)).map(ti -> ti.lastCompleted.orElse(null)).orElse(null);
			}
		};
	}

	private void execute(ClusterID id, Address sender, DistributedTask<?, ?> task, TaskSpec spec) {
		synchronized(taskInfo) {
			var handler = new RemoteHandler<>(id, sender, task, spec);
			var now = Instant.now();
			ScheduledFuture<?> future = null;
			switch (spec.schedule()) {
			case TRIGGER:
				var triggerContext = createTriggerContext(id);
				var nextFire = spec.trigger().nextFire(triggerContext);
				if(LOG.isDebugEnabled()) {
					LOG.debug("Next Fire for {} is {}", id, nextFire);
				}
				future = delegate.schedule(
						handler, 
						Math.max(0, nextFire.toEpochMilli() - now.toEpochMilli()),
						TimeUnit.MILLISECONDS
					);
				break;
			case NOW:
				if(LOG.isDebugEnabled()) {
					LOG.debug("Execute {} {} [] with {} from {}", spec.schedule(), id, task.id(), task.affinity(), sender);
				}
				delegate.execute(handler);
				break;
			case ONE_SHOT:
				if(LOG.isDebugEnabled()) {
					LOG.debug("Scheduling {} {} [] with {} from {} in {} {}", spec.schedule(), id, task.id(), task.affinity(),
							sender, spec.initialDelay(), spec.unit());
				}
				future = delegate.schedule(handler, spec.initialDelay(), spec.unit());
				break;
			case FIXED_DELAY:
				if(LOG.isDebugEnabled()) {
					LOG.debug("Scheduling {} {} [] with {} from {} in {} {} after {} {}", spec.schedule(), id, task.id(),
							task.affinity(), sender, spec.period(), spec.unit(), spec.initialDelay(), spec.unit());
				}
				future = delegate.scheduleWithFixedDelay(handler, spec.initialDelay(), spec.period(), spec.unit());
				break;
			case FIXED_RATE:
				if(LOG.isDebugEnabled()) {
					LOG.debug("Scheduling {} {} [] with {} from {} in {} {} after {} {}", spec.schedule(), id, task.id(),
							task.affinity(), sender, spec.period(), spec.unit(), spec.initialDelay(), spec.unit());
				}
				future = delegate.scheduleAtFixedRate(handler, spec.initialDelay(), spec.period(), spec.unit());
				break;
			default:
				throw new IllegalArgumentException();
			}

			if(LOG.isDebugEnabled()) {
				LOG.debug("Scheduled {}", id);
			}
			taskInfo.put(id, new TaskInfo(now, future, taskInfo.get(id)));
		}
	}

	private void handleExecute(ClusterID id, Address sender, DistributedTask<?, ?> task, TaskSpec spec) {
		tasks.computeIfAbsent(id, iid -> new Entry<>(task, sender, 1, spec));

		if (shouldRun(id, sender, task, spec)) {
			execute(id, sender, task, spec);
		}

		if(LOG.isDebugEnabled()) {
			LOG.debug("Sending EXECUTED for {}", id);
		}
		sendRequest(sender, new Request<Object>(Request.Type.EXECUTED, null, id, null, null, null, null));
	}

	private void sendRequest(Address sender, Request<?> repreq) {
		try {
			if(LOG.isDebugEnabled()) {
				LOG.debug("Sending request: {} to {}", repreq.type(), sender == null ? "ALL" : sender);
			}
			
			srlzr.set(taskSerializer);
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

	private void handleLock(Request<Object> req) {
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
				sendRequest(req.locker(), new Request<Object>(Request.Type.LOCKED, null, null, null, null, locker,
							req.lockName()));
			}
		} else {
			LOG.warn(
					"Received LOCK command, but this scheduler is not using {}. This suggests different configuration between nodes and is unsupported.",
					DefaultLockProvider.class.getName());
		}
	}

	private void handleLocked(Request<Object> req) {
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

	private void handleRemove(Address sender, Request<Object> req) {
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
		sendRequest(sender, new Request<Object>(Request.Type.REMOVED, null, req.id(), null, null, null, null));
	}

	private void handleRemoved(Address sender, Request<Object> req) {
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

	private void handleExecuted(Request<Object> req) {
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

	private void handleUnlock(Request<Object> req) {
		if (lockProvider instanceof DefaultLockProvider dlp) {
			if(LOG.isDebugEnabled()) {
				LOG.debug("Removing lock {}", req.lockName());
			}
			dlp.locks.remove(req.lockName());
			sendRequest(req.locker(), new Request<Object>(Request.Type.UNLOCKED, null, null, null, null, req.locker(),
					req.lockName()));
		} else {
			LOG.warn(
					"Received UNLOCK command, but this scheduler is not using {}. This suggests different configuration between nodes and is unsupported.",
					DefaultLockProvider.class.getName());
		}
	}

	private void handleUnlocked(Request<Object> req) {
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
		if(LOG.isDebugEnabled()) {
			LOG.debug("Sending remove request {} ({} in cluster right now)", id, clusterSize);
		}
		var sem = new Semaphore(clusterSize);
		try {
			sem.acquire(clusterSize);
			removeAcks.put(id, sem);
			
			sendRequest(null, new Request<>(Request.Type.REMOVE, null, id, null, DEFAULT_TASK, null, null));
			
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
		}
	}

	private boolean shouldRun(ClusterID id, Address sender, DistributedTask<?, ?> task, TaskSpec spec) {
		var run = false;
		if (spec.schedule() == Schedule.NOW) {

			/*
			 * Request to execute NOW. Whether the task runs on this node will be based on
			 * its affinity.
			 */

			switch (task.affinity()) {
			case ALL:
				if(LOG.isDebugEnabled()) {
					LOG.debug("Running immediate task {} [{}] on this node because its affinity is {}", id, task.id(),
							task.affinity());
				}
				run = true;
				break;
			case ANY:
				int index = id.getId() % clusterSize;
				run = index == rank;
				if (run) {
					if(LOG.isDebugEnabled()) {
						LOG.debug(
								"Running immediate task {} [{}] on this node because its affinity is {} and it's rank of {} matches.",
								id, task.id(), task.affinity(), rank);
					}
				} else {
					if(LOG.isDebugEnabled()) {
						LOG.debug(
								"Not running immediate task {} [{}] on this node because its affinity is {} and it's rank of {} does not match.",
								id, task.id(), task.affinity(), rank);
					}
				}
				break;
			case MEMBER:
				if(LOG.isDebugEnabled()) {
					LOG.debug(
							"Running immediate task {} [{}] on this node because its affinity is {} and this is the node it was directed at",
							id, task.id(), task.affinity());
				}
				run = true;
				break;
			case THIS:
				if (sender.equals(ch.getAddress())) {
					if(LOG.isDebugEnabled()) {
						LOG.debug(
								"Running immediate task {} [{}] on this node because its affinity is {} and this is the node it was scheduled on",
								id, task.id(), task.affinity());
					}
					run = true;
				} else {
					if(LOG.isDebugEnabled()) {
						LOG.debug(
								"Not running immediate task {} [{}] on this node because its affinity is {} and this is not the node it was scheduled on",
								id, task.id(), task.affinity());
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
				LOG.debug("Running {} [{}] on this node because it has a schedule.", id, task.id());
			}
			run = true;
		}
		return run;
	}
	
	static final ThreadLocal<TaskSerializer> srlzr = new ThreadLocal<>();
	static final ThreadLocal<TaskFilter> fltr = new ThreadLocal<>();
}
