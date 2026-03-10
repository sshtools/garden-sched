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

import java.io.IOException;
import java.io.Serializable;
import java.io.UncheckedIOException;
import java.text.MessageFormat;
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
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

import org.jgroups.Address;
import org.jgroups.Message;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.sshtools.gardensched.Request.AckPayload;
import com.sshtools.gardensched.Request.ProgressMessagePayload;
import com.sshtools.gardensched.Request.ProgressPayload;
import com.sshtools.gardensched.Request.ResultPayload;
import com.sshtools.gardensched.Request.StartProgressPayload;
import com.sshtools.gardensched.Request.SubmitPayload;
import com.sshtools.gardensched.Request.Type;

public final class DistributedScheduledExecutor implements ScheduledExecutorService, DistributedComponent, MessageReceiver {
	
	public final static class Builder {
		private int schedulerThreads = 1;
		private boolean alwaysDistribute = false;
		private boolean startPaused = false;
		private Optional<TaskErrorHandler> taskErrorHandler = Optional.empty();
		private Optional<TaskSuccessHandler> taskSuccessHandler = Optional.empty();
		private Optional<TaskStore> taskStore = Optional.empty();
		private boolean persistentByDefault = false;
		private boolean restoreTasksAtStartup = true;
		private boolean deferStorageUntilStarted = false;
		private final DistributedMachine machine;
		
		public Builder(DistributedMachine machine) {
			this.machine = machine;
		}

		public DistributedScheduledExecutor build() throws Exception {
			return new DistributedScheduledExecutor(this);
		}
		
		public Builder withAlwaysDistribute() {
			return withAlwaysDistribute(true);
		}
		
		public Builder withAlwaysDistribute(boolean alwaysDistribute) {
			this.alwaysDistribute = alwaysDistribute;
			return this;
		}
		
		public Builder withDeferStorageUntilStarted() {
			return withDeferStorageUntilStarted(true);
		}

		
		public Builder withDeferStorageUntilStarted(boolean deferStorageUntilStarted) {
			this.deferStorageUntilStarted = deferStorageUntilStarted;
			return this;
		}
		
		public Builder withoutRestoreTasksAtStartup() {
			return withRestoreTasksAtStartup(false);
		}

		
		public Builder withPersistentByDefault() {
			return withPersistentByDefault(true);
		}
		
		public Builder withPersistentByDefault(boolean persistentByDefault) {
			this.persistentByDefault = persistentByDefault;
			return this;
		}

		public Builder withRestoreTasksAtStartup(boolean restoreTasksAtStartup) {
			this.restoreTasksAtStartup = restoreTasksAtStartup;
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

	private abstract class AbstractHandler<TSK extends DistributedTask<RESULT>, RESULT extends Serializable> implements Runnable {
		protected final ClusterID id;
		protected final TSK task;
		protected TaskSpec taskSpec;

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
			if(tinfo == null) {
				LOG.error("No task info for {}, cannot run task.", id);
				return;
			}
			
			var retry = new AtomicLong(Integer.MIN_VALUE);
			var cancel = new AtomicBoolean();
			var newSpec = new AtomicReference<TaskSpec>();
			try {
				
				TaskContext.ctx.set(new TaskContext() {
					
					private final TaskProgress progress = createProgress(tinfo);

					@Override
					public Address address() {
						return DistributedScheduledExecutor.this.machine.address();
					}
					
					@Override
					public void cancel() {
						/* If this is TRIGGER type, only cancels this run */
						DistributedScheduledExecutor.this.future(id).cancel(true);
					}
					
					@Override
					public ClusterID id() {
						return id;
					}
					
					@Override
					public boolean isCancelled() {
						return DistributedScheduledExecutor.this.future(id).isCancelled();
					}

					@Override
					public TaskProgress progress() {
						return progress;
					}

					@Override
					public void reschedule(long delay, TimeUnit unit) {
						newSpec.set(new TaskSpec(Instant.now(), Schedule.ONE_SHOT, delay, delay, unit, taskSpec.trigger()));
					}

					@Override
					public void reschedule(TaskTrigger trigger) {
						newSpec.set(new TaskSpec(Instant.now(), Schedule.TRIGGER, 0, 0, TimeUnit.MILLISECONDS, trigger));
					}

					@Override
					public void rescheduleWithFixedDelay(long delay, long period, TimeUnit unit) {
						newSpec.set(new TaskSpec(Instant.now(), Schedule.FIXED_DELAY, period, delay, unit, taskSpec.trigger()));
					}

					@Override
					public void rescheduleWithFixedRate(long delay, long period, TimeUnit unit) {
						newSpec.set(new TaskSpec(Instant.now(), Schedule.FIXED_RATE, period, delay, unit, taskSpec.trigger()));
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
					noRetrigger();
					
					LOG.info("Retrying {} in {} ms because error handling requested it.", id, retry.get());

					/* Broadcast to everyone the same task, but marked to fire immediately */
					var spec = new TaskSpec(Instant.now(), Schedule.ONE_SHOT, retry.get(), 0, TimeUnit.MILLISECONDS, null);
					var req = new Request(Request.Type.SUBMIT, id, new SubmitPayload((DistributedTask<?>)task, spec, true));
					try {
						sendSubmitRequest(null, req);
					} catch (Exception e) {
						LOG.error("Failed to re-submit task {}", id, e);
					}
				}
				else if(newSpec.get() != null) {

					noRetrigger();
					
					LOG.info("Rescheduling {} with new spec because task requested it.", id);
					
					var req = new Request(Request.Type.SUBMIT, id, new SubmitPayload((DistributedTask<?>)task, newSpec.get(), false));
					try {
						sendSubmitRequest(null, req);
					} catch (Exception e) {
						LOG.error("Failed to re-submit task {}", id, e);
					}
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
		
		protected void noRetrigger() {
		}

		abstract void retrigger();
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
				public void message(String text) {
					tinfo.message(text);
				}
				
				@Override
				public void message(String bundle, String key, String... args) {
					tinfo.message(bundle, key, args);
				}
				
				@Override
				public void progress(long value) {
					tinfo.progress = Optional.of(value);
				}
				
				@Override
				public void start(long max) {
					tinfo.maxProgress = Optional.of(max);
				}
			};
		}

		@SuppressWarnings("unchecked")
		@Override
		protected Object doRunTask() throws Exception {
			/* Bit hacky, but this is to save having to manually autowire
			 * AFFINITY.LOCAL tasks 
			 */
			if(task instanceof DistributedCallable dcl)
				return ((Callable<Object>)machine.payloadFilter().filter(dcl.task())).call();
			else if(task instanceof DistributedRunnable dcr) {
				((Runnable)machine.payloadFilter().filter(dcr.task())).run();
			}
			else {
				throw new IllegalArgumentException("Unknown task type.");
			}
			return null;
		}

		@Override
		protected void noRetrigger() {
			if(taskSpec.schedule() == Schedule.FIXED_RATE || taskSpec.schedule() == Schedule.FIXED_DELAY) {
				LOG.debug("{} is a repeating task, so leaving task.",  id);
			}
			else {
				taskInfo.remove(id);
			}
		}

		@Override
		void retrigger() {
			submitLocal(id, taskSpec, this);
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
				public void message(String text) {
					tinfo.message(text);
					machine.queue().submit(() -> {
						machine.sendRequest(null, new Request(Type.PROGRESS_MESSAGE, id, new ProgressMessagePayload(text)));
					});
				}
				
				@Override
				public void message(String bundle, String key, String... args) {
					tinfo.message(bundle, key, args);
					machine.queue().submit(() -> {
						machine.sendRequest(null, new Request(Type.PROGRESS_MESSAGE, id, new ProgressMessagePayload(bundle, key, args)));
					});
				}
				
				@Override
				public void progress(long value) {
					tinfo.progress = Optional.of(value);
					machine.queue().submit(() -> {
						machine.sendRequest(null, new Request(Type.PROGRESS, id, new ProgressPayload(value)));
					});
				}
				
				@Override
				public void start(long max) {
					tinfo.maxProgress = Optional.of(max);
					machine.queue().submit(() -> {
						machine.sendRequest(null, new Request(Type.START_PROGRESS, id, new StartProgressPayload(max)));
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
				machine.queue().submit(() -> {
					machine.sendRequest(null, new Request(Type.EXECUTING, id));
				});
				result = (Serializable) super.doTask(taskInfo, taskErrorContext);
				break;
			case ONLY_THIS:
				if (entry.submitter.equals(machine.address().toString())) {
					if(LOG.isDebugEnabled()) {
						LOG.debug(
								"Running task {} [{}] on this node because its affinity is {} and it was scheduled on this node.",
								id, task.id(), task.affinity());
					}
					machine.queue().submit(() -> {
						machine.sendRequest(null, new Request(Type.EXECUTING, id));
					});
					result = (Serializable) super.doTask(taskInfo, taskErrorContext);
				}
				else {
					return null;
				}
				break;
			case THIS:
				if (!machine.getMemberList().contains(entry.submitter) && machine.leader()) {
					if(LOG.isDebugEnabled()) {
						LOG.debug(
								"Running task {} [{}] on this node because its affinity is {} and the node it was scheduled on is not active and we are leader.",
								id, task.id(), task.affinity());
					}
					machine.queue().submit(() -> {
						machine.sendRequest(null, new Request(Type.EXECUTING, id));
					});
					result = (Serializable) super.doTask(taskInfo, taskErrorContext);
				}
				else if (entry.submitter.equals(machine.address().toString())) {
					if(LOG.isDebugEnabled()) {
						LOG.debug(
								"Running task {} [{}] on this node because its affinity is {} and it was scheduled on this node.",
								id, task.id(), task.affinity());
					}
					machine.queue().submit(() -> {
						machine.sendRequest(null, new Request(Type.EXECUTING, id));
					});
					result = (Serializable) super.doTask(taskInfo, taskErrorContext);
				}
				else {
					return null;
				}
				break;
			case ANY:
				if (id.getId() % machine.clusterSize() == machine.rank()) {
					if(LOG.isDebugEnabled()) {
						LOG.debug(
								"Running task {} [{}] on this node because its affinity is {} and it's rank of {} matches.",
								id, task.id(), task.affinity(), machine.rank());
					}
					machine.queue().submit(() -> {
						machine.sendRequest(null, new Request(Type.EXECUTING, id));
					});
					result = (Serializable) super.doTask(taskInfo, taskErrorContext);
				} else {
					if(LOG.isDebugEnabled()) {
						LOG.debug(
								"Not running task {} [{}] on this node because its affinity is {} and it's rank of {} does not match.",
								id, task.id(), task.affinity(), machine.rank());
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
				machine.acks().runWithAck(Request.Type.RESULT, id, machine.clusterSize(), () -> {
					machine.sendRequest(null, new Request(Request.Type.RESULT, id, rp));
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

	private final static Logger LOG = LoggerFactory.getLogger(DistributedScheduledExecutor.class);
	
	private static TaskSpec DEFAULT_TASK = new TaskSpec(Instant.EPOCH, Schedule.NOW, 0, 0, TimeUnit.MILLISECONDS, null);

	private final ScheduledExecutorService delegate;
	private final ConcurrentMap<ClusterID, TaskEntry> tasks = new ConcurrentHashMap<>();
	private final ConcurrentMap<ClusterID, TaskInfo> taskInfo = new ConcurrentHashMap<>();
	private final ConcurrentMap<ClusterID, Future<?>> localFutures = new ConcurrentHashMap<>();
	private final boolean alwaysDistribute;
	private final Optional<TaskErrorHandler> taskErrorHandler;
	private final Optional<TaskSuccessHandler> taskSuccessHandler;
	private final Optional<TaskStore> taskStore;
	private final boolean persistentByDefault;
	private final boolean deferStorageUntilStarted;
	private final List<TaskEntry> deferredStorage = new ArrayList<>();
	private final AtomicBoolean started = new AtomicBoolean();
	private final DistributedMachine machine;
	
	private int threads;
	private boolean startPaused;
	private boolean restoreTasksAtStartup;
	private Semaphore sem;

	
	private DistributedScheduledExecutor(Builder bldr) throws Exception {
		machine = bldr.machine;
		threads = bldr.schedulerThreads;
		delegate = Executors.newScheduledThreadPool(threads);
		startPaused = bldr.startPaused;
		taskErrorHandler = bldr.taskErrorHandler;
		taskSuccessHandler = bldr.taskSuccessHandler;
		taskStore = bldr.taskStore;
		persistentByDefault = bldr.persistentByDefault;
		alwaysDistribute = bldr.alwaysDistribute;
		restoreTasksAtStartup = bldr.restoreTasksAtStartup;
		taskStore.ifPresent(ts -> ts.initialise(this));
		deferStorageUntilStarted = bldr.deferStorageUntilStarted;
		
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

	@Override
	public boolean awaitTermination(long timeout, TimeUnit timeUnit) throws InterruptedException {
		return delegate.awaitTermination(timeout, timeUnit);
	}
	
	/**
	 * Note this is  in Java 19+ only, we want a shorter timeout
	 */
//	@Override
	public void close() {
		shutdown();
	}
	
	@Override
	public void execute(Runnable runnable) {
		submit(runnable);
	}
	
	@Override
	public void forceClose() {
		shutdownNow();
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
	public void handleLeftMember(Address mbr) {
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

	@Override
	public void handleNewMember(int wasRank, Address mbr) {
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

	@Override
	public void prepareClose() {
        if (!isTerminated()) {

    		/* Cancel tasks on shutdown so only actually running tasks will remain subject
    		 * to the timeout */ 
    		LOG.info("Cancelling locally scheduled tasks.");
    		taskInfo.entrySet().forEach(tsk -> {
    			LOG.info("Cancelling {}", tsk.getKey());
    			tsk.getValue().underlyingFuture.cancel(false); 
    		});
    		taskInfo.clear();
    		
    		releaseStartBlock();
        }
	}

	@Override
	public void receive(Message msg, Request req) {
		switch(req.type()) {
		case START_PROGRESS:
			handleStartProgress(msg.getSrc(), req);
			break;
		case PROGRESS:
			handleProgress(msg.getSrc(), req);
			break;
		case PROGRESS_MESSAGE:
			handleProgressMessage(msg.getSrc(), req);
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
			machine.sendRequest(msg.getSrc(), new Request(Request.Type.ACK, req.id(), new AckPayload(Request.Type.RESULT)));
			break;
		case REMOVE:
			handleRemove(msg.getSrc(), req);
			break;
		default:
			throw new UnsupportedOperationException("Unsupported request type: " + req.type());
		}
	}

	public void restore(TaskEntry entry) {
		if (!tasks.containsKey(entry.id) && shouldRun(entry)) {
			execute(entry, false);
		}
	}

	@SuppressWarnings("unchecked")
	@Override
	public <V> ScheduledFuture<V> schedule(Callable<V> callable, long delay, TimeUnit unit) {
		
		if (checkTask(callable)) {

			var dtask = (DistributedCallable<?>) callable;
			
			var id = dtask.id().
					map(i -> ClusterID.createNext(i)).
					orElseGet(() -> ClusterID.createNext(machine.address()));

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
				
				var id = ClusterID.createNext(machine.address());
				
				var sfut = delegate.schedule(() -> {
					try {
						return machine.payloadFilter().filter(callable).call();
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
		LOG.info("Shutting down distributed executor.");
        if (!isTerminated()) {
        	prepareClose();
        }

		try {
			delegate.shutdown();
		}
		finally {
			releaseStartBlock();
		}
	}

	@Override
	public List<Runnable> shutdownNow() {
		LOG.info("Requested shutdown now.");
        if (!isTerminated()) {
        	prepareClose();
        }
		try {
			return delegate.shutdownNow();
		}
		finally {
			releaseStartBlock();
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
					.orElseGet(() -> ClusterID.createNext(machine.address()));

			TaskEntry dfut = checkTaskId(id);
			if (dfut != null) {
				return (Future<T>)taskInfo.get(id).userFuture;
			}

			return doSubmit(id, dtask, DEFAULT_TASK);
			
		} else {
			if(alwaysDistribute) {
				if(task instanceof SerializableCallable cr)
					return submit(DistributedCallable.of(cr));
				else
					return (Future<T>) submit(DistributedCallable.of(new SerializableCallable<Serializable>() {
						@Override
						public Serializable call() throws Exception {
							return (Serializable)task.call();
						}
					}));
			}
			else {
				if (LOG.isDebugEnabled()) {
					LOG.debug("Not a {}, submitting locally only.", DistributedCallable.class.getName());
				}
				var id = ClusterID.createNext(machine.address());
				
				var sfut = delegate.submit(() -> {
					try {
						return machine.payloadFilter().filter(task).call();
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

	@Override
	public List<Type> types() {
		return List.of(
			Request.Type.SUBMIT, 
			Request.Type.RESULT, 
			Request.Type.REMOVE, 
			Request.Type.START_PROGRESS, 
			Request.Type.PROGRESS_MESSAGE, 
			Request.Type.EXECUTING, 
			Request.Type.PROGRESS
		);
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
					.orElseGet(() -> ClusterID.createNext(machine.address()));

			TaskEntry dfut = checkTaskId(id);
			if (dfut != null) {
				return (ScheduledFuture<V>) taskInfo.get(id).userFuture;
			}

			return (ScheduledFuture<V>)doSubmit(id, dtask, new TaskSpec(Instant.now(), schedule, initialDelay, period, unit, taskTrigger));
		} else {
			if(alwaysDistribute) {
				if(command instanceof SerializableRunnable sr) {
					return doRunnable(schedule, DistributedRunnable.of(sr), initialDelay, period, unit, taskTrigger);
				}
				else if(command instanceof Serializable) {
					return doRunnable(schedule, DistributedRunnable.of(()->command.run()), initialDelay, period, unit, taskTrigger);
				}
				else {
					return doRunnable(schedule, DistributedRunnable.local(() -> command.run()), initialDelay, period, unit, taskTrigger);
				}
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
				var id = ClusterID.createNext(machine.address());
				
				switch(schedule) {
				case ONE_SHOT:
					sfut = (ScheduledFuture<V>) delegate.schedule(() -> {
						try {
							machine.payloadFilter().filter(command).run();
						}
						finally {
							localFutures.remove(id);
						}
					}, initialDelay, unit);
					break;
				case FIXED_DELAY:
					sfut = (ScheduledFuture<V>) delegate.scheduleWithFixedDelay(machine.payloadFilter().filter(command), initialDelay, period, unit);
					break;
				case FIXED_RATE:
					sfut = (ScheduledFuture<V>) delegate.scheduleAtFixedRate(machine.payloadFilter().filter(command), initialDelay, period, unit);
					break;
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
				
				var entry = new TaskEntry(id, task, machine.address().toString(), spec);
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
				machine.queue().execute(() -> removeRequest(id));
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
			var offset = 0; //Instant.now().toEpochMilli() - entry.spec.submitted().toEpochMilli();
			
			ScheduledFuture<?> future = null;
			switch (entry.spec.schedule()) {
			case TRIGGER:
			{
				var nextFire = runNow ? Instant.now() : entry.spec.trigger().nextFire(createTriggerContext(entry.id));
				var msdely = Math.max(0, nextFire.toEpochMilli() - now.toEpochMilli()); 
				if(LOG.isDebugEnabled()) {
					LOG.debug("Next Fire for {} is {} in {} ms", entry.id, nextFire, msdely);
				}
				future = delegate.schedule(
					handler, 
					msdely,
					TimeUnit.MILLISECONDS
				);
				break;
			}
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
				
				var msdely = runNow 
					? 0 
					: Math.max(0l,  entry.spec.unit().toMillis(entry.spec.initialDelay()) - offset);
				
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
			
			tinfo.userFuture = new AbstractDelegateScheduledFuture<>(
					entry.id, 
					(Future<Object>)efuture, 
					entry.task.classifiers(), 
					entry.task.attributes()
				) {
				
				@Override
				public int compareTo(Delayed o) {
					return ffuture.compareTo(o);
				}

				@Override
				public long getDelay(TimeUnit unit) {
					return unit.convert(Duration.ofMillis(
							Math.max(
								0l, 
								ffuture.getDelay(TimeUnit.MILLISECONDS) - foffset
							)
						)
					);
				}

				@Override
				public TaskInfo info() {
					return tinfo;
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

	private int expectedResults(Affinity affinity) {
		switch(affinity) {
		case ALL:
			return machine.clusterSize();
		default:
			return 1;
		}
	}

	private void handleExecuting(Address sender, Request req) {
		if(sender.equals(machine.address()))
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
		if(sender.equals(machine.address()))
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

	private void handleProgressMessage(Address sender, Request req) {
		if(sender.equals(machine.address()))
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
		machine.sendRequest(sender, new Request(Request.Type.ACK, req.id(), new AckPayload(Request.Type.REMOVE)));
	}
	
	private void handleStartProgress(Address sender, Request req) {
		if(sender.equals(machine.address()))
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
	
	private void handleSubmit(ClusterID id, Address sender, DistributedTask<?> task, TaskSpec spec, boolean runNow) {
		var entry = new TaskEntry(id, task, sender.toString(), spec);
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
		machine.sendRequest(sender, new Request(Request.Type.ACK, id, new AckPayload(Request.Type.SUBMIT)));
	}

	private void releaseStartBlock() {
		if(!started.get()) {
			try {
				sem.release(threads);
			}
			finally {
				started.set(true);
			}
		}
	}

	private void removeRequest(ClusterID id) {
		var tsk = tasks.get(id);
		if(tsk == null) {
			throw new IllegalArgumentException("No task with ID " + id);
		}
		if(LOG.isDebugEnabled()) {
			LOG.debug("Sending remove request {} ({} in cluster right now)", id, machine.clusterSize());
		}
		
		try {
			machine.acks().runWithAck(Request.Type.REMOVE, id, machine.clusterSize(), () -> {
				machine.sendRequest(null, new Request(Request.Type.REMOVE, id));
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
		var acksReqd = mbr == null ? machine.clusterSize() : 1;
		machine.acks().runWithAck(Request.Type.SUBMIT, req.id(), acksReqd, () -> {
			machine.sendRequest(mbr, req);
		});
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
				int index = entry.id.getId() % machine.clusterSize();
				int rank = machine.rank();
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
				if (!machine.getMemberList().contains(entry.submitter) && machine.leader()) {
					if(LOG.isDebugEnabled()) {
						LOG.debug(
								"Running immediate task {} [{}] on this node because its affinity is {} and the node it was scheduled on is not active and we are leader",
								entry.id, entry.task.id(), entry.task.affinity());
					}
					run = true;
				}
				else if (entry.submitter.equals(machine.address().toString())) {
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
				if (entry.submitter.equals(machine.address().toString())) {
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
}
