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
package com.sshtools.gardensched.spring;

import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.util.Date;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;

import org.springframework.core.task.TaskRejectedException;
import org.springframework.scheduling.TaskScheduler;
import org.springframework.scheduling.Trigger;

import com.sshtools.gardensched.DistributedScheduledExecutor;

public class GardenSchedTaskScheduler implements TaskScheduler {
	private static final TimeUnit NANO = TimeUnit.NANOSECONDS;
	
	private final DistributedScheduledExecutor executor;
	private Clock clock = Clock.systemDefaultZone();

	public GardenSchedTaskScheduler(DistributedScheduledExecutor executor) {
		this.executor = executor;
	}

	
	@Override
	public ScheduledFuture<?> schedule(Runnable task, Trigger trigger) {
		return executor.schedule(task, new TaskTriggerAdapter(trigger));
	}

	@Override
	public ScheduledFuture<?> schedule(Runnable task, Date startTime) {
		var startInstant = Instant.ofEpochMilli(startTime.getTime());
		var delay = Duration.between(this.clock.instant(), startInstant);
		try {
			return executor.schedule(task, NANO.convert(delay), NANO);
		}
		catch (RejectedExecutionException ex) {
			throw new TaskRejectedException(executor, task, ex);
		}
	}

	@Override
	public ScheduledFuture<?> scheduleAtFixedRate(Runnable task, Date startTime, long period) {
		var startInstant = Instant.ofEpochMilli(startTime.getTime());
		var duration = Duration.ofMillis(period);
		var initialDelay = Duration.between(this.clock.instant(), startInstant);
		try {
			return executor.scheduleAtFixedRate(task,
					NANO.convert(initialDelay), NANO.convert(duration), NANO);
		}
		catch (RejectedExecutionException ex) {
			throw new TaskRejectedException(executor, task, ex);
		}
	}

	@Override
	public ScheduledFuture<?> scheduleAtFixedRate(Runnable task, long period) {
		try {
			return executor.scheduleAtFixedRate(task,
					0,  NANO.convert(Duration.ofMillis(period)), NANO);
		}
		catch (RejectedExecutionException ex) {
			throw new TaskRejectedException(executor, task, ex);
		}
	}

	@Override
	public ScheduledFuture<?> scheduleWithFixedDelay(Runnable task, Date startTime, long delay) {
		var startInstant = Instant.ofEpochMilli(startTime.getTime());
		var duration = Duration.ofMillis(delay);
		var initialDelay = Duration.between(this.clock.instant(), startInstant);
		try {
			return executor.scheduleWithFixedDelay(task,
					NANO.convert(initialDelay), NANO.convert(duration), NANO);
		}
		catch (RejectedExecutionException ex) {
			throw new TaskRejectedException(executor, task, ex);
		}
		
	}

	@Override
	public ScheduledFuture<?> scheduleWithFixedDelay(Runnable task, long delay) {
		return scheduleWithFixedDelay(task, Duration.ofMillis(delay));
	}

	@Override
	public ScheduledFuture<?> schedule(Runnable task, Instant startTime) {
		var delay = Duration.between(this.clock.instant(), startTime);
		try {
			return executor.schedule(task, NANO.convert(delay), NANO);
		}
		catch (RejectedExecutionException ex) {
			throw new TaskRejectedException(executor, task, ex);
		}
	}

	@Override
	public ScheduledFuture<?> scheduleAtFixedRate(Runnable task, Instant startTime, Duration period) {
		var initialDelay = Duration.between(this.clock.instant(), startTime);
		try {
			return executor.scheduleAtFixedRate(task,
					NANO.convert(initialDelay), NANO.convert(period), NANO);
		}
		catch (RejectedExecutionException ex) {
			throw new TaskRejectedException(executor, task, ex);
		}
	}

	@Override
	public ScheduledFuture<?> scheduleAtFixedRate(Runnable task, Duration period) {
		try {
			return executor.scheduleAtFixedRate(task,
					0, NANO.convert(period), NANO);
		}
		catch (RejectedExecutionException ex) {
			throw new TaskRejectedException(executor, task, ex);
		}
	}

	@Override
	public ScheduledFuture<?> scheduleWithFixedDelay(Runnable task, Instant startTime, Duration delay) {
		var initialDelay = Duration.between(this.clock.instant(), startTime);
		try {
			return executor.scheduleWithFixedDelay(task, 
					NANO.convert(initialDelay), NANO.convert(delay), NANO);
		}
		catch (RejectedExecutionException ex) {
			throw new TaskRejectedException(executor, task, ex);
		}
	}

	@Override
	public ScheduledFuture<?> scheduleWithFixedDelay(Runnable task, Duration delay) {
		try {
			return executor.scheduleWithFixedDelay(task, 
					0, NANO.convert(delay), NANO);
		}
		catch (RejectedExecutionException ex) {
			throw new TaskRejectedException(executor, task, ex);
		}
	}

}
