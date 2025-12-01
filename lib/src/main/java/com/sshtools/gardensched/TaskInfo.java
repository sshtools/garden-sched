package com.sshtools.gardensched;

import java.time.Instant;
import java.util.Optional;
import java.util.concurrent.Future;

public final class TaskInfo {
	Instant lastScheduled;
	Optional<Instant> lastExecuted = Optional.empty();
	Optional<Instant> lastCompleted = Optional.empty();
	Optional<Throwable> lastError = Optional.empty();
	Future<?> future;
	
	TaskInfo(Instant lastScheduled, Future<?> future, TaskInfo previous) {
		this.lastScheduled = lastScheduled;
		this.future = future;
		if(previous != null) {
			lastExecuted = previous.lastExecuted;
			lastCompleted = previous.lastCompleted;
			lastError  = previous.lastError;
		}
	}
	
	public Instant lastScheduled() {
		return lastScheduled;
	}
	
	public Optional<Instant> lastExecuted() {
		return lastExecuted;
	}
	
	public Optional<Instant> lastCompleted() {
		return lastCompleted;
	}
	
	public Optional<Throwable> lastError() {
		return lastError;
	}
}