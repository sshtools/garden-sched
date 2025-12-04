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

import java.time.Instant;
import java.util.Optional;
import java.util.concurrent.Future;

public final class TaskInfo {
	Instant lastScheduled;
	Optional<Instant> lastExecuted = Optional.empty();
	Optional<Instant> lastCompleted = Optional.empty();
	Optional<Throwable> lastError = Optional.empty();
	Future<?> underlyingFuture;
	IdentifiableFuture<?> userFuture;
	Optional<Long> maxProgress = Optional.empty();
	Optional<Long> progress = Optional.empty();
	Optional<String> message = Optional.empty();
	Optional<String> bundle = Optional.empty();;
	Optional<String> key = Optional.empty();
	Optional<String[]> args = Optional.empty();
	boolean active;
	TaskSpec spec;
	
	TaskInfo(TaskSpec spec, Instant lastScheduled, Future<?> underlyingFuture, TaskInfo previous) {
		this.lastScheduled = lastScheduled;
		this.underlyingFuture = underlyingFuture;
		this.spec = spec;
		if(previous != null) {
			userFuture = previous.userFuture;
			lastExecuted = previous.lastExecuted;
			lastCompleted = previous.lastCompleted;
			lastError  = previous.lastError;
			maxProgress = previous.maxProgress;
			progress = previous.progress;
			bundle = previous.bundle;
			key = previous.key;
			args = previous.args;
			message = previous.message;
		}
	}

	void message(String bundle, String key, String... args) {
		this.message = Optional.empty();
		this.bundle = Optional.of(bundle);
		this.key = Optional.of(key);
		this.args = Optional.of(args);
	}
	
	void message(String message) {
		this.message = Optional.of(message);
		this.bundle = Optional.empty();;
		this.key = Optional.empty();
		this.args = Optional.empty();
	}
	
	public TaskSpec spec() {
		return spec;
	}
	
	public boolean active() {
		return active;
	}

	public Optional<Long> maxProgress() {
		return maxProgress;
	}

	public Optional<Long> progress() {
		return progress;
	}

	public Optional<String> message() {
		return message;
	}

	public Optional<String> bundle() {
		return bundle;
	}

	public Optional<String> key() {
		return key;
	}

	public Optional<String[]> args() {
		return args;
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