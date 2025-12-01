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