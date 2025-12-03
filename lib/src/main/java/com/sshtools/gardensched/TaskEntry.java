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

import java.util.concurrent.atomic.AtomicInteger;

import org.jgroups.Address;
import org.jgroups.util.Promise;

public class TaskEntry {
	
	final DistributedTask<?> task;
	final Address submitter;
	final Promise<Object> promise = new Promise<>();
	final AtomicInteger results;
	final TaskSpec spec;
	final ClusterID id;

	public TaskEntry(ClusterID id, DistributedTask<?> task, Address submitter, int expectedResults, TaskSpec spec) {
		this.task = task;
		this.id = id;
		this.submitter = submitter;
		this.results = new AtomicInteger(expectedResults);
		this.spec = spec;
	}
	
	public ClusterID id() {
		return id;
	}

	public DistributedTask<?> task() {
		return task;
	}

	public Address submitter() {
		return submitter;
	}

	public Promise<Object> promise() {
		return promise;
	}

	public AtomicInteger results() {
		return results;
	}

	public TaskSpec spec() {
		return spec;
	}
}