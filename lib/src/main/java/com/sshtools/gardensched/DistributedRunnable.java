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

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public interface DistributedRunnable extends Runnable, DistributedTask<SerializableRunnable> {

	public final class DistributedRunnableImpl extends AbstractTask<SerializableRunnable> implements DistributedRunnable {
		
		private SerializableRunnable task;
		
		public DistributedRunnableImpl() {
		}

		public DistributedRunnableImpl(AbstractBuilder<?, ?, ?> bldr, SerializableRunnable task) {
			super(bldr);
			this.task = task;
		}

		public SerializableRunnable task() {
			return task;
		}

		@Override
		public void run() {
			task.run();
		}

		@Override
		public void writeTo(DataOutput out) throws IOException {
			super.writeTo(out);
			DistributedScheduledExecutor.currentSerializer().serialize(task, out);
		}

		@Override
		public void readFrom(DataInput in) throws IOException, ClassNotFoundException {
			super.readFrom(in);
			task = (SerializableRunnable) DistributedScheduledExecutor.currentSerializer().deserialize(SerializableRunnable.class, in);
			task = (SerializableRunnable) DistributedScheduledExecutor.currentFilter().filter(task);
		}

		@Override
		public String key() {
			return key == null ? task.getClass().getName() : key;
		}
	}
	
	public final static class Builder  extends AbstractBuilder<Builder, SerializableRunnable, DistributedRunnable > {

		public Builder( SerializableRunnable  task) {
			super(task);
		}

		public Builder(String id,  SerializableRunnable task) {
			super(id, task);
		}


		public DistributedRunnable build() {
			return new DistributedRunnableImpl(this, task);
		}
	}


	public static DistributedRunnable of(String id, SerializableRunnable task, String... classifiers) {
		return builder(id, task).withClassifiers(classifiers).build();
	}

	public static DistributedRunnable local(SerializableRunnable task, String... classifiers) {
		return builder(null, task).withAffinity(Affinity.LOCAL).withClassifiers(classifiers).build();
	}

	public static DistributedRunnable of(SerializableRunnable task, String... classifiers) {
		return builder(null, task).withClassifiers(classifiers).build();
	}

	public static Builder builder(SerializableRunnable task) {
		return builder(null, task);
	}
	
	public static Builder builder(String id, SerializableRunnable task) {
		return new Builder(id, task);
	}

}
