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
import java.util.concurrent.Callable;

public interface DistributedCallable<RESULT> extends Callable<RESULT>, DistributedTask<RESULT, SerializableCallable<RESULT>> {

	public final class DistributedCallableImpl<RESULT> extends AbstractTask<RESULT, SerializableCallable<RESULT>> implements DistributedCallable<RESULT> {
		
		private SerializableCallable<RESULT> task;
		
		public DistributedCallableImpl() {
		}

		DistributedCallableImpl(AbstractBuilder<?, RESULT, ?> bldr, SerializableCallable<RESULT> task) {
			super(bldr);
			this.task = task;
		}

		public SerializableCallable<RESULT> task() {
			return task;
		}

		@Override
		public RESULT call() throws Exception {
			return task.call();
		}

		@Override
		public void writeTo(DataOutput out) throws IOException {
			super.writeTo(out);
			DistributedScheduledExecutor.srlzr.get().serialize(task, out);
		}

		@SuppressWarnings("unchecked")
		@Override
		public void readFrom(DataInput in) throws IOException, ClassNotFoundException {
			super.readFrom(in);
			task = (SerializableCallable<RESULT>) DistributedScheduledExecutor.srlzr.get().deserialize(SerializableCallable.class, in);
			task = (SerializableCallable<RESULT>) DistributedScheduledExecutor.currentFilter().filter(task);
		}
	}
	
	public final static class Builder<RESULT> extends AbstractBuilder<Builder<RESULT>, RESULT,  SerializableCallable<RESULT> > {

		public Builder( SerializableCallable<RESULT>  task) {
			super(task);
		}

		public Builder(String id,  SerializableCallable<RESULT>  task) {
			super(id, task);
		}


		public DistributedCallable<RESULT> build() {
			return new DistributedCallableImpl<>(this, task);
		}
	}

	public static <RESULT> DistributedCallable<RESULT> of(SerializableCallable<RESULT> task, String... classifiers) {
		return builder(null, task).withClassifiers(classifiers).build();
	}

	public static <RESULT> DistributedCallable<RESULT> local(SerializableCallable<RESULT> task, String... classifiers) {
		return builder(null, task).withClassifiers(classifiers).withAffinity(Affinity.LOCAL).build();
	}

	public static <RESULT> DistributedTask<RESULT, SerializableCallable<RESULT>> of(String id, SerializableCallable<RESULT> task, String... classifiers) {
		return builder(id, task).withClassifiers(classifiers).build();
	}
	
	public static <RESULT> Builder<RESULT> builder(String id, SerializableCallable<RESULT> task) {
		return new Builder<>(id, task);
	}
}
