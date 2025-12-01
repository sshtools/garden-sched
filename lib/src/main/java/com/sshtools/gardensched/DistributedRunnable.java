package com.sshtools.gardensched;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public interface DistributedRunnable extends Runnable, DistributedTask<Void, SerializableRunnable> {

	public final class DistributedRunnableImpl extends AbstractTask<Void, SerializableRunnable> implements DistributedRunnable {
		
		private SerializableRunnable task;
		
		public DistributedRunnableImpl() {
		}

		public DistributedRunnableImpl(AbstractBuilder<?, Void, ?> bldr, SerializableRunnable task) {
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
			DistributedScheduledExecutor.srlzr.get().serialize(task, out);
		}

		@Override
		public void readFrom(DataInput in) throws IOException, ClassNotFoundException {
			super.readFrom(in);
			task = (SerializableRunnable) DistributedScheduledExecutor.srlzr.get().deserialize(SerializableRunnable.class, in);
			task = (SerializableRunnable) DistributedScheduledExecutor.currentFilter().filter(task);
		}
	}
	
	public final static class Builder  extends AbstractBuilder<Builder, Void,  SerializableRunnable > {

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
	
	public static Builder builder(String id, SerializableRunnable task) {
		return new Builder(id, task);
	}
}
