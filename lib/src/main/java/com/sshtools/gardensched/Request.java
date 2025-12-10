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
import java.io.Serializable;

import org.jgroups.Address;
import org.jgroups.util.Streamable;
import org.jgroups.util.Util;

import com.sshtools.gardensched.DistributedCallable.DistributedCallableImpl;
import com.sshtools.gardensched.DistributedRunnable.DistributedRunnableImpl;

public class Request implements Streamable {
	public enum Type {
		SUBMIT, EXECUTING, RESULT, REMOVE, UNLOCK, UNLOCKED, LOCK, LOCKED,EVENT, START_PROGRESS, PROGRESS, PROGRESS_MESSAGE, ACK, STORED_OBJECT, REMOVE_OBJECT, HAS_OBJECT, GET_OBJECT
	}
	
	public final static class StorePayload implements Streamable {

		private Serializable key;
		private String path;

		public StorePayload() { }
		
		public StorePayload(String path, Serializable key) {
			super();
			this.key = key;
			this.path = path;
		}

		public Serializable key() {
			return key;
		}

		public String path() {
			return path;
		}

		@Override
		public void writeTo(DataOutput out) throws IOException {
			DistributedScheduledExecutor.currentSerializer().serialize(key, out);
			out.writeUTF(path);
		}

		@Override
		public void readFrom(DataInput in) throws IOException, ClassNotFoundException {
			key = DistributedScheduledExecutor.currentSerializer().deserialize(Serializable.class, in);
			path = in.readUTF();
		}
	}
	
	public final static class AckPayload implements Streamable {

		private Type type;
		private Serializable result;

		public AckPayload() { }

		public AckPayload(Type type) {
			this(type, null);
		}
		
		public AckPayload(Type type, Serializable result) {
			super();
			this.type = type;
			this.result = result;
		}

		public Type type() {
			return type;
		}

		public Serializable result() {
			return result;
		}

		@Override
		public void writeTo(DataOutput out) throws IOException {
			out.writeUTF(type.name());
			DistributedScheduledExecutor.currentSerializer().serialize(result, out);
		}

		@Override
		public void readFrom(DataInput in) throws IOException, ClassNotFoundException {
			type = Type.valueOf(in.readUTF());
			result = DistributedScheduledExecutor.currentSerializer().deserialize(Serializable.class, in);
		}
	}
	
	public final static class ResultPayload implements Streamable {

		private Serializable result;

		public ResultPayload() { }
		
		public ResultPayload(Serializable result) {
			super();
			this.result = result;
		}

		public Serializable result() {
			return result;
		}

		@Override
		public void writeTo(DataOutput out) throws IOException {
			DistributedScheduledExecutor.currentSerializer().serialize(result, out);
		}

		@Override
		public void readFrom(DataInput in) throws IOException, ClassNotFoundException {
			result = DistributedScheduledExecutor.currentSerializer().deserialize(Serializable.class, in);
		}
	}

	public final static class ProgressMessagePayload implements Streamable {
		
		private String message;
		private String key;
		private String bundle;
		private String[] args;

		public ProgressMessagePayload() { }

		public ProgressMessagePayload(String message) {
			super();
			this.message = message;
		}

		public ProgressMessagePayload(String bundle, String key, String... args) {
			super();
			this.key = key;
			this.bundle = bundle;
			this.args = args;
		}

		public String message() {
			return message;
		}

		public String key() {
			return key;
		}

		public String bundle() {
			return bundle;
		}

		public String[] args() {
			return args;
		}

		@Override
		public void writeTo(DataOutput out) throws IOException {
			out.writeUTF(message);
			out.writeUTF(key);
			out.writeUTF(bundle);
			if(args == null)
				out.writeInt(0);
			else {
				out.writeInt(args.length);
				for(var a : args) {
					out.writeUTF(a);
				}
			}
		}

		@Override
		public void readFrom(DataInput in) throws IOException, ClassNotFoundException {
			message = in.readUTF();
			key = in.readUTF();
			bundle = in.readUTF();
			var a = in.readInt();
			args = new String[a];
			for(var i = 0 ; i < a ; i++) {
				args[i] = in.readUTF();
			}
		}
	}

	public final static class StartProgressPayload implements Streamable {
		
		private long max;
		public StartProgressPayload() { }
		
		public StartProgressPayload(long max) {
			super();
			this.max = max;
		}

		public long max() {
			return max;
		}

		@Override
		public void writeTo(DataOutput out) throws IOException {
			out.writeLong(max);
		}

		@Override
		public void readFrom(DataInput in) throws IOException, ClassNotFoundException {
			max = in.readLong();
		}
	}

	public final static class ProgressPayload implements Streamable {
		
		private long progress;
		
		public ProgressPayload() { }
		
		public ProgressPayload(long progress) {
			super();
			this.progress = progress;
		}

		public long progress() {
			return progress;
		}

		@Override
		public void writeTo(DataOutput out) throws IOException {
			out.writeLong(progress);
		}

		@Override
		public void readFrom(DataInput in) throws IOException, ClassNotFoundException {
			progress = in.readLong();
		}
		
	}
	
	public final static class LockPayload implements Streamable {
		private String lockName;
		private Address locker;

		public LockPayload() {}

		public LockPayload(String lockName, Address locker) {
			super();
			this.lockName = lockName;
			this.locker = locker;
		}

		@Override
		public void writeTo(DataOutput out) throws IOException {
			out.writeUTF(lockName);
			Util.writeAddress(locker, out);
		}

		@Override
		public void readFrom(DataInput in) throws IOException, ClassNotFoundException {
			lockName = in.readUTF();
			locker = Util.readAddress(in);
		}

		public Address locker() {
			return locker;
		}

		public String lockName() {
			return lockName;
		}
		
	}

	public final static class SubmitPayload implements Streamable {
		private DistributedTask<?> task;
		private TaskSpec spec;
		private boolean runNow;
		
		public SubmitPayload() {}

		public SubmitPayload(DistributedTask<?> task, TaskSpec spec) {
			this(task, spec, false);
		}
		

		public SubmitPayload(DistributedTask<?> task, TaskSpec spec, boolean runNow) {
			super();
			this.task = task;
			this.spec = spec;
			this.runNow = runNow;
		}

		public DistributedTask<?> task() {
			return task;
		}

		public TaskSpec spec() {
			return spec;
		}

		@Override
		public void writeTo(DataOutput out) throws IOException {
			out.writeBoolean(runNow);
			Util.writeStreamable(spec, out);
			out.writeBoolean(task != null);
			if (task != null) {
				out.writeBoolean(task instanceof DistributedCallable);
				Util.writeStreamable(task, out);
			}
		}

		@Override
		public void readFrom(DataInput in) throws IOException, ClassNotFoundException {
			runNow = in.readBoolean();
			spec = Util.readStreamable(TaskSpec::new, in);
			var haveTask = in.readBoolean();
			if (haveTask) {
				var isCallable = in.readBoolean();
				if(isCallable) {
					task = Util.readStreamable(DistributedCallableImpl::new, in);
				}
				else {
					task = (DistributedTask<?>) Util.readStreamable(DistributedRunnableImpl::new, in);
				}
			} else {
				task = null;
			}
			
		}

		public boolean runNow() {
			return runNow;
		}
		
	}
	
	public final static class EventPayload implements Streamable {
		private Serializable event;
		
		public EventPayload() {}
		
		public EventPayload(Serializable event) {
			this.event = event;
		}

		@Override
		public void writeTo(DataOutput out) throws IOException {
			DistributedScheduledExecutor.currentSerializer().serialize(event, out);
		}

		@Override
		public void readFrom(DataInput in) throws IOException, ClassNotFoundException {
			event = DistributedScheduledExecutor.currentSerializer().deserialize(Serializable.class, in);
		}
		
		public  Serializable event() {
			return  event;
		}
	}

	private Type type;
	private ClusterID id;
	private Streamable payload;

	public Request() {
	}

	Request(Type type, ClusterID id) {
		this(type, id, null);
	}

	Request(Type type, Streamable payload) {
		this(type, null, payload);
	}

	Request(Type type, ClusterID id, Streamable payload) {
		this.type = type;
		this.id = id;
		this.payload = payload;
	}
	
	@SuppressWarnings("unchecked")
	public <S extends Streamable> S payload() {
		return (S)payload;
	}

	public Type type() {
		return type;
	}

	public ClusterID id() {
		return id;
	}

	@Override
	public void writeTo(DataOutput out) throws IOException {
		out.writeInt(type.ordinal());
		Util.writeStreamable(id, out);
		if(payload != null)
			payload.writeTo(out);
	}

	@Override
	public void readFrom(DataInput in) throws IOException, ClassNotFoundException {
		type = Type.values()[in.readInt()];
		id = Util.readStreamable(ClusterID::new, in);
		switch(type) {
		case EVENT:
			payload = new EventPayload();
			break;
		case SUBMIT:
			payload = new SubmitPayload();
			break;
		case LOCK:
		case LOCKED:
		case UNLOCK:
		case UNLOCKED:
			payload = new LockPayload();
			break;
		case RESULT:
			payload = new ResultPayload();
			break;
		case START_PROGRESS:
			payload = new StartProgressPayload();
			break;
		case PROGRESS:
			payload = new ProgressPayload();
			break;
		case PROGRESS_MESSAGE:
			payload = new ProgressMessagePayload();
			break;
		case ACK:
			payload = new AckPayload();
			break;
		case STORED_OBJECT:
		case REMOVE_OBJECT:
		case GET_OBJECT:
		case HAS_OBJECT:
			payload = new StorePayload();
			break;
		default:
			payload = null;
			break;
		}
		if(payload != null) {
			payload.readFrom(in);
		}
	}

}