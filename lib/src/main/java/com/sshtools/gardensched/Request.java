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
		EXECUTE, EXECUTED, RESULT, REMOVE, REMOVED, UNLOCK, LOCK, LOCKED, UNLOCKED, EVENT
	}

	private Type type;
	private DistributedTask<?> task;
	private ClusterID id;
	private Object result;
	private TaskSpec spec;
	private String lockName;
	private Address locker;
	private Serializable event;

	public Request() {
	}

	Request(Type type, DistributedTask<?> task, ClusterID id, Object result, TaskSpec spec, Address locker,
			String lockName, Serializable event) {
		this.type = type;
		this.task = task;
		this.id = id;
		this.result = result;
		this.spec = spec;
		this.lockName = lockName;
		this.locker = locker;
		this.event = event;
	}

	public Address locker() {
		return locker;
	}

	public String lockName() {
		return lockName;
	}

	public Type type() {
		return type;
	}

	public DistributedTask<?> task() {
		return task;
	}

	public ClusterID id() {
		return id;
	}

	public Object result() {
		return result;
	}

	public TaskSpec spec() {
		return spec;
	}

	public Serializable event() {
		return event;
	}

	@Override
	public void writeTo(DataOutput out) throws IOException {
		out.writeInt(type.ordinal());
		Util.writeStreamable(spec, out);
		out.writeBoolean(task != null);
		if (task != null) {
			out.writeBoolean(task instanceof DistributedCallable);
			Util.writeStreamable(task, out);
		}
		Util.writeStreamable(id, out);
		Util.objectToStream(result, out);
		out.writeUTF(lockName);
		Util.writeAddress(locker, out);
		DistributedScheduledExecutor.currentSerializer().serialize(event, out);
	}

	@Override
	public void readFrom(DataInput in) throws IOException, ClassNotFoundException {
		type = Type.values()[in.readInt()];
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
		id = Util.readStreamable(ClusterID::new, in);
		result = Util.objectFromStream(in);
		lockName = in.readUTF();
		locker = Util.readAddress(in);
		event = DistributedScheduledExecutor.currentSerializer().deserialize(Serializable.class, in);
	}

}