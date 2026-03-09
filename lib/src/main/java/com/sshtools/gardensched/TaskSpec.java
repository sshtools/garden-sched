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
import java.time.Instant;
import java.util.concurrent.TimeUnit;

import org.jgroups.util.Streamable;

public class TaskSpec implements Streamable {

	private Schedule schedule;
	private long initialDelay;
	private long period;
	private TimeUnit unit;
	private Instant submitted;
	private TaskTrigger trigger;

	public TaskSpec() {
	}

	public TaskSpec(Instant submitted, Schedule schedule, long initialDelay, long period, TimeUnit unit, TaskTrigger trigger) {
		this.schedule = schedule;
		this.initialDelay = initialDelay;
		this.period = period;
		this.unit = unit;
		this.submitted = submitted;
		this.trigger = trigger;
	}

	public TaskTrigger trigger() {
		return trigger;
	}

	public Instant submitted() {
		return submitted;
	}

	public Schedule schedule() {
		return schedule;
	}

	public long initialDelay() {
		return initialDelay;
	}

	public long period() {
		return period;
	}

	public TimeUnit unit() {
		return unit;
	}

	@Override
	public void writeTo(DataOutput out) throws IOException {
		out.writeInt(schedule.ordinal());
		out.writeLong(initialDelay);
		out.writeLong(period);
		out.writeInt(unit.ordinal());
		out.writeLong(submitted.toEpochMilli());

		DistributedScheduledExecutor.currentSerializer().serialize(trigger, out);

	}

	@Override
	public void readFrom(DataInput in) throws IOException, ClassNotFoundException {
		schedule = Schedule.values()[in.readInt()];
		initialDelay = in.readLong();
		period = in.readLong();
		unit = TimeUnit.values()[in.readInt()];
		submitted = Instant.ofEpochMilli(in.readLong());

		trigger = (TaskTrigger) DistributedScheduledExecutor.currentSerializer().deserialize(TaskTrigger.class, in);
		trigger = (TaskTrigger) DistributedScheduledExecutor.currentFilter().filter(trigger);
	}

	@Override
	public String toString() {
		return "TaskSpec [repetition=" + schedule + ", initialDelay=" + initialDelay + ", period=" + period
				+ ", unit=" + unit + "]";
	}
}