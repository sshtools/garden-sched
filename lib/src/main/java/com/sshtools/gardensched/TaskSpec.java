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

		DistributedScheduledExecutor.srlzr.get().serialize(trigger, out);

	}

	@Override
	public void readFrom(DataInput in) throws IOException, ClassNotFoundException {
		schedule = Schedule.values()[in.readInt()];
		initialDelay = in.readLong();
		period = in.readLong();
		unit = TimeUnit.values()[in.readInt()];
		submitted = Instant.ofEpochMilli(in.readLong());

		trigger = (TaskTrigger) DistributedScheduledExecutor.srlzr.get().deserialize(TaskTrigger.class, in);
		trigger = (TaskTrigger) DistributedScheduledExecutor.currentFilter().filter(trigger);
	}

	@Override
	public String toString() {
		return "TaskSpec [repetition=" + schedule + ", initialDelay=" + initialDelay + ", period=" + period
				+ ", unit=" + unit + "]";
	}

	public TaskSpec adjustTimes() {
		/* TODO nano resolution */
		var newSubmitted = Instant.now();
		var diff = newSubmitted.toEpochMilli() - submitted.toEpochMilli();
		switch(schedule) {
		case ONE_SHOT:
		{
			var initDelay = unit.toMillis(initialDelay);
			if(diff > initDelay) {
				/* Other node should have already fired */
				return null;
			}
			else {
				initDelay -= diff;
				initialDelay = unit.convert(initDelay, TimeUnit.MILLISECONDS);
			}
			break;
		}
		case FIXED_RATE:
		case FIXED_DELAY:
		{
			var initDelay = unit.toMillis(initialDelay);
			if(diff > initDelay) {
				/* Other node should have already fired at least once. Make
				 * the initial delay be what remains until approximately
				 * the next fire.
				 *
				 * Note, for FIXED_DELAY this wont be very accurate, as we
				 * don't know how long previous executions took
				 **/
				var delay = unit.toMillis(period);
				initialDelay = delay - ( ( initDelay - diff ) % delay );
			}
			else {
				initDelay -= diff;
				initialDelay = unit.convert(initDelay, TimeUnit.MILLISECONDS);
			}
			break;
		}
		default:
			break;
		}
		return this;
	}
}