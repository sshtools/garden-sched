package com.sshtools.gardensched.spring;

import java.time.Instant;

import org.jspecify.annotations.Nullable;
import org.springframework.scheduling.Trigger;
import org.springframework.scheduling.TriggerContext;

import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;
import com.sshtools.gardensched.TaskTrigger;
import com.sshtools.gardensched.TaskTriggerContext;

@SuppressWarnings("serial")
public final class TaskTriggerAdapter implements TaskTrigger {
	
	private Trigger trigger;

	public TaskTriggerAdapter() {
	}

	public TaskTriggerAdapter(Trigger trigger) {
		this.trigger = trigger;
	}

	@JsonSerialize(using = TaskTriggerSerializer.class/* , as=Object.class */)
	public Trigger getTrigger() {
		return trigger;
	}

	@JsonDeserialize(using = TaskTriggerDeserializer.class)
	public void setTrigger(Trigger trigger) {
		this.trigger = trigger;
	}

	@Override
	public Instant nextFire(TaskTriggerContext context) {
		return trigger.nextExecution(new TriggerContext() {
			
			@Override
			public @Nullable Instant lastScheduledExecution() {
				return context.lastScheduled();
			}
			
			@Override
			public @Nullable Instant lastCompletion() {
				return context.lastCompleted();
			}
			
			@Override
			public @Nullable Instant lastActualExecution() {
				return context.lastExecuted();
			}
		});
	}
	
}