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
package com.sshtools.gardensched.spring;

import java.time.Instant;

import org.springframework.scheduling.Trigger;
import org.springframework.scheduling.TriggerContext;

import com.sshtools.gardensched.TaskTrigger;
import com.sshtools.gardensched.TaskTriggerContext;

@SuppressWarnings("serial")
public class TriggerAdapter implements TaskTrigger {
	
	private Trigger trigger;

	public TriggerAdapter() {
	}

	public TriggerAdapter(Trigger trigger) {
		this.trigger = trigger;
	}

	public Trigger getTrigger() {
		return trigger;
	}

	public void setTrigger(Trigger trigger) {
		this.trigger = trigger;
	}

	@Override
	public Instant nextFire(TaskTriggerContext context) {
		return trigger.nextExecution(new TriggerContext() {
			
			@Override
			public Instant lastScheduledExecution() {
				return context.lastScheduled();
			}
			
			@Override
			public Instant lastCompletion() {
				return context.lastCompleted();
			}
			
			@Override
			public Instant lastActualExecution() {
				return context.lastExecuted();
			}
		});
	}

	@Override
	public String toString() {
		return trigger.toString();
	}
	
}