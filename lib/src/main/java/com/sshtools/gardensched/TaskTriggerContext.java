package com.sshtools.gardensched;

import java.time.Instant;

public interface TaskTriggerContext {

	Instant lastScheduled();

	Instant lastCompleted();

	Instant lastExecuted();

}
