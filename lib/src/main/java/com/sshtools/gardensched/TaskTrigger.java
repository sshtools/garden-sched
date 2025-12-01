package com.sshtools.gardensched;

import java.io.Serializable;
import java.time.Instant;

public interface TaskTrigger extends Serializable {

	Instant nextFire(TaskTriggerContext context);
}
