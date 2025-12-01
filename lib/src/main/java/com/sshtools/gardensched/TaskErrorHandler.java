package com.sshtools.gardensched;

import java.io.Serializable;
import java.time.Duration;

public interface TaskErrorHandler {

	public interface TaskErrorContext {
		void retry(Duration delay);

		void cancel();
	}

	void handleError(ClusterID id, TaskSpec spec, Serializable task, TaskErrorContext context, Throwable exception);
}
