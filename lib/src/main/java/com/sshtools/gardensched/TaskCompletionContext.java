package com.sshtools.gardensched;

import java.time.Duration;

public interface TaskCompletionContext {
	void retry(Duration delay);

	void cancel();
}