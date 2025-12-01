package com.sshtools.gardensched;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.concurrent.ScheduledExecutorService;

public class DistributedExecutors {

	public static ScheduledExecutorService newExecutorService() {
		try {
			return new DistributedScheduledExecutor.Builder().build();
		} catch (RuntimeException re) {
			throw re;
		} catch (IOException re) {
			throw new UncheckedIOException(re);
		} catch (Exception e) {
			throw new IllegalStateException(e);
		}
	}
}
