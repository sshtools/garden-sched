package com.sshtools.gardensched;

import java.io.IOException;
import java.io.UncheckedIOException;

public interface ThrowingRunnable extends Runnable {
	default void run() {
		try {
			execute();
		}
		catch(IOException ioe) {
			throw new UncheckedIOException(ioe);
		}
		catch(RuntimeException re) {
			throw re;
		}
		catch(Exception e) {
			throw new IllegalStateException(e.getMessage() == null ? "Job failed." : e.getMessage(), e);
		}
	}
	
	void execute() throws Exception;
}