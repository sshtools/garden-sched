package com.sshtools.gardensched;

import java.io.Closeable;

public interface LockProvider {

	public interface Lock extends Closeable {
		@Override
		void close();
	}
	
	Lock acquireLock(String name) throws InterruptedException;
}
