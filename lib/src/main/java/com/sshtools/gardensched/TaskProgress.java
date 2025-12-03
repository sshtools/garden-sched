package com.sshtools.gardensched;

public interface TaskProgress {
	
	public final static long INDETERMINATED = Long.MAX_VALUE;

	void start(long max);
	
	void progress(long value);
	
	void message(String text);
	
	void message(String bundle, String key, Object... args);
}
