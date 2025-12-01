package com.sshtools.gardensched;

public interface TaskFilter {

	<O> O filter(O task);

	static TaskFilter nullFilter() {
		return new TaskFilter() {
			@Override
			public <O> O filter(O task) {
				return task;
			}
		};
	}

}
