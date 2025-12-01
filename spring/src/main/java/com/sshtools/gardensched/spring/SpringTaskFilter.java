package com.sshtools.gardensched.spring;

import org.springframework.context.ApplicationContext;

import com.sshtools.gardensched.TaskFilter;

public class SpringTaskFilter implements TaskFilter {
	
	private ApplicationContext context;

	public  SpringTaskFilter(ApplicationContext context) {
		this.context = context;
	}

	@Override
	public <O> O filter(O task) {
		if(task != null)
			context.getAutowireCapableBeanFactory().autowireBean(task);
		return task;
	}
}
