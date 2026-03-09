package com.sshtools.gardensched.spring.example;

import java.util.concurrent.TimeUnit;

import org.springframework.beans.factory.annotation.Autowired;

import com.sshtools.gardensched.SerializableRunnable;
import com.sshtools.gardensched.TaskContext;

@SuppressWarnings("serial")
public class TestRandomReschedulingJob implements SerializableRunnable {
	
	private String param1;
	private int param2;
	
	@Autowired
	private RandomService randomService;


	public TestRandomReschedulingJob() {
	}
	

	public TestRandomReschedulingJob(String param1, int param2) {
		super();
		this.param1 = param1;
		this.param2 = param2;
	}


	public String getParam1() {
		return param1;
	}


	public void setParam1(String param1) {
		this.param1 = param1;
	}


	public int getParam2() {
		return param2;
	}


	public void setParam2(int param2) {
		this.param2 = param2;
	}


	@Override
	public void run() {
		
		if("error".equals(param1)) {
			throw new IllegalStateException("Task threw an error because it was told to!");
		}
		
		System.out.format("TestJob! Param1: %s  Param2: %d  Random: %d%n", 
				param1,  
				param2,
				randomService.nextNumber()
			);
		
		TaskContext.get().reschedule((long)(Math.random() * 30f), TimeUnit.SECONDS);
	}

}
