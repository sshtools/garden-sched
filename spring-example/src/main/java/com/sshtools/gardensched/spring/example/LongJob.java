package com.sshtools.gardensched.spring.example;

import org.springframework.beans.factory.annotation.Autowired;

import com.sshtools.gardensched.SerializableRunnable;
import com.sshtools.gardensched.TaskContext;

@SuppressWarnings("serial")
public class LongJob implements SerializableRunnable {
	
	private String param1;
	private int param2;
	
	@Autowired
	private RandomService randomService;


	public LongJob() {
	}
	

	public LongJob(String param1, int param2) {
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
		var taskContext = TaskContext.get();
		var prg = taskContext.progress();
		prg.message("Starting job .. ");
		prg.start(param2);
		for(int i = 0 ; i < param2 ; i++) {
			prg.message("Tick " + i);
			try {
				Thread.sleep(1000);
			} catch (InterruptedException e) {
				throw new IllegalStateException(e);
			}
			prg.progress(i + 1);
		}
		System.out.format("TestJob! Param1: %s  Param2: %d  Random: %d%n", 
				param1,  
				param2,
				randomService.nextNumber()
			);
	}

}
