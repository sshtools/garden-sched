package com.sshtools.gardensched.spring.example;

import java.util.concurrent.TimeUnit;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpStatus;
import org.springframework.scheduling.support.CronTrigger;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.ResponseBody;
import org.springframework.web.bind.annotation.ResponseStatus;

import com.sshtools.gardensched.DistributedRunnable;
import com.sshtools.gardensched.DistributedScheduledExecutor;
import com.sshtools.gardensched.spring.TaskTriggerAdapter;

import jakarta.servlet.http.HttpServletRequest;
import jakarta.servlet.http.HttpServletResponse;

@Controller
public class SchedController {
	

	static Logger LOG = LoggerFactory.getLogger(SchedController.class);
	
	@Autowired
	private DistributedScheduledExecutor distributedScheduledExecutor;
	
	@RequestMapping(value = "/start-simple-job", method = RequestMethod.GET, produces = { "text/plain" })
	@ResponseBody
	@ResponseStatus(value = HttpStatus.OK)
	public String startJob(HttpServletRequest request,
			HttpServletResponse response) {
		
		distributedScheduledExecutor.schedule(() -> {
			System.out.println("I RUN ON " + DistributedScheduledExecutor.get().address());
		}, 10, TimeUnit.SECONDS);
		
		return "Job Started";
	}
	
	@RequestMapping(value = "/start-job-class", method = RequestMethod.GET, produces = { "text/plain" })
	@ResponseBody
	@ResponseStatus(value = HttpStatus.OK)
	public String startJobClass(HttpServletRequest request,
			HttpServletResponse response) {
		
		var  job = new TestJob("Hello World", 99);
		
		distributedScheduledExecutor.schedule(DistributedRunnable.of(job), 10, TimeUnit.SECONDS);
		
		return "Job Started";
	}
	
	@RequestMapping(value = "/start-trigger", method = RequestMethod.GET, produces = { "text/plain" })
	@ResponseBody
	@ResponseStatus(value = HttpStatus.OK)
	public String startTrugger(HttpServletRequest request,
			HttpServletResponse response) {
		
		var  job = new TestJob("Hello Trigger", 123);
		
		distributedScheduledExecutor.schedule(DistributedRunnable.of(job), new TaskTriggerAdapter(new CronTrigger("0 * * * * *")));
		
		return "Job Started";
	}
		
}
