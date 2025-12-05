package com.sshtools.gardensched.spring.example;

import java.io.Serializable;
import java.util.concurrent.TimeUnit;

import org.jgroups.Address;
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

import com.sshtools.gardensched.BroadcastEventListener;
import com.sshtools.gardensched.DistributedRunnable;
import com.sshtools.gardensched.DistributedScheduledExecutor;
import com.sshtools.gardensched.TaskContext;
import com.sshtools.gardensched.spring.TriggerAdapter;

import jakarta.annotation.PostConstruct;
import jakarta.servlet.http.HttpServletRequest;
import jakarta.servlet.http.HttpServletResponse;

@Controller
public class SchedController implements BroadcastEventListener {
	

	static Logger LOG = LoggerFactory.getLogger(SchedController.class);
	
	@Autowired
	private DistributedScheduledExecutor distributedScheduledExecutor;
	
	@PostConstruct
	private void setup() {
		distributedScheduledExecutor.addBroadcastListener(this);
	}
	
	@RequestMapping(value = "/list-jobs", method = RequestMethod.GET, produces = { "text/html" })
	@ResponseBody
	@ResponseStatus(value = HttpStatus.OK)
	public String listJobs(HttpServletRequest request,
			HttpServletResponse response) {
		
		var top = 
			"""
			<html>
			<body>
			<table border="1">
			<thead>
			<tr>
				<td>CID</td>
				<td>Status</td>
				<td>Classifiers</td>
				<td>%age</td>
				<td>Max</td>
				<td>Message</td>
				<td>Schedule</td>
				<td>Delay</td>
				<td>Period</td>
				<td>Unit</td>
				<td>Trigger</td>
			</tr>
			</thead>
			""";
		
		var middle = "";
		for(var future : distributedScheduledExecutor.futures()) {
			var info = future.info();
			var row = """
					<tr>
						<td>%1</td>
						<td>%2</td>
						<td>%3</td>
						<td>%4</td>
						<td>%5</td>
						<td>%6</td>
						<td>%7</td>
						<td>%8</td>
						<td>%9</td>
						<td>%A</td>
						<td>%B</td>
					</tr>
					""".
					replace("%1", future.clusterID().toString()).
					replace("%2", info.active() ? "Active" : "Idle").
					replace("%3", String.join(", ", future.classifiers())).
					replace("%4", info.progress().map(String::valueOf).orElse("None")).
					replace("%5", info.maxProgress().map(String::valueOf).orElse("None")).
					replace("%6", info.message().map(String::valueOf).orElseGet(() -> {
						return info.key().map(k -> info.bundle().orElse("*") + "/" + k + " : " + String.join(", ", info.args().orElse(new String[0]))).orElse("-");
					})).
					replace("%7", info.spec().schedule().name()).
					replace("%8", String.valueOf(info.spec().initialDelay())).
					replace("%9", String.valueOf(info.spec().period())).
					replace("%A", info.spec().unit().name()).
					replace("%B", String.valueOf(info.spec().trigger()));
			middle += row;
		}
		
		var bottom = 
			"""
			</table>
			</body>
			</html>
			""";
		
		return top +
			   middle +
			   bottom;
	}
	
	@RequestMapping(value = "/put-object", method = RequestMethod.GET, produces = { "text/plain" })
	@ResponseBody
	@ResponseStatus(value = HttpStatus.OK)
	public String putObject(HttpServletRequest request,
			HttpServletResponse response) {
		
		var obj = new TestObject((int)(Math.random() * 10), (int)(Math.random() * 10), (int)(Math.random() * 10));
		distributedScheduledExecutor.put("OBJECTS", "SomeKey", obj);
		
		return "Object Stored - " + obj;
	}
	
	@RequestMapping(value = "/get-object", method = RequestMethod.GET, produces = { "text/plain" })
	@ResponseBody
	@ResponseStatus(value = HttpStatus.OK)
	public String getObject(HttpServletRequest request,
			HttpServletResponse response) {
		
		return "Object Retrieved - " + distributedScheduledExecutor.get("OBJECTS", "SomeKey");
	}
	
	@RequestMapping(value = "/remove-object", method = RequestMethod.GET, produces = { "text/plain" })
	@ResponseBody
	@ResponseStatus(value = HttpStatus.OK)
	public String removeObject(HttpServletRequest request,
			HttpServletResponse response) {
		
		return "Object Removed - " + distributedScheduledExecutor.remove("OBJECTS", "SomeKey");
	}
	
	@RequestMapping(value = "/start-simple-job", method = RequestMethod.GET, produces = { "text/plain" })
	@ResponseBody
	@ResponseStatus(value = HttpStatus.OK)
	public String startJob(HttpServletRequest request,
			HttpServletResponse response) {
		
		distributedScheduledExecutor.schedule(() -> {
			System.out.println("I RUN ON " + TaskContext.get().address());
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
	
	@RequestMapping(value = "/start-long-job", method = RequestMethod.GET, produces = { "text/plain" })
	@ResponseBody
	@ResponseStatus(value = HttpStatus.OK)
	public String startLongJob(HttpServletRequest request,
			HttpServletResponse response) {
		
		var  job = new LongJob("Finally .. Hello World", 30);
		
		distributedScheduledExecutor.schedule(DistributedRunnable.of(job), 10, TimeUnit.SECONDS);
		
		return "Job Started";
	}
	
	@RequestMapping(value = "/start-repeating-long-job", method = RequestMethod.GET, produces = { "text/plain" })
	@ResponseBody
	@ResponseStatus(value = HttpStatus.OK)
	public String startRepeatingLongJob(HttpServletRequest request,
			HttpServletResponse response) {
		
		var  job = new LongJob("Finally .. Hello World", 30);
		
		distributedScheduledExecutor.scheduleAtFixedRate(DistributedRunnable.of(job), 10, 60, TimeUnit.SECONDS);
		
		return "Job Started";
	}
	
	@RequestMapping(value = "/send-event", method = RequestMethod.GET, produces = { "text/plain" })
	@ResponseBody
	@ResponseStatus(value = HttpStatus.OK)
	public String sendEvent(HttpServletRequest request,
			HttpServletResponse response) {
		
		distributedScheduledExecutor.event(new TestEvent("I am an event!"));
		
		return "Event sent";
	}
	
	@RequestMapping(value = "/start-trigger", method = RequestMethod.GET, produces = { "text/plain" })
	@ResponseBody
	@ResponseStatus(value = HttpStatus.OK)
	public String startTrugger(HttpServletRequest request,
			HttpServletResponse response) {
		
		var  job = new TestJob("Hello Trigger", 123);
		
		distributedScheduledExecutor.schedule(DistributedRunnable.of(job), new TriggerAdapter(new CronTrigger("0 * * * * *")));
		
		return "Job Started";
	}
	
	@RequestMapping(value = "/start-fixed-delay", method = RequestMethod.GET, produces = { "text/plain" })
	@ResponseBody
	@ResponseStatus(value = HttpStatus.OK)
	public String startFixedDelay(HttpServletRequest request,
			HttpServletResponse response) {
		
		var  job = new TestJob("Hello Fixed Delay", 123);
		
		distributedScheduledExecutor.scheduleWithFixedDelay(DistributedRunnable.of(job), 5, 5, TimeUnit.SECONDS);
		
		return "Job Started";
	}
	
	@RequestMapping(value = "/start-fixed-rate", method = RequestMethod.GET, produces = { "text/plain" })
	@ResponseBody
	@ResponseStatus(value = HttpStatus.OK)
	public String startFixedRate(HttpServletRequest request,
			HttpServletResponse response) {
		
		var  job = new TestJob("Hello Fixed Rate", 123);
		
		distributedScheduledExecutor.scheduleAtFixedRate(DistributedRunnable.of(job), 5, 5, TimeUnit.SECONDS);
		
		return "Job Started";
	}

	@Override
	public void accept(Address sender, Serializable event) {
		System.out.println("XXX EVENT: " + sender + " sent " + event);
		
	}
		
}
