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
			<table>
			""";
		
		var middle = "";
		for(var future : distributedScheduledExecutor.futures()) {
			var row = """
					<tr>
						<td>%1</td>
						<td>%2</td>
						<td>%3</td>
					</tr>
					""".
					replace("%1", future.clusterID().toString()).
					replace("%2", future.info().active() ? "Active" : "Idle").
					replace("%3", String.join(", ", future.classifiers()));
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
