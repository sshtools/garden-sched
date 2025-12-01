package com.sshtools.gardensched;

import java.util.Date;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import org.jgroups.util.Util;

public class Tester {

	public static void main(String[] args) throws Exception {
        loop(DistributedExecutors.newExecutorService());
    }

    private static void loop(ScheduledExecutorService server) throws InterruptedException, ExecutionException {
        boolean looping=true;
        Future<Date> future = null;
        while(looping) {

        	try {
	            int key=Util.keyPress("""
	            		[1] Submit
	            		[2] Submit long running task
	            		[3] Submit All
	            		[4] Schedule+Get 10 Secs 
	            		[5] Schedule with ID 10 Secs (throw)
	            		[6] Schedule with ID 10 Secs (ignore)
	            		[7] Schedule with ID 10 Secs (replace)
	            		[c] Cancel last
	            		[q] Quit
	            		""");
	            switch(key) {
	            
	                case '1':
	                	System.out.println((future = server.submit(DistributedCallable.of(() -> {
	                		System.out.println("Calculating date here");
	                    	return new Date();
	                    }))).get());
	                    break;
	                case '2':
	                	System.out.println((future = server.submit(DistributedCallable.of(() -> {
	                        System.out.println("sleeping for 15 secs...");
	                        Util.sleep(15000);
	                        System.out.println("done");
	                        return new Date();
	                    }))).get());
	                    break;
	                case '3':
	                	
	                	System.out.println((future = server.submit(new DistributedCallable.Builder<>(() -> {
	                		System.out.println("I RUN HERE");
	                    	return new Date();
	                	}).withAffinity(Affinity.ALL).build())).get());
	                    break;
	                case '4':
	                	System.out.println((future = server.schedule(new DistributedCallable.Builder<>(() -> {
	                		System.out.println("I RUN HERE");
	                    	return new Date();
	                	}).build(), 10, TimeUnit.SECONDS)).get());
	                    break;
	                case '5':
	                	System.out.println(future = server.schedule(new DistributedCallable.Builder<>("MYID", () -> {
	                		System.out.println("I RUN HERE " + System.currentTimeMillis());
	                    	return new Date();
	                	}).build(), 10, TimeUnit.SECONDS));
	                    break;
	                case '6':
	                	System.out.println(future = server.schedule(new DistributedCallable.Builder<>("MYID", () -> {
	                		System.out.println("I RUN HERE " + System.currentTimeMillis());
	                    	return new Date();
	                	}).onConflict(ConflictResolution.IGNORE).build(), 10, TimeUnit.SECONDS));
	                    break;
	                case '7':
	                	System.out.println(future = server.schedule(new DistributedCallable.Builder<>("MYID", () -> {
	                		System.out.println("I RUN HERE");
	                    	return new Date();
	                	}).onConflict(ConflictResolution.REPLACE).build(), 10, TimeUnit.SECONDS));
	                    break;
	                case 'c':
	                	if(future == null)
	                		System.out.println("Nothing to cancel");
	                	else {
		                	future.cancel(false);
		                	System.out.println("CANCELLED " + future);
		                	future = null;
	                	}
	                	break;
	                case 'q':
	                case -1:
	                    looping=false;
	                    break;
	                case 'r':
	                case '\n':
	                    break;
	            }
        	}
        	catch(Exception e) {
        		e.printStackTrace();
        	}
        }
        server.shutdown();
    }
}
