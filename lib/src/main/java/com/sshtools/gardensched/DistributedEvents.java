/*
 * Copyright © 2025 JAdaptive Limited (support@jadaptive.com)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.sshtools.gardensched;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;

import org.jgroups.Address;
import org.jgroups.Message;

import com.sshtools.gardensched.Request.EventPayload;

public class DistributedEvents implements DistributedComponent, MessageReceiver {
	private final List<BroadcastEventListener> broadcastListeners = new ArrayList<>();
	private final DistributedMachine machine;
	private boolean closed;
	
	public DistributedEvents(DistributedMachine machine) {
		this.machine = machine;
		machine.addComponent(this);
		machine.addReceiver(this);
	}

	public void addBroadcastListener(BroadcastEventListener listener) {
		this.broadcastListeners.add(listener);
	}

	public void removeBroadcastListener(BroadcastEventListener listener) {
		this.broadcastListeners.remove(listener);
	}
	
	@Override
	public void prepareClose() {
	}

	@Override
	public void close() {
		closed = true;
		machine.removeComponent(this);
		machine.removeReceiver(this);
	}

	@Override
	public void forceClose() {
	}

	@Override
	public boolean awaitTermination(long millis, TimeUnit milliseconds) throws InterruptedException {
		return true;
	}

	@Override
	public List<Request.Type> types() {
		return List.of(Request.Type.EVENT);
	}

	@Override
	public void receive(Message msg, Request request) {
		if(closed) {
			return;
		}
		
		switch(request.type()) {
		case EVENT:
			handleEvent(msg.src(), request);
			break;
		default:
			throw new IllegalStateException("Received a request of type " + request.type() + " which is not supported by this component!");
		}
	}

	@Override
	public void handleLeftMember(Address mbr) {
	}

	@Override
	public void handleNewMember(int rank, Address mbr) {
	}
	
	public void event(Serializable event) {
		if(closed) {
			throw new IllegalStateException("Cannot send event, closed!");
		}
		machine.queue().execute(() -> {
			machine.sendRequest(null, new Request(Request.Type.EVENT, ClusterID.createNext(machine.address()), new  EventPayload(event)));
		});
	}

	private void handleEvent(Address sender, Request req) {
		for(var i = broadcastListeners.size() - 1 ; i >= 0 ; i--) {
			broadcastListeners.get(i).accept(sender, ((EventPayload)req.payload()).event());
		}
	}
}
