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

import java.io.IOException;
import java.io.UncheckedIOException;
import java.time.Duration;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import org.jgroups.Address;
import org.jgroups.BytesMessage;
import org.jgroups.JChannel;
import org.jgroups.Message;
import org.jgroups.Receiver;
import org.jgroups.View;
import org.jgroups.util.Util;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.sshtools.gardensched.Request.AckPayload;

public final class DistributedMachine {

	public final static class Builder {
		private String props = "udp.xml";
		private String clusterName = "garden-sched";
		private String groupName = null;
		private Optional<PayloadSerializer> payloadSerializer = Optional.empty();
		private Optional<PayloadFilter> payloadFilter = Optional.empty();
		private Duration acknowledgeTimeout = Duration.ofSeconds(10);
		private Duration closeTimeout = Duration.of(1, ChronoUnit.DAYS);

		public DistributedMachine build() throws Exception {
			return new DistributedMachine(this);
		}

		public Builder withAcknowledgeTimeout(Duration acknowledgeTimeout) {
			this.acknowledgeTimeout = acknowledgeTimeout;
			return this;
		}

		public Builder withCloseTimeout(Duration closeTimeout) {
			this.closeTimeout = closeTimeout;
			return this;
		}

		public Builder withClusterName(String clusterName) {
			this.clusterName = clusterName;
			return this;
		}

		public Builder withGroupName(String groupName) {
			this.groupName = groupName;
			return this;
		}

		public Builder withJGroupsProps(String props) {
			this.props = props;
			return this;
		}

		public Builder withPayloadFilter(PayloadFilter payloadFilter) {
			this.payloadFilter = Optional.of(payloadFilter);
			return this;
		}

		public Builder withPayloadSerializer(PayloadSerializer payloadSerializer) {
			this.payloadSerializer = Optional.of(payloadSerializer);
			return this;
		}

	}

	private class SchedReceiver implements Receiver {

		@Override
		public void receive(Message msg) {
			if (msg instanceof BytesMessage bmsg) {

				srlzr.set(payloadSerializer);
				fltr.set(payloadFilter);
				try {
					Request req = Util.streamableFromByteBuffer(Request.class, bmsg.getBytes(), msg.getOffset(),
							msg.getLength());
					switch (req.type()) {
					case ACK:
						AckPayload ack = req.payload();
						acks.ack(ack.type(), req.id(), ack.result());
						break;
					default:
						var receiver = receivers.get(req.type());
						if (receiver != null) {
							receiver.receive(msg, req);
						} else
							throw new IllegalArgumentException("Type " + req.type() + " is not recognized");
					}
				} catch (Exception e) {
					LOG.error("Exception receiving message from {}", msg.getSrc(), e);
				} finally {
					srlzr.remove();
					fltr.remove();
				}
			} else {
				LOG.warn("Unexpected message: {}", msg);
			}
		}

		@Override
		public void viewAccepted(View view) {
			acceptView(view);
		}
	}

	private final static Logger LOG = LoggerFactory.getLogger(DistributedMachine.class);

	private static final ThreadLocal<PayloadSerializer> srlzr = new ThreadLocal<>();
	private static final ThreadLocal<PayloadFilter> fltr = new ThreadLocal<>();

	public static PayloadFilter currentFilter() {
		return fltr.get();
	}

	public static PayloadSerializer currentSerializer() {
		return srlzr.get();
	}

	private final PayloadSerializer payloadSerializer;
	private final JChannel ch;
	private final Duration acknowledgeTimeout;
	private final List<NodeListener> listeners = new ArrayList<>();
	private final Duration closeTimeout;
	private final Ack acks;
	private final Map<Request.Type, MessageReceiver> receivers = new HashMap<>();
	private final List<DistributedComponent> components = new ArrayList<>();

	private View view;
	private int rank = -1;
	private int clusterSize = -1;
	private final PayloadFilter payloadFilter;
	private final ExecutorService queue;

	private boolean terminated;

	private DistributedMachine(Builder bldr) throws Exception {
		payloadSerializer = bldr.payloadSerializer.orElseGet(PayloadSerializer::defaultSerializer);
		acknowledgeTimeout = bldr.acknowledgeTimeout;
		closeTimeout = bldr.closeTimeout;
		payloadFilter = bldr.payloadFilter.orElseGet(() -> PayloadFilter.nullFilter());
		acks = new Ack(acknowledgeTimeout);
		queue = Executors.newSingleThreadExecutor();

		ch = new JChannel(bldr.props);
		ch.name(bldr.groupName);
		ch.setReceiver(new SchedReceiver());
		ch.connect(bldr.clusterName);
	}

	void addComponent(DistributedComponent component) {
		this.components.add(component);
		LOG.info("Registered component {}", component);
	}

	void removeComponent(DistributedComponent component) {
		this.components.remove(component);
		LOG.info("Deregistered component {}", component);
	}

	void removeReceiver(MessageReceiver receiver) {
		receiver.types().forEach(t -> {
			if (receivers.remove(t, receiver)) {
				LOG.info("Deregistered receiver {} for type {}", receiver, t);
			} else {
				LOG.warn("Couldn't deregister receiver {} for type {}, it was not registered", receiver, t);
			}
		});
	}

	void addReceiver(MessageReceiver receiver) {
		receiver.types().forEach(t -> {
			if (receivers.putIfAbsent(t, receiver) != null) {
				throw new IllegalStateException("Receiver for type " + t + " already exists");
			}
			LOG.info("Registered receiver {} for type {}", receiver, t);
		});
	}

	public void addListener(NodeListener listener) {
		this.listeners.add(listener);
	}

	public Address address() {
		return ch.getAddress();
	}

	/**
	 * Note this is in Java 19+ only, we want a shorter timeout
	 */
//	@Override
	public void close() {
		LOG.info("Closing distributed machine.");
		boolean closed = isClosed();

		if (!closed) {
			var componentsToClose = new ArrayList<>(components);
			componentsToClose.forEach(c -> {
				LOG.info("Preparing to close component {}", c);
				c.prepareClose();
			});

			try {
				LOG.info("Closing channel.");
				ch.close();
			} finally {
				LOG.info("Shutting down executor.");
				componentsToClose.forEach(c -> {
					LOG.info("Closing component {}", c);
					c.close();
				});
				;
				LOG.info("Shutting down internal operations queue.");
				queue.shutdown();
			}

			boolean interrupted = false;
			try {
				LOG.info("Awaiting termination ....");
				for (var c : componentsToClose) {
					closed |= c.awaitTermination(closeTimeout.toMillis(), TimeUnit.MILLISECONDS);
				}
			} catch (InterruptedException e) {
				if (!interrupted) {
					LOG.info("Forcibly terminating");
					componentsToClose.forEach(c -> {
						LOG.info("Closing component {}", c);
						c.forceClose();
					});
					;
					interrupted = true;
				}
			}
			if (interrupted) {
				Thread.currentThread().interrupt();
			}
		}
	}

	public boolean isClosed() {
		return terminated;
	}

	public boolean leader() {
		return rank == 0;
	}

	public int rank() {
		return rank;
	}

	public void removeListener(NodeListener listener) {
		this.listeners.remove(listener);
	}

	public int clusterSize() {
		return clusterSize;
	}

	public View view() {
		return ch.getView();
	}

	private void acceptView(View view) {

		queue.execute(() -> {

			List<Address> leftMembers = DistributedMachine.this.view != null && view != null
					? Util.leftMembers(this.view.getMembers(), view.getMembers())
					: Collections.emptyList();

			List<Address> newMembers = DistributedMachine.this.view != null && view != null
					? Util.newMembers(this.view.getMembers(), view.getMembers())
					: Collections.emptyList();

			this.view = view;

			LOG.info("View: " + view);
			clusterSize = view.size();

			var mbrs = view.getMembers();
			var oldRank = rank;
			for (var i = 0; i < mbrs.size(); i++) {
				var tmp = mbrs.get(i);
				if (tmp.equals(ch.getAddress())) {
					rank = i;
					break;
				}
			}

			if (oldRank == -1 || oldRank != rank) {
				LOG.info("My rank is {}", rank);
			}

			for (Address mbr : leftMembers) {
				handleLeftMember(mbr);
			}

			for (Address mbr : newMembers) {
				handleNewMember(oldRank, mbr);
			}

			for (var i = listeners.size() - 1; i >= 0; i--) {
				listeners.get(i).nodesChanged(leftMembers, newMembers);
			}
		});
	}

	private void handleLeftMember(Address mbr) {
		components.forEach(c -> c.handleLeftMember(mbr));
		
		/*
		 * Take over the tasks previously assigned to this member *if* the ID matches my
		 * (new rank)
		 */

//		LOG.info("Handling left member {}. I know about {} tasks", mbr, tasks.size());
//		for (Map.Entry<ClusterID, Entry<?>> entry : tasks.entrySet()) {
//			ClusterID id = entry.getKey();
//			if (!shouldRun(id, entry.getValue().submitter, entry.getValue().task, entry.getValue().spec)) {
//				continue;
//			}
//
//			var val = entry.getValue();
//			if (mbr.equals(val.submitter)) {
//				LOG.info("Will not take over tasks submitted by {} because it left the cluster", mbr);
//				continue;
//			}
//			LOG.info("Taking over task {} from {} (submitted by {})", id, mbr, val.submitter);
//			execute(id, val.submitter, val.task, val.spec);
//
//			// XXXXXXXXXXXXXXXXXXXXXXXXXX
//			// TODO
//			// XXXXXXXXXXXXXXXXXXXXXXXXXXX
//			// Pretty sure something similar to handleExecute is needed here
//
//		}
	}

	private void handleNewMember(int wasRank, Address mbr) {
		components.forEach(c -> c.handleNewMember(wasRank, mbr));
		/*
		 * We only we the new member to be sent tasks once, so only send if .. if we
		 * WERE the leader before this new member
		 * 
		 * None of this happens if there is shared task storage
		 */
//		if(!taskStore.isPresent() && wasRank == 0) {
//			LOG.info("I was leader, handling new member {}. I know about {} tasks", mbr, tasks.size());
//			for (Map.Entry<ClusterID, TaskEntry> entry : tasks.entrySet()) {
//				var value = entry.getValue();
//				var task = (DistributedTask<?>) value.task;
//				LOG.info("   Sending {} [{}]", value.id(), entry.getKey());
//				var req = new Request(Request.Type.SUBMIT, value.id(), new SubmitPayload(task, value.spec));
//				try {
//					sendSubmitRequest(mbr, req);
//				} catch (Exception ioe) {
//					LOG.warn("Couldn't update new member with task {}", task.id());
//				}
//			}
//		}

	}

	void sendRequest(Address recipient, Request repreq) {
		try {
			if (LOG.isDebugEnabled()) {
				LOG.debug("Sending request: {} to {}", repreq.type(), recipient == null ? "ALL" : recipient);
			}

			srlzr.set(payloadSerializer);
			var buf = Util.streamableToByteBuffer(repreq);
			ch.send(new BytesMessage(recipient, buf));
		} catch (RuntimeException re) {
			throw re;
		} catch (IOException ioe) {
			throw new UncheckedIOException(ioe);
		} catch (Exception e) {
			throw new IllegalStateException(e);
		} finally {
			srlzr.remove();
		}
	}

	List<String> getMemberList() {
		return ch.getView().getMembers().stream().map(Address::toString).toList();
	}

	JChannel channel() {
		return ch;
	}

	/**
	 * Get the timeout to wait for acknowledgements from other nodes before assuming
	 * they are down and ignoring them.
	 * 
	 * @return acknowledge timeout
	 */
	public Duration acknowledgeTimeout() {
		return acknowledgeTimeout;
	}

	/**
	 * All messages received by this machine are processed on a single thread, so
	 * that message handlers don't have to worry about synchronisation. This is the
	 * queue that messages are processed on, and can be used for any other
	 * operations that need to be single-threaded with message processing.
	 * <p>
	 * In general you do not need to use this unless you are writing your own
	 * {@link DistributedComponent}.
	 * 
	 * @return queue
	 */
	public ExecutorService queue() {
		return queue;
	}

	Ack acks() {
		return acks;
	}

	/**
	 * Get the filter that is applied to all incoming payloads.
	 * 
	 * @return payload filter
	 */
	public PayloadFilter payloadFilter() {
		return payloadFilter;
	}
}
