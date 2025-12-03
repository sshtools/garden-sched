package com.sshtools.gardensched;

import java.io.Serializable;

import org.jgroups.Address;

public interface BroadcastEventListener {

	void accept(Address sender, Serializable event);
}
