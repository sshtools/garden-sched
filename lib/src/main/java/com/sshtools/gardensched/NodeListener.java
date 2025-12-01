package com.sshtools.gardensched;

import java.util.List;

import org.jgroups.Address;

public interface NodeListener {

	void nodesChanged(List<Address> leftMembers, List<Address> newMembers);
}
