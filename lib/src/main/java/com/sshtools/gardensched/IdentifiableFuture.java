package com.sshtools.gardensched;

import java.util.Set;
import java.util.concurrent.Future;

public interface IdentifiableFuture<V> extends Future<V> {

	ClusterID clusterID();
	
	Set<String> classifiers();
}
