/**
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
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public interface ObjectStore {
	boolean has(String path, Serializable key);
	
	Serializable get(String path, Serializable key);

	void put(String path, Serializable key, Serializable value);
	
	boolean remove(String path, Serializable key);

	static ObjectStore basicMap() {
		return new ObjectStore() {
			
			private Map<String, Map<Serializable, Serializable>> objects = new ConcurrentHashMap<>();
			
			Map<Serializable, Serializable> objects(String path) {
				synchronized(objects) {
					var m = objects.get(path);
					if(m == null) {
						m = new ConcurrentHashMap<>();
						objects.put(path, m);
					}
					return m;
				}
			}
			
			@Override
			public boolean remove(String path, Serializable key) {
				return objects(path).remove(key) != null;
			}
			
			@Override
			public void put(String path, Serializable key, Serializable value) {
				objects(path).put(key, value);
			}
			
			@Override
			public boolean has(String path, Serializable key) {
				return objects(path).containsKey(key);
			}
			
			@Override
			public Serializable get(String path, Serializable key) {
				return objects(path).get(key);
			}
		};
	}
}
