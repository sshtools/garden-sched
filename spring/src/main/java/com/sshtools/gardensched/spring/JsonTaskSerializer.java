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
package com.sshtools.gardensched.spring;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.io.Serializable;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.sshtools.gardensched.TaskSerializer;

public class JsonTaskSerializer implements TaskSerializer {

	private final ObjectMapper mapper;
	private final static ThreadLocal<ClassLocator> currentClassLocator = new ThreadLocal<>();
	private final ClassLocator classLocator;

	public JsonTaskSerializer(ObjectMapper mapper) {
		this(mapper, n -> Class.forName(n));
	}
	
	public JsonTaskSerializer(ObjectMapper mapper, ClassLocator classLocator) {
		this.mapper = mapper;
		this.classLocator = classLocator;
	}

	@Override
	public void serialize(Serializable task, DataOutput output) throws IOException {
		output.writeUTF(task == null ? "" : task.getClass().getName());
		if(task != null) {
			var tskJson = mapper.writeValueAsString(task);
			output.writeUTF(tskJson);
		}
	}
	
	public static ClassLocator classLocator() {
		return currentClassLocator.get();
	}

	@SuppressWarnings("unchecked")
	@Override
	public Serializable deserialize(Class<? extends Serializable> type, DataInput input) throws IOException {
		try {
			currentClassLocator.set(classLocator);
			var tskName = input.readUTF();
			if(tskName.equals(""))
				return null;
			var clz = (Class<Serializable>) classLocator().locate(tskName);
			var tskJson = input.readUTF();
			return mapper.readValue(tskJson, clz);
		} catch (ClassNotFoundException cnfe) {
			throw new IllegalArgumentException(cnfe);
		} finally {
			currentClassLocator.remove();
		}
	}

}
