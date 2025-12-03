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
import com.sshtools.gardensched.PayloadSerializer;

public class JsonPayloadSerializer implements PayloadSerializer {

	private final ObjectMapper mapper;
	private final static ThreadLocal<ClassLocator> currentClassLocator = new ThreadLocal<>();
	private final ClassLocator classLocator;

	public JsonPayloadSerializer(ObjectMapper mapper) {
		this(mapper, n -> Class.forName(n));
	}
	
	public JsonPayloadSerializer(ObjectMapper mapper, ClassLocator classLocator) {
		this.mapper = mapper;
		this.classLocator = classLocator;
	}

	@Override
	public void serialize(Serializable task, DataOutput output) throws IOException {
		String writeValueAsString = mapper.writeValueAsString(task);
		System.out.println("XXXX " + writeValueAsString);
		output.writeUTF(writeValueAsString);
	}
	
	public static ClassLocator classLocator() {
		return currentClassLocator.get();
	}

	@Override
	public Serializable deserialize(Class<? extends Serializable> type, DataInput input) throws IOException {
		try {
			currentClassLocator.set(classLocator);
			var tskJson = input.readUTF();
			return mapper.readValue(tskJson, Serializable.class);
		}
		finally {
			currentClassLocator.remove();
		}
	}

}
