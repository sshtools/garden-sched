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

import static org.jgroups.util.Util.objectFromStream;
import static org.jgroups.util.Util.objectToStream;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInput;
import java.io.DataInputStream;
import java.io.DataOutput;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.Serializable;
import java.util.Base64;

public interface PayloadSerializer {

	void serialize(Serializable object, DataOutput output) throws IOException;

	Serializable deserialize(Class<? extends Serializable> type, DataInput input) throws IOException;
	
	default String serializeToString(Serializable object) throws IOException {
		var baos = new ByteArrayOutputStream();
		serialize(object, new DataOutputStream(baos));
		return Base64.getEncoder().encodeToString(baos.toByteArray());
	}

	default Serializable deserializeFromString(Class<? extends Serializable> type, String input) throws IOException {
		return deserialize(type, new DataInputStream(new ByteArrayInputStream(Base64.getDecoder().decode(input))));
	}

	public static PayloadSerializer defaultSerializer() {
		return new PayloadSerializer() {

			@Override
			public void serialize(Serializable task, DataOutput output) throws IOException {
				objectToStream(task, output);
			}

			@Override
			public Serializable deserialize(Class<? extends Serializable> type, DataInput input) throws IOException {
				try {
					return objectFromStream(input);
				} catch (ClassNotFoundException e) {
					throw new IllegalArgumentException(e);
				}

			}
		};
	}
}
