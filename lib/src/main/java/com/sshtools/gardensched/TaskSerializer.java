package com.sshtools.gardensched;

import static org.jgroups.util.Util.objectFromStream;
import static org.jgroups.util.Util.objectToStream;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.io.Serializable;

public interface TaskSerializer {

	void serialize(Serializable task, DataOutput output) throws IOException;

	Serializable deserialize(Class<? extends Serializable> type, DataInput input) throws IOException;

	public static TaskSerializer defaultSerializer() {
		return new TaskSerializer() {

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
