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
