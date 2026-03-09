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

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.io.Serializable;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashSet;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

import org.jgroups.util.Streamable;
import org.jgroups.util.UUID;

public interface DistributedTask<TASK extends Serializable> extends Streamable {
	
	public final static Affinity DEFAULT_AFFINTY = Affinity.ANY;
	public final static ConflictResolution DEFAULT_CONFLICT_RESOLUTION = ConflictResolution.THROW;
	
	public abstract class AbstractTask<TASK extends Serializable> implements DistributedTask<TASK> {
		
		private Affinity affinity;
		private String id;
		private ConflictResolution onConflict;
		private Set<String> classifiers = Collections.emptySet();
		private Boolean persistent;
		private String bundle;
		private String name;
		private Map<String, Serializable> attributes = Collections.emptyMap();
		protected String key;

		
		public AbstractTask() {
		}
		
		public AbstractTask(AbstractBuilder<?, ?, ?> bldr) {
			this.affinity = bldr.affinity;
			this.affinity = bldr.affinity;
			this.onConflict = bldr.onConflict;
			this.id = bldr.id;
			this.classifiers = Collections.unmodifiableSet(new LinkedHashSet<>( bldr.classifiers));
			this.persistent = bldr.persistent;
			this.key = bldr.key;
			this.bundle = bldr.bundle;
			this.name = bldr.name;
			this.attributes = Collections.unmodifiableMap(new HashMap<>(bldr.attributes));
		}

		@Override
		public Map<String, Serializable> attributes() {
			return attributes;
		}

		@Override
		public Optional<String> name() {
			return Optional.ofNullable(name);
		}

		@Override
		public Optional<String> bundle() {
			return Optional.ofNullable(bundle);
		}

		@Override
		public final Set<String> classifiers() {
			return classifiers;
		}

		@Override
		public final ConflictResolution onConflict() {
			return onConflict;
		}

		@Override
		public final Optional<String> id() {
			return Optional.ofNullable(id);
		}

		@Override
		public final Optional<Boolean> persistent() {
			return Optional.ofNullable(persistent);
		}

		@Override
		public final Affinity affinity() {
			return affinity;
		}

		@Override
		public void writeTo(DataOutput out) throws IOException {
			out.writeUTF(affinity.name());
			out.writeUTF(id);
			out.writeUTF(onConflict.name());
			out.writeBoolean(persistent != null);
			out.writeBoolean(persistent != null && persistent);
			out.writeUTF(key);
			out.writeUTF(bundle);
			out.writeUTF(name);
			out.writeInt(classifiers.size());
			for(var c : classifiers)
				out.writeUTF(c);
			out.writeInt(attributes.size());
			for(var entry : attributes.entrySet()) {
				out.writeUTF(entry.getKey());
				DistributedScheduledExecutor.currentSerializer().serialize(entry.getValue(), out);
			}
		}

		@Override
		public void readFrom(DataInput in) throws IOException, ClassNotFoundException {
			affinity = Affinity.valueOf(in.readUTF());
			id =  in.readUTF();
			onConflict = ConflictResolution.valueOf(in.readUTF());
			if(in.readBoolean()) {
				persistent = in.readBoolean();
			}
			else {
				in.readBoolean();
				persistent = null;
			}
			key = in.readUTF();
			bundle = in.readUTF();
			name = in.readUTF();
			var c = in.readInt();
			var s = new String[c];
			for(var i = 0 ; i < c ; i++) {
				s[i] = in.readUTF();
			}
			classifiers = Set.of(s);
			c = in.readInt();
			var attributes = new  HashMap<String,  Serializable>();
			for(var i = 0 ; i < c ; i++) {
				attributes.put(
					in.readUTF(), 
					DistributedScheduledExecutor.currentSerializer().deserialize(Serializable.class, in)
				);
			}
			this.attributes = Collections.unmodifiableMap(attributes);
		}

	}
	
	public static abstract class AbstractBuilder<BLDR extends AbstractBuilder<BLDR, TSK, DTSK>, TSK extends Serializable, DTSK extends DistributedTask<TSK>> {

		protected final TSK task;
		
		private String id;
		private Affinity affinity = DistributedTask.DEFAULT_AFFINTY;
		private ConflictResolution onConflict = DistributedTask.DEFAULT_CONFLICT_RESOLUTION;
		private Set<String> classifiers =  new LinkedHashSet<>();
		private Boolean persistent;
		private String key;
		private String bundle;
		private String name;
		private Map<String, Serializable> attributes = new HashMap<>();

		public AbstractBuilder(TSK task) {
			this(UUID.randomUUID().toString(), task);
		}
		
		public AbstractBuilder(String id, TSK task) {
			this.id = id;
			this.task = task;
			fromAnnotatedObject(task);
		}

		@SuppressWarnings("unchecked")
		public final BLDR fromAnnotatedObject(Object object) {
			var tc = object.getClass().getAnnotation(TaskConfig.class);
			if(tc != null) {
			
				if(!tc.id().equals(""))
					id = tc.id();
				
				if(!tc.key().equals(""))
					withKey(tc.key());
				
				if(!tc.bundle().equals(""))
					withBundle(tc.bundle());
				
				if(!tc.name().equals(""))
					withName(tc.name());
				
				withAffinity(tc.affinity());
				onConflict(tc.onConflict());
				
				addClassifiers(tc.classifiers());
				
				if(tc.doPersist() && !tc.dontPersist())
					withPersistent(true);
				else if(tc.dontPersist())
					withPersistent(false);
			}

			return (BLDR)this;
		}

		@SuppressWarnings("unchecked")
		public final BLDR withKey(String key) {
			this.key = key;
			return (BLDR)this;
		}

		@SuppressWarnings("unchecked")
		public final BLDR withBundle(String bundle) {
			this.bundle = bundle;
			return (BLDR)this;
		}

		@SuppressWarnings("unchecked")
		public final BLDR withName(String name) {
			this.name = name;
			return (BLDR)this;
		}

		public final BLDR withPersistent() {
			return withPersistent(true);
		}

		@SuppressWarnings("unchecked")
		public final BLDR withPersistent(boolean persistent) {
			this.persistent = persistent;
			return (BLDR)this;
		}

		@SuppressWarnings("unchecked")
		public final BLDR addAttribute(String key, Serializable value) {
			this.attributes.put(key, value);
			return (BLDR)this;
		}

		@SuppressWarnings("unchecked")
		public final BLDR addAttributes(Map<String, Serializable> attributes) {
			this.attributes.putAll(attributes);
			return (BLDR)this;
		}

		public final BLDR withAttributes(Map<String, Serializable> attributes) {
			this.attributes.clear();
			return addAttributes(attributes);
		}

		public final BLDR withClassifiers(String... classifiers) {
			return withClassifiers(Arrays.asList(classifiers));
		}

		public final BLDR addClassifiers(String... classifiers) {
			return addClassifiers(Arrays.asList(classifiers));
		}

		public final BLDR withClassifiers(Collection<String> classifiers) {
			this.classifiers.clear();
			return addClassifiers(classifiers);
		}

		@SuppressWarnings("unchecked")
		public final BLDR addClassifiers(Collection<String> classifiers) {
			this.classifiers.addAll(classifiers);
			return (BLDR)this;
		}

		@SuppressWarnings("unchecked")
		public final BLDR withAffinity(Affinity affinity) {
			this.affinity = affinity;
			return (BLDR)this;
		}
		
		@SuppressWarnings("unchecked")
		public final BLDR onConflict(ConflictResolution onConflict) {
			this.onConflict = onConflict;
			return (BLDR)this;
		}
		
		public abstract DTSK build();
	}
	
	Optional<Boolean> persistent();
	
	default <S extends Serializable> S attr(String attr) {
		return attr(attr, null);
	}
	
	@SuppressWarnings("unchecked")
	default <S extends Serializable> S attr(String attr, S defaultValue) {
		return (S)attributes().put(attr, defaultValue);
	}
	
	Map<String, Serializable> attributes();

	Set<String> classifiers();
	
	default String displayName() {
		return name().orElseGet(() -> key());
	}
	
	Optional<String> name();
	
	String key();
	
	Optional<String> bundle();

	TASK task();

	Affinity affinity();

	Optional<String> id();

	default ConflictResolution onConflict() {
		return ConflictResolution.THROW;
	}

}