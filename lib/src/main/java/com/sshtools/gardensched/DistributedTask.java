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
import java.util.LinkedHashSet;
import java.util.Optional;
import java.util.Set;

import org.jgroups.util.Streamable;
import org.jgroups.util.UUID;

public interface DistributedTask<RESULT, TASK extends Serializable> extends Streamable {
	
	public abstract class AbstractTask<RESULT, TASK extends Serializable> implements DistributedTask<RESULT, TASK> {
		
		private Affinity affinity;
		private String id;
		private ConflictResolution onConflict;
		private Set<String> classifiers;
		
		public AbstractTask() {
		}
		
		public AbstractTask(AbstractBuilder<?, RESULT, ?> bldr) {
			this.affinity = bldr.affinity;
			this.affinity = bldr.affinity;
			this.onConflict = bldr.onConflict;
			this.id = bldr.id;
			this.classifiers = Collections.unmodifiableSet(new LinkedHashSet<>( bldr.classifiers));
		}

		public AbstractTask(Affinity affinity, String id, ConflictResolution onConflict, String... classifiers) {
			this.affinity = affinity;
			this.onConflict = onConflict;
			this.id = id;
			this.classifiers = Set.of(classifiers);
		}

		@Override
		public Set<String> classifiers() {
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
		public final Affinity affinity() {
			return affinity;
		}

		@Override
		public void writeTo(DataOutput out) throws IOException {
			out.writeUTF(affinity.name());
			out.writeUTF(id);
			out.writeUTF(onConflict.name());
		}

		@Override
		public void readFrom(DataInput in) throws IOException, ClassNotFoundException {
			affinity = Affinity.valueOf(in.readUTF());
			id =  in.readUTF();
			onConflict = ConflictResolution.valueOf(in.readUTF());
		}

	}
	
	public static abstract class AbstractBuilder<BLDR extends AbstractBuilder<BLDR, RESULT, TSK>, RESULT, TSK> {

		protected final TSK task;
		
		private final String id;
		private Affinity affinity = Affinity.ANY;
		private ConflictResolution onConflict = ConflictResolution.THROW;
		private Set<String> classifiers =  new LinkedHashSet<>();

		public AbstractBuilder(TSK task) {
			this(UUID.randomUUID().toString(), task);
		}
		
		public AbstractBuilder(String id, TSK task) {
			this.id = id;
			this.task = task;
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
	}
	
	Set<String> classifiers();

	TASK task();

	Affinity affinity();

	Optional<String> id();

	default ConflictResolution onConflict() {
		return ConflictResolution.THROW;
	}

}