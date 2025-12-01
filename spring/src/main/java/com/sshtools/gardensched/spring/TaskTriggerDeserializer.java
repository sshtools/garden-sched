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

import java.io.IOException;
import java.time.Duration;

import org.springframework.scheduling.Trigger;
import org.springframework.scheduling.support.CronTrigger;
import org.springframework.scheduling.support.PeriodicTrigger;

import com.fasterxml.jackson.core.JacksonException;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.databind.DeserializationContext;
import com.fasterxml.jackson.databind.JsonDeserializer;
import com.fasterxml.jackson.databind.node.NumericNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.fasterxml.jackson.databind.node.TextNode;

public class TaskTriggerDeserializer extends JsonDeserializer<Trigger> {

	@Override
	public Trigger deserialize(JsonParser p, DeserializationContext ctxt) throws IOException, JacksonException {
		var node = p.getCodec().readTree(p);
		var classname = ((TextNode) node.get("type")).asText();
		try {
			if(classname.equals(CronTrigger.class.getName())) {
				return new CronTrigger(((TextNode) node.get("expression")).asText());
			}
			else if(classname.equals(PeriodicTrigger.class.getName())) {
				return new PeriodicTrigger(
					Duration.ofMillis(((NumericNode) node.get("period")).asLong())
				);
			}
			else {
				/* TODO pretty sure this isn't correct */
				var obj = JsonTaskSerializer.classLocator().locate(classname).getConstructor().newInstance();
				((ObjectNode) node.get("object")).pojoNode(obj);
				return (Trigger) obj;
			}
		} catch (Exception cnfe) {
			throw new IOException("Could not deserialize.", cnfe);
		}
	}
}