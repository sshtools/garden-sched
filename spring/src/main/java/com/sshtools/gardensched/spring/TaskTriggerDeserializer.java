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