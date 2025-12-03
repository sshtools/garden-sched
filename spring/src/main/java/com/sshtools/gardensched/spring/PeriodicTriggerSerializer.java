package com.sshtools.gardensched.spring;

import java.io.IOException;

import org.springframework.scheduling.support.PeriodicTrigger;

import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.core.JsonToken;
import com.fasterxml.jackson.databind.JsonSerializer;
import com.fasterxml.jackson.databind.SerializerProvider;
import com.fasterxml.jackson.databind.jsontype.TypeSerializer;

public class PeriodicTriggerSerializer extends JsonSerializer<PeriodicTrigger> {
	@Override
	public void serialize(PeriodicTrigger value, JsonGenerator gen, SerializerProvider serializers) throws IOException {
		gen.writeNumber(value.getPeriodDuration().toMillis());
	}

	@Override
	public void serializeWithType(PeriodicTrigger value, JsonGenerator gen, SerializerProvider serializers,
			TypeSerializer typeSer) throws IOException {
		var typeId = typeSer.typeId(value, JsonToken.VALUE_STRING);
		typeSer.writeTypePrefix(gen, typeId);
		gen.writeNumber(value.getPeriodDuration().toMillis());
		typeSer.writeTypeSuffix(gen, typeId);
	}
}