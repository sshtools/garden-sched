package com.sshtools.gardensched.spring;

import java.io.IOException;

import org.springframework.scheduling.support.CronTrigger;

import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.core.JsonToken;
import com.fasterxml.jackson.databind.JsonSerializer;
import com.fasterxml.jackson.databind.SerializerProvider;
import com.fasterxml.jackson.databind.jsontype.TypeSerializer;

public class CronTriggerSerializer extends JsonSerializer<CronTrigger> {
	@Override
	public void serialize(CronTrigger value, JsonGenerator gen, SerializerProvider serializers) throws IOException {
		gen.writeString(value.getExpression());
	}

	@Override
	public void serializeWithType(CronTrigger value, JsonGenerator gen, SerializerProvider serializers,
			TypeSerializer typeSer) throws IOException {
		var typeId = typeSer.typeId(value, JsonToken.VALUE_STRING);
		typeSer.writeTypePrefix(gen, typeId);
		gen.writeString(value.getExpression());
		typeSer.writeTypeSuffix(gen, typeId);
	}
}