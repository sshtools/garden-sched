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