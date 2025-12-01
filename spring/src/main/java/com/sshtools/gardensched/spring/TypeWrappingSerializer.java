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
import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.databind.JsonSerializer;
import com.fasterxml.jackson.databind.SerializerProvider;

import java.io.IOException;

public class TypeWrappingSerializer extends JsonSerializer<Object> {

    @Override
    public void serialize(Object value, JsonGenerator gen, SerializerProvider serializers)
            throws IOException {

        if (value == null) {
            gen.writeNull();
            return;
        }

        Class<?> cls = value.getClass();

        if (isPrimitiveLike(cls)) {
            // Let Jackson handle primitive-like types normally
            serializers.defaultSerializeValue(value, gen);
        } else {
            // Wrap as { "type": "fqcn", "object": { ...normal bean... } }
            gen.writeStartObject();
            gen.writeStringField("type", cls.getName());
            gen.writeFieldName("object");
            serializers.defaultSerializeValue(value, gen);
            gen.writeEndObject();
        }
    }

    private boolean isPrimitiveLike(Class<?> cls) {
        return cls.isPrimitive()
                || Number.class.isAssignableFrom(cls)
                || Boolean.class.equals(cls)
                || Character.class.equals(cls)
                || CharSequence.class.isAssignableFrom(cls);
    }

    @Override
    public Class<Object> handledType() {
        return Object.class;
    }
}
