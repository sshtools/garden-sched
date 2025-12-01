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
