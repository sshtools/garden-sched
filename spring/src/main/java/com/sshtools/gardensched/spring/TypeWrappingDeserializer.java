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
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonToken;
import com.fasterxml.jackson.core.ObjectCodec;
import com.fasterxml.jackson.databind.DeserializationContext;
import com.fasterxml.jackson.databind.JsonDeserializer;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.sshtools.gardensched.DistributedScheduledExecutor;

import java.io.IOException;
import java.util.*;

public class TypeWrappingDeserializer extends JsonDeserializer<Object> {

    // Inner mapper without the custom module to avoid recursion
    private final ObjectMapper innerMapper = new ObjectMapper();

    @Override
    public Object deserialize(JsonParser p, DeserializationContext ctxt)
            throws IOException {

        JsonToken t = p.currentToken();
        if (t == null) {
            t = p.nextToken();
        }

        if (t == JsonToken.VALUE_NULL) {
            return null;
        }

        ObjectCodec codec = p.getCodec();
        JsonNode node = codec.readTree(p);

        return fromNode(node);
    }

    private Object fromNode(JsonNode node) throws IOException {
        if (node == null || node.isNull()) {
            return null;
        }

        if (node.isTextual()) {
            return node.textValue();
        }
        if (node.isNumber()) {
            return node.numberValue();
        }
        if (node.isBoolean()) {
            return node.booleanValue();
        }

        if (node.isArray()) {
            List<Object> list = new ArrayList<>();
            for (JsonNode element : node) {
                list.add(fromNode(element));
            }
            return list;
        }

        if (node.isObject()) {
            JsonNode typeNode = node.get("type");
            JsonNode objectNode = node.get("object");

            // Our special wrapper: { "type": "...", "object": { ... } }
            if (typeNode != null && typeNode.isTextual()
                    && objectNode != null && !objectNode.isNull()) {
                String typeName = typeNode.asText();
                try {
                    Class<?> clazz = JsonTaskSerializer.classLocator().locate(typeName);
                    // Use plain Jackson bean deserialization into that class
                    return DistributedScheduledExecutor.currentFilter().filter( innerMapper.treeToValue(objectNode, clazz));
                } catch (ClassNotFoundException e) {
                    throw new IOException("Unknown type: " + typeName, e);
                }
            }

            // Plain JSON object -> Map<String, Object>
            Map<String, Object> map = new LinkedHashMap<>();
            Iterator<Map.Entry<String, JsonNode>> it = node.fields();
            while (it.hasNext()) {
                Map.Entry<String, JsonNode> entry = it.next();
                map.put(entry.getKey(), fromNode(entry.getValue()));
            }
            return map;
        }

        // Fallback
        return null;
    }

    @Override
    public Class<?> handledType() {
        return Object.class;
    }
}
