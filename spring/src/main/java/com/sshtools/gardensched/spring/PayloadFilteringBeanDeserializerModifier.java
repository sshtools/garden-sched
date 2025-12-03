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

import com.fasterxml.jackson.databind.BeanDescription;
import com.fasterxml.jackson.databind.DeserializationConfig;
import com.fasterxml.jackson.databind.JsonDeserializer;
import com.fasterxml.jackson.databind.deser.BeanDeserializerBase;
import com.fasterxml.jackson.databind.deser.BeanDeserializerModifier;
import com.sshtools.gardensched.PayloadFilter;

public class PayloadFilteringBeanDeserializerModifier extends BeanDeserializerModifier {

    private static final long serialVersionUID = -6309713905209281198L;
    
	private final PayloadFilter payloadFilter;

    public PayloadFilteringBeanDeserializerModifier(PayloadFilter payloadFilter) {
        this.payloadFilter = payloadFilter;
    }

    @Override
    public JsonDeserializer<?> modifyDeserializer(DeserializationConfig config,
                                                  BeanDescription beanDesc,
                                                  JsonDeserializer<?> deserializer) {
        // Only touch bean deserializers
        if (deserializer instanceof BeanDeserializerBase
                && shouldAutowire(beanDesc.getBeanClass())) {

            return new PayloadFilteringDeserializer(
                    (BeanDeserializerBase) deserializer,
                    payloadFilter
            );
        }
        return deserializer;
    }

    private boolean shouldAutowire(Class<?> beanClass) {
        // Tune this to your needs; examples:
        // return beanClass.getName().startsWith("com.sshtools.gardensched.");
        // or marker interface / annotation:
        // return InjectableFromJson.class.isAssignableFrom(beanClass);
        return true;
    }
}
