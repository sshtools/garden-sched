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
import com.fasterxml.jackson.databind.DeserializationContext;
import com.fasterxml.jackson.databind.deser.BeanDeserializer;
import com.fasterxml.jackson.databind.deser.BeanDeserializerBase;
import com.fasterxml.jackson.databind.jsontype.TypeDeserializer;
import com.sshtools.gardensched.PayloadFilter;

import java.io.IOException;

public class PayloadFilteringDeserializer extends BeanDeserializer {

	private static final long serialVersionUID = -7316631264897012830L;
	private final PayloadFilter beanFactory;

	public PayloadFilteringDeserializer(BeanDeserializerBase src, PayloadFilter beanFactory) {
		super(src);
		this.beanFactory = beanFactory;
	}

	@Override
	public Object deserialize(JsonParser p, DeserializationContext ctxt) throws IOException {
		Object bean = super.deserialize(p, ctxt);
		if (bean != null) {
			System.out.println("FILTERING " + bean.hashCode());
			
			bean = beanFactory.filter(bean);
			System.out.println("FILTERED " + bean.hashCode());
		}
		return bean;
	}

	@Override
	public Object deserialize(JsonParser p, DeserializationContext ctxt, Object intoValue) throws IOException {
		Object bean = super.deserialize(p, ctxt, intoValue);
		if (bean != null) {
			bean = beanFactory.filter(bean);
		}
		return bean;
	}

	@Override
	public Object deserializeWithType(JsonParser p, DeserializationContext ctxt, TypeDeserializer typeDeserializer)
			throws IOException {
		Object bean = super.deserializeWithType(p, ctxt, typeDeserializer);
		if (bean != null) {
			bean = beanFactory.filter(bean);
		}
		return bean;
	}
}
