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

import java.time.Duration;

import org.springframework.context.ApplicationContext;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.scheduling.support.CronTrigger;
import org.springframework.scheduling.support.PeriodicTrigger;

import com.fasterxml.jackson.annotation.JsonAutoDetect;
import com.fasterxml.jackson.annotation.JsonTypeInfo;
import com.fasterxml.jackson.annotation.PropertyAccessor;
import com.fasterxml.jackson.databind.MapperFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.json.JsonMapper;
import com.fasterxml.jackson.databind.jsontype.impl.LaissezFaireSubTypeValidator;
import com.fasterxml.jackson.databind.module.SimpleModule;
import com.fasterxml.jackson.datatype.jdk8.Jdk8Module;
import com.sshtools.gardensched.DistributedScheduledExecutor;
import com.sshtools.gardensched.PayloadFilter;
import com.sshtools.gardensched.PayloadSerializer;

@Configuration
public class SpringGardenSchedConfig {
	
	@Bean
	public ObjectMapper objectMapper(PayloadFilter payloadFilter) {

		var sm = new SimpleModule();
		sm.addSerializer(CronTrigger.class, new CronTriggerSerializer());
		sm.addSerializer(PeriodicTrigger.class, new PeriodicTriggerSerializer());
		
//		var pf = new PayloadFilteringModule(payloadFilter);

		return JsonMapper.builder()
			    .addModule(new Jdk8Module())
			    .addModule(sm)
//			    .addModule(pf)
			    .configure(MapperFeature.REQUIRE_SETTERS_FOR_GETTERS, true)
			    .activateDefaultTyping(new LaissezFaireSubTypeValidator(), 
			    		ObjectMapper.DefaultTyping.NON_FINAL_AND_ENUMS,
			    		JsonTypeInfo.As.WRAPPER_ARRAY)
				.visibility(PropertyAccessor.FIELD, JsonAutoDetect.Visibility.PUBLIC_ONLY)
				.visibility(PropertyAccessor.GETTER, JsonAutoDetect.Visibility.PUBLIC_ONLY)
				.visibility(PropertyAccessor.SETTER, JsonAutoDetect.Visibility.PUBLIC_ONLY)
				.visibility(PropertyAccessor.IS_GETTER, JsonAutoDetect.Visibility.PUBLIC_ONLY)

			    .build();
	}
	

	@Bean
	public PayloadSerializer payloadSerializer(ObjectMapper mapper) {
		return new JsonPayloadSerializer(mapper);
	}

	@Bean
	public PayloadFilter payloadFilter(ApplicationContext context) {
		return new SpringPayloadFilter(context);
	}

	@Bean
	public DistributedScheduledExecutor distributedScheduledExecutor(PayloadSerializer payloadSerializer, PayloadFilter payloadFilter) throws Exception {
		return new DistributedScheduledExecutor.Builder().
				withPayloadSerializer(payloadSerializer).
				withPayloadFilter(payloadFilter).
				withAcknowledgeTimeout(Duration.ofMinutes(30)). // Helps debugging
				build();
	}
}
