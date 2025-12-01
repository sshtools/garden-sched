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

import org.springframework.scheduling.Trigger;
import org.springframework.scheduling.support.CronTrigger;
import org.springframework.scheduling.support.PeriodicTrigger;

import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonSerializer;
import com.fasterxml.jackson.databind.SerializerProvider;

public class TaskTriggerSerializer extends JsonSerializer<Trigger> {
  
    @Override
    public void serialize(Trigger trgr, 
                          JsonGenerator jsonGenerator, 
                          SerializerProvider serializerProvider) 
                          throws IOException, JsonProcessingException {
        jsonGenerator.writeStartObject();
        jsonGenerator.writeFieldName("type");
        jsonGenerator.writeString(trgr.getClass().getName());
    	if(trgr instanceof CronTrigger ctrigger) {
            jsonGenerator.writeFieldName("expression");
            jsonGenerator.writeString(ctrigger.getExpression());
    	}
    	else if(trgr instanceof PeriodicTrigger ptrigger) {
            jsonGenerator.writeFieldName("period");
            jsonGenerator.writeNumber(ptrigger.getPeriodDuration().toMillis());
            jsonGenerator.writeFieldName("timeUnit");
            jsonGenerator.writeNumber(ptrigger.getTimeUnit().name());
    	}
    	else {
            jsonGenerator.writeFieldName("object");
            jsonGenerator.writeObject(trgr);
    	}
    	
        
    }
}