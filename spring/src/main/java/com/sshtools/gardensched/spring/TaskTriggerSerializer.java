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