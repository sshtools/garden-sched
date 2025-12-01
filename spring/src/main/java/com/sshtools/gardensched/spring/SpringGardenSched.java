package com.sshtools.gardensched.spring;

import org.springframework.context.ApplicationContext;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import com.fasterxml.jackson.annotation.JsonAutoDetect;
import com.fasterxml.jackson.annotation.PropertyAccessor;
import com.fasterxml.jackson.databind.MapperFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.json.JsonMapper;
import com.fasterxml.jackson.datatype.jdk8.Jdk8Module;
import com.sshtools.gardensched.DistributedScheduledExecutor;
import com.sshtools.gardensched.TaskFilter;
import com.sshtools.gardensched.TaskSerializer;

@Configuration
public class SpringGardenSched {
	
	@Bean
	public ObjectMapper objectMapper() {
		var om = JsonMapper.builder()
			    .addModule(new Jdk8Module())
			    .configure(MapperFeature.REQUIRE_SETTERS_FOR_GETTERS, true)
			    .build();
		
		
		// Only consider public fields
		om.setVisibility(PropertyAccessor.FIELD, JsonAutoDetect.Visibility.PUBLIC_ONLY);

		// (optional) likewise restrict getters / setters to public
		om.setVisibility(PropertyAccessor.GETTER, JsonAutoDetect.Visibility.PUBLIC_ONLY);
		om.setVisibility(PropertyAccessor.SETTER, JsonAutoDetect.Visibility.PUBLIC_ONLY);
		om.setVisibility(PropertyAccessor.IS_GETTER, JsonAutoDetect.Visibility.PUBLIC_ONLY);
		
		return om;
	}
	

	@Bean
	public TaskSerializer taskSerializer(ObjectMapper mapper) {
		return new JsonTaskSerializer(mapper);
	}

	@Bean
	public TaskFilter taskFilter(ApplicationContext context) {
		return new SpringTaskFilter(context);
	}

	@Bean
	public DistributedScheduledExecutor distributedScheduledExecutor(TaskSerializer taskSerializer, TaskFilter taskFilter) throws Exception {
		return new DistributedScheduledExecutor.Builder().
				withTaskSerializer(taskSerializer).
				withTaskFilter(taskFilter).
//				withAcknowledgeTimeout(Duration.ofMinutes(30)). // Helps debugging
				build();
	}
}
