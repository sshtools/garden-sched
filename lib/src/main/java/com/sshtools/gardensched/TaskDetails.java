package com.sshtools.gardensched;

import static java.lang.annotation.RetentionPolicy.RUNTIME;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.Target;

@Retention(RUNTIME)
@Target(ElementType.TYPE)
public @interface TaskDetails {
	
	String id() default "";

	Affinity affinity() default Affinity.ANY;
	
	ConflictResolution onConflict() default ConflictResolution.THROW;
	
	String[] classifiers() default {};
	
}
