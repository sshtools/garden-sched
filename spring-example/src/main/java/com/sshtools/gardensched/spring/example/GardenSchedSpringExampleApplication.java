package com.sshtools.gardensched.spring.example;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.annotation.ComponentScan;

import com.sshtools.gardensched.spring.SpringGardenSched;

@SpringBootApplication
@ComponentScan(basePackageClasses = SpringGardenSched.class)
public class GardenSchedSpringExampleApplication {

	public static void main(String[] args) {
		SpringApplication.run(GardenSchedSpringExampleApplication.class, args);
	}

}
