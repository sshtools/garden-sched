module com.sshtools.gardensched.spring {
	requires transitive spring.context;
	requires com.fasterxml.jackson.annotation;
	requires com.sshtools.gardensched; 
	requires transitive com.fasterxml.jackson.databind;
	requires transitive com.fasterxml.jackson.datatype.jdk8;
	requires spring.core;
	requires spring.beans;
	
}