package com.sshtools.gardensched.spring;

public interface ClassLocator {
	 Class<?> locate(String name) throws ClassNotFoundException;
}
