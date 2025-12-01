package com.sshtools.gardensched.spring.example;

import java.security.SecureRandom;

import org.springframework.stereotype.Service;

@Service
public class RandomService {

	private SecureRandom secRnd = new SecureRandom();
	
	public int nextNumber() {
		return secRnd.nextInt();
	}
}
