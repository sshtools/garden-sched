package com.sshtools.gardensched.spring.example;

public final class TestInnerPayload implements TestPayload {
	private static final long serialVersionUID = -1411822653172173022L;
	private int number;

	public TestInnerPayload() { }
	
	public TestInnerPayload(int number) {
		this.number = number;
	}

	public int getNumber() {
		return number;
	}

	public void setNumber(int number) {
		this.number = number;
	}
	
	
}