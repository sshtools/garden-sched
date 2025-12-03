package com.sshtools.gardensched.spring.example;

import java.io.Serializable;

public class TestEvent implements Serializable {
	
	private static final long serialVersionUID = -8755572644875520058L;

	private String text;
	private TestPayload inner1 = new TestInnerPayload((int)(Math.random() * 100));
	private TestPayload inner2 = new TestInnerPayload2(Math.random() > 0.5f);
	
	public TestEvent() {}

	public TestEvent(String  text) {
		this.text= text;
	}

	public String getText() {
		return text;
	}

	public void setText(String text) {
		this.text = text;
	}

	public TestPayload getInner1() {
		return inner1;
	}

	public void setInner1(TestPayload inner1) {
		this.inner1 = inner1;
	}

	public TestPayload getInner2() {
		return inner2;
	}

	public void setInner2(TestPayload inner2) {
		this.inner2 = inner2;
	}
	
	
}
