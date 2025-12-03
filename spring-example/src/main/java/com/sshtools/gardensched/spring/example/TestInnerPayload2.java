package com.sshtools.gardensched.spring.example;

public class TestInnerPayload2 implements TestPayload {
	private static final long serialVersionUID = -3851694651784600807L;
	private boolean flag;
	
	public TestInnerPayload2() {}
	
	public TestInnerPayload2(boolean flag) {
		super();
		this.flag = flag;
	}

	public boolean isFlag() {
		return flag;
	}

	public void setFlag(boolean flag) {
		this.flag = flag;
	}
	
	
}
