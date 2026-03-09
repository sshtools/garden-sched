/**
 * Copyright © 2025 JAdaptive Limited (support@jadaptive.com)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.sshtools.gardensched;

import java.io.PrintWriter;
import java.io.Serializable;
import java.io.StringWriter;

public class TaskError implements Serializable {
	
	public final static class TaskException extends Exception {

		private static final long serialVersionUID = -1385657145615407163L;
		
		private String remoteExceptionType;
		private String remoteExceptionTrace;
		
		private TaskException(TaskError error) {
			super(error.remoteExceptionMessage);
			this.remoteExceptionType = error.remoteExceptionType;
			this.remoteExceptionTrace = error.remoteExceptionTrace;
		}
		
		public String getRemoteExceptionType() {
			return remoteExceptionType;
		}
		
		public String getRemoteExceptionTrace() {
			return remoteExceptionTrace;
		}
	}

	private static final long serialVersionUID = -5654145039847907039L;
	private String remoteExceptionType;
	private String remoteExceptionMessage;
	private String remoteExceptionTrace;

	public TaskError() {
		this("No message supplied.", null);
	}

	public TaskError(String message, Throwable cause) {
		remoteExceptionType = cause == null ? TaskError.class.getName() : cause.getClass().getName();
		remoteExceptionTrace = toString(cause);
		remoteExceptionMessage = message;
	}

	public TaskError(String message) {
		remoteExceptionType = TaskError.class.getName();
		remoteExceptionTrace = toString(new  Exception());
		remoteExceptionMessage = message;
	}

	public TaskError(Throwable cause) {
		remoteExceptionType = cause == null ? TaskError.class.getName() :cause.getClass().getName();
		remoteExceptionTrace = toString(cause);
		remoteExceptionMessage = cause == null ? null :cause.getMessage();
	}

	public String getRemoteExceptionType() {
		return remoteExceptionType;
	}

	public void setRemoteExceptionType(String remoteExceptionType) {
		this.remoteExceptionType = remoteExceptionType;
	}

	public String getRemoteExceptionMessage() {
		return remoteExceptionMessage;
	}

	public void setRemoteExceptionMessage(String remoteExceptionMessage) {
		this.remoteExceptionMessage = remoteExceptionMessage;
	}

	public String getRemoteExceptionTrace() {
		return remoteExceptionTrace;
	}

	public void setRemoteExceptionTrace(String remoteExceptionTrace) {
		this.remoteExceptionTrace = remoteExceptionTrace;
	}

	private String toString(Throwable cause) {
		
		if(cause == null)
			return null;
		
		var sw = new StringWriter();
		cause.printStackTrace(new PrintWriter(sw));
		return sw.toString();
	}

	public Exception toException() {
		return new TaskException(this);
	}

}
