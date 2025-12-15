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

import java.io.IOException;
import java.io.UncheckedIOException;

public interface SerializableJob extends SerializableRunnable {

	default void run() {
		try {
			execute();
		}
		catch(IOException ioe) {
			throw new UncheckedIOException(ioe);
		}
		catch(RuntimeException re) {
			throw re;
		}
		catch(Exception e) {
			throw new IllegalStateException("Job failed.", e);
		}
	}
	
	void execute() throws Exception;
}
