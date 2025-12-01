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

public enum Affinity {
	/**
	 * Run on the originating node only. If this node goes down, the task will be
	 * lost. No other nodes have any knowledge of this task. The task must be
	 * cancelled from the originating node too.
	 * <p>
	 * This basically makes the task act like a normal local in-JVM task, but allows
	 * the task to be decorated with classifiers and to be accessed by it's ID.
	 */
	LOCAL,
	/**
	 * Run on the originating node only, but if the node goes down allow another
	 * node to take over the task.
	 */
	THIS,
	/**
	 * Run on any available node. The actual node used will be determined by the
	 * distributed schedulers node picking algorithm (current based on the hash
	 * code).
	 */
	ANY,
	/**
	 * Run on all nodes online at the time of triggering.
	 */
	ALL,
	/**
	 * Run on any node except for this one, unless all other nodes go down.
	 */
	MEMBER
}