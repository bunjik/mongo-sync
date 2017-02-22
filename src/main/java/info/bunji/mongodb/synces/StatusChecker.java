/*
 * Copyright 2016 Fumiharu Kinoshita
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
package info.bunji.mongodb.synces;

import java.util.EventListener;
import java.util.Map;

/**
 * @author Fumiharu Kinoshita
 */
public interface StatusChecker {

	/**
	 **********************************
	 * get sync configs.
	 * @return sync config
	 **********************************
	 */
	Map<String, SyncConfig> getConfigs();

	/**
	 **********************************
	 * check sync process running.
	 * @param syncName sync name
	 * @return true if sync process running, otherwise false
	 **********************************
	 */
	boolean isRunning(final String syncName);

	/**
	 **********************************
	 * start indexer.
	 * @param syncName sync name
	 * @return true if start process successed, otherwise false
	 **********************************
	 */
	boolean startIndexer(String syncName);

	/**
	 **********************************
	 * stop indexer.
	 * @param syncName sync name
	 * @return true if stop process successed, otherwise false
	 **********************************
	 */
	boolean stopIndexer(String syncName);

	/**
	 **********************************
	 * resync indexer.
	 * @param syncName sync name
	 * @return true if resync process successed, otherwise false
	 **********************************
	 */
	boolean resyncIndexer(String syncName);

	/**
	 **********************************
	 * remove sync congig.
	 * @param syncName sync name
	 * @return true if remove config successed, otherwise false
	 **********************************
	 */
	boolean removeConfig(String syncName);

	/**
	 **********************************
	 *
	 **********************************
	 */
	static interface Listener extends EventListener {
		void onStop();
	}
}
