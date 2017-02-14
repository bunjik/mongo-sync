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
	 * @return
	 **********************************
	 */
	Map<String, SyncConfig> getConfigs();

	/**
	 **********************************
	 * 
	 * @param syncName 
	 * @return 
	 **********************************
	 */
	boolean isRunning(final String syncName);

	/**
	 **********************************
	 * start indexer.
	 * @param syncName
	 * @return
	 **********************************
	 */
	boolean startIndexer(String syncName);

	/**
	 **********************************
	 * stop indexer.
	 * @param syncName 
	 * @return
	 **********************************
	 */
	boolean stopIndexer(String syncName);

	/**
	 **********************************
	 * resync indexer.
	 * <br>
	 * default implement do nothing.
	 * @param syncName
	 **********************************
	 */
	boolean resyncIndexer(String syncName);

	/**
	 **********************************
	 * remove sync congig.
	 * @param syncName
	 * @return
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
