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

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

import org.bson.BsonTimestamp;
import org.bson.Document;

import com.mongodb.BasicDBObject;
import com.mongodb.MongoClient;
import com.mongodb.client.MongoCollection;

import info.bunji.asyncutil.AsyncExecutor;
import info.bunji.asyncutil.AsyncIntervalProcess;
import info.bunji.asyncutil.AsyncProcess;
import info.bunji.asyncutil.AsyncResult;

/**
 ************************************************
 * status check process base implementation
 * @author Fumiharu Kinoshita
 ************************************************
 */
public abstract class StatusChecker<T> extends AsyncIntervalProcess<T>
										implements SyncProcess.Listener {

	/** running indexer map */
	private ConcurrentMap<String, SyncProcess> indexerMap = new ConcurrentHashMap<>();

	private final int syncQueueLimit;

	/**
	 **********************************
	 * constructor.
	 * @param interval check Interval
	 * @param syncQueueLimit 
	 **********************************
	 */
	public StatusChecker(long interval, int syncQueueLimit) {
		super(interval);
		this.syncQueueLimit = syncQueueLimit;
	}

	/*
	 **********************************
	 * (non Javadoc)
	 * @see info.bunji.asyncutil.AsyncIntervalProcess#executeInterval()
	 **********************************
	 */
	@Override
	protected final boolean executeInterval() {
		if (isInterrupted()) {
			return false;	// finish status check.
		}

		// check status
		return doCheckStatus();
	}

	/**
	 **********************************
	 * check status
	 * @return true if continue check process, false to stop check process
	 **********************************
	 */
//	protected abstract boolean doCheckStatus();
	protected boolean doCheckStatus() {
		try {
			// check status
			checkStatus();
		} catch (Throwable t) {
			// 
			//doCheckError(t);
		}
		return true;
	}

	
	protected void doCheckError(Throwable t) throws Exception {
		
	}

	/**
	 **********************************
	 * get sync configs.
	 * @return sync configs
	 **********************************
	 */
	public Map<String, SyncConfig> getConfigs() {
		return getConfigs(false);
	}

	/**
	 **********************************
	 * get sync configs.
	 * @param withExtendInfo
	 * @return sync configs
	 **********************************
	 */
	public abstract Map<String, SyncConfig> getConfigs(boolean withExtendInfo);

	/**
	 **********************************
	 * create indexer.
	 * @param config sync config
	 * @param syncData mongo sync data
	 * @return created sync process
	 **********************************
	 */
	protected abstract SyncProcess createSyncProcess(SyncConfig config, AsyncResult<SyncOperation> syncData);

	/**
	 **********************************
	 * check sync status.
	 * @throws Exception error occurred
	 **********************************
	 */
	protected void checkStatus() throws Exception {
		Map<String, SyncConfig> configs = getConfigs();
		for (Entry<String, SyncConfig> entry : configs.entrySet()) {
			SyncConfig config = entry.getValue();
			AsyncProcess<SyncOperation> extractor = null;

			String syncName = config.getSyncName();
			if (config.getStatus() == null) {
				if (!isRunning(config.getSyncName())) {
					// initial import
					if (validateInitialImport(config)) {
						try (MongoClient client = MongoClientService.getClient(config)) {
							// get current oplog timestamp.
							MongoCollection<Document> oplog  = client.getDatabase("local").getCollection("oplog.rs");
							Document lastOp = oplog.find().sort(new BasicDBObject("$natural", -1)).limit(1).first();
							config.setLastOpTime(lastOp.get("ts", BsonTimestamp.class));
						}
						// create extractor for initial import.
						extractor = new CollectionExtractor(config, null);
//extractor = new CollectionExtractor(config, config.getLastOpTime());
						updateStatus(config, Status.INITIAL_IMPORTING, null);
					} else {
						// faild initial import.
						updateStatus(config, Status.INITIAL_IMPORT_FAILED, null);
					}
				}
			} else {
				switch (config.getStatus()) {

				case RUNNING :
					// restart sync. if indexer not running.
					if (!isRunning(syncName)) {
						logger.debug("[{}] restart sync.", syncName);
						// create extractor for oplog sync.
						extractor = new OplogExtractor(config, config.getLastOpTime());
					}
					break;
	
				case INITIAL_IMPORT_FAILED :
				case START_FAILED :
				case STOPPED :
					if (isRunning(syncName)) {
						SyncProcess indexer = indexerMap.remove(syncName);
						indexer.stop();
					}
					break;
	
				default :
					// do nothing status
					// - INITIAL_IMPORTING
					// - WAITING_RETRY
					// - UNKNOWN
					break;
				}
			}

			if (extractor != null) {
				// start sync
				List<AsyncProcess<SyncOperation>> procList = new ArrayList<>();
				procList.add(extractor);
				if (extractor instanceof CollectionExtractor) {
					BsonTimestamp ts = config.getLastOpTime();
					procList.add(new OplogExtractor(config, ts));
				}
				AsyncResult<SyncOperation> result = AsyncExecutor.execute(procList, 1, syncQueueLimit);
				SyncProcess indexer = createSyncProcess(config, result);
				indexerMap.put(syncName, indexer);
				AsyncExecutor.execute(indexer);

//				AsyncResult<SyncOperation> result = AsyncExecutor.execute(extractor, syncQueueLimit);
//				SyncProcess indexer = createSyncProcess(config, result);
//				indexerMap.put(syncName, indexer);
//				AsyncExecutor.execute(indexer);
			}

		}

		// stop indexer, if config not exists.
		for (String syncName : getIndexerNames()) {
			if (!configs.containsKey(syncName) && isRunning(syncName)) {
				getIndexer(syncName).stop();
			}
		}
		return;
	}

	/**
	 **********************************
	 * update sync status.
	 * @param config sync config
	 * @param status status
	 * @param ts last sync timestamp
	 **********************************
	 */
	protected abstract void updateStatus(SyncConfig config, Status status, BsonTimestamp ts);

	/**
	 **********************************
	 * get indexer.
	 * @param syncName sync name
	 * @return indexer process. return null if not running indexer
	 **********************************
	 */
	protected SyncProcess getIndexer(String syncName) {
		return indexerMap.get(syncName);
	}

	/**
	 **********************************
	 * get indexer list.
	 * @return running indexer names
	 **********************************
	 */
	protected Set<String> getIndexerNames() {
		return indexerMap.keySet();
	}

	/**
	 **********************************
	 * check initial import.
	 * @param config sync config
	 * @return true if initial import start validtion ok, otherwise false
	 **********************************
	 */
	protected boolean validateInitialImport(SyncConfig config) {
		return !indexerMap.containsKey(config.getSyncName());
	}

	/**
	 **********************************
	 * start indexer.
	 * @param syncName sync name
	 * @return true if start process successed, otherwise false
	 **********************************
	 */
	public boolean startIndexer(String syncName) {
		boolean ret = indexerMap.containsKey(syncName);
		if (!ret) {
			SyncConfig config = getConfigs().get(syncName);
			if (config != null) {
				updateStatus(config, Status.RUNNING, config.getLastOpTime());
				ret = true;
			}
		} else {
			logger.info("[{}] sync process already running.", syncName);
		}
		return ret;
	}

	/**
	 **********************************
	 * stop indexer.
	 * @param syncName sync name
	 * @return true if stop process successed, otherwise false
	 **********************************
	 */
	public boolean stopIndexer(String syncName) {
		if (isRunning(syncName)) {
			SyncConfig config = getConfigs().get(syncName);
					if (config != null) {
				updateStatus(config, Status.STOPPED, null);
				logger.debug("[{}] stopping sync.", syncName);
			} else {
				// config not found
			}
		} else {
			logger.debug("[{}] sync process not running. ", syncName);
			SyncConfig config = getConfigs().get(syncName);
			updateStatus(config, Status.STOPPED, null);
		}
		return true;
	}

	/**
	 **********************************
	 * resync indexer.
	 * @param syncName sync name
	 * @return true if resync process successed, otherwise false
	 **********************************
	 */
	public boolean resyncIndexer(String syncName) {
		// default not support
		return true;
	}

	/**
	 **********************************
	 * remove sync congig.
	 * @param syncName sync name
	 * @return true if remove config successed, otherwise false
	 **********************************
	 */
	public abstract boolean removeConfig(String syncName);

	/*
	 **********************************
	 * (non Javadoc)
	 * @see info.bunji.asyncutil.AsyncProcess#postProcess()
	 **********************************
	 */
	@Override
	protected void postProcess() {
		for (String syncName : indexerMap.keySet()) {
			stopIndexer(syncName);
		}
		super.postProcess();
	}

	/**
	 **********************************
	 * check sync process running.
	 * @param syncName sync name
	 * @return true if sync process running, otherwise false
	 **********************************
	 */
	public boolean isRunning(String syncName) {
		return indexerMap.containsKey(syncName);
	}

	/*
	 **********************************
	 * (non Javadoc)
	 * @see info.bunji.mongodb.synces.IndexerProcess.Listener#onIndexerStop(java.lang.String)
	 **********************************
	 */
	@Override
	public void onIndexerStop(String syncName) {
		indexerMap.remove(syncName);
	}

//	/**
//	 **********************************
//	 *
//	 **********************************
//	 */
//	static interface Listener extends EventListener {
//		void onStop();
//	}
}
