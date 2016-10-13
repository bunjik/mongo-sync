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
import java.util.EventListener;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Objects;
import java.util.TreeMap;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

import org.apache.commons.lang3.builder.ToStringBuilder;
import org.apache.commons.lang3.builder.ToStringStyle;
import org.bson.BsonTimestamp;
import org.elasticsearch.action.WriteConsistencyLevel;
import org.elasticsearch.action.bulk.BulkAction;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.delete.DeleteRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.update.UpdateRequest;
import org.elasticsearch.client.Client;
import org.elasticsearch.index.IndexNotFoundException;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.SearchHits;
import org.elasticsearch.search.sort.SortBuilders;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.gson.Gson;

import info.bunji.asyncutil.AsyncExecutor;
import info.bunji.asyncutil.AsyncProcess;
import info.bunji.mongodb.synces.util.EsUtils;

/**
 ************************************************
 * 同期状態を監視するクラス.
 * この処理は同期設定単位ではなく、全同期設定に対するチェックを一括して行う。
 *
 * @author Fumiharu Kinoshita
 ************************************************
 */
public class StatusCheckProcess extends AsyncProcess<Boolean> implements IndexerProcess.Listener {

	private Logger logger = LoggerFactory.getLogger(getClass());

	private Client esClient;

	private long checkInterval;

	/** 同期設定単位の管理 */
	private ConcurrentMap<String, IndexerProcess> indexerMap = new ConcurrentHashMap<>();

	/**
	 **********************************
	 *
	 * @param esClient
	 * @param checkInterval
	 **********************************
	 */
	StatusCheckProcess(Client esClient, long checkInterval) {
		this.esClient = esClient;
		this.checkInterval = checkInterval;
	}

	public boolean isRunning(String syncName) {
		return indexerMap.containsKey(syncName);
	}

	/**
	 *
	 * @return
	 */
	public Map<String, Object> getConfigs() {
		SearchResponse res = esClient.prepareSearch(SyncConfig.STATUS_INDEX)
				.setTypes("config", "status")
				.addSort(SortBuilders.fieldSort("_type"))
				.setSize(1000)
				.execute()
				.actionGet();

		Map<String, Map<String, Object>> tmpConfig = new HashMap<>();
		Map<String, SyncStatus> tmpStatus = new HashMap<>();
		for (SearchHit hit : res.getHits().getHits()) {
			String id = hit.getId();
			if ("config".equals(hit.getType())) {
				tmpConfig.put(id, hit.getSource());
			} else {
				tmpStatus.put(id, new SyncStatus(hit.getSource()));
			}
		}

		Map<String, Object> mergeConfigs = new TreeMap<>();
		for (Entry<String, Map<String, Object>> entry : tmpConfig.entrySet()) {
			Map<String, Object> config = new TreeMap<>();

			SyncStatus status = tmpStatus.get(entry.getKey());
			if (status != null) {
				config.put("status", status.getStatus().toString());
				if (status.getLastOpTime() != null) {
					config.put("lastTimestamp", status.getLastOpTime().getTime() * 1000L);
				}
				config.put("indexCnt", status.getIndexCnt());
			} else {
				//config.put("status", "UNKNOWN");
			}
			config.put("config", entry.getValue());
			mergeConfigs.put(entry.getKey(), config);
		}
		return mergeConfigs;
	}

	/*
	 **********************************
	 * (非 Javadoc)
	 * @see info.bunji.asyncutil.AsyncProcess#execute()
	 **********************************
	 */
	@Override
	protected void execute() throws Exception {

		Gson gson = new Gson();

		while (!isInterrupted()) {
			try {
//				// esからステータスを取得
//				Map<String, SyncConfig> tmpConfigs = new HashMap<>();
//				for (Entry<String, Object> entry : getConfigs().entrySet()) {
//					String syncName = entry.getKey();
//
//					@SuppressWarnings("unchecked")
//					Map<String, Object> config = Map.class.cast(entry.getValue());
//					SyncConfig syncConf = JSON.decode(JSON.encode(config.get("config")), SyncConfig.class);
//
//					syncConf.setStatus(Status.valueOf(Objects.toString(config.get("status"), "UNKNOWN")));
//					syncConf.setLastOpTime(BsonTimestamp.class.cast(config.get("lastTimestamp")));
//					tmpConfigs.put(syncName, syncConf);
//				}
//
//				for (Entry<String, SyncConfig> entry : tmpConfigs.entrySet()) {
//					String syncName = entry.getKey();
//					SyncConfig config = entry.getValue();
//					DocumentExtractor extractor = null;
//
//					if (!indexerMap.containsKey(syncName)) {
//						// 初期インポート(config != null && status == null)
//						if (config.getStatus().equals(Status.UNKNOWN)
//								&& !EsUtils.isExistsIndex(esClient, config.getIndexName())) {
//							extractor = new DocumentExtractor(config, null);
//							config.setStatus(Status.STARTING);
//						} else {
//							// インポート対象のインデックスが存在
//							logger.error("import index already exists.[" + config.getIndexName() + "]");
//							esClient.update(EsUtils.makeStatusRequest(config, Status.START_FAILED.name(), null));
//							config.setStatus(Status.INITIAL_IMPORT_FAILED);
//						}
//					} else {
//						// config != null && status != null
//						IndexerInfo indexerInfo = indexerMap.get(syncName);
//						if (indexerInfo == null) {
//							if (Status.RUNNING.equals(config.getStatus())) {
//								// 起動時の再開
//								extractor = new DocumentExtractor(config, config.getLastOpTime());
//							}
//						} else {
//							if (!Status.RUNNING.equals(config.getStatus())
//								&& !Status.INITIAL_IMPORTING.equals(config.getStatus())
//								&& !Status.STARTING.equals(config.getStatus())) {
//								// 実行中処理の停止
//								logger.debug("indexer stopping.[" + syncName + "]");
//								indexerInfo.indexerProc.stop();
//								indexerMap.remove(syncName);
//							}
//						}
//					}
//
//					if (extractor != null) {
//						// 同期処理の開始
//						OplogExtractor oplogExtractor = new OplogExtractor(config, config.getLastOpTime());
//						IndexerProcess indexerProc = new IndexerProcess(esClient, config, this,
//											AsyncExecutor.execute(Arrays.asList(extractor, oplogExtractor), 1, 5000));
//
//						AsyncExecutor.execute(indexerProc);
//						indexerMap.put(syncName, new IndexerInfo(indexerProc, "RUNNING"));
//					}
//				}
//
//				// configの存在しないindexerは停止する
//				for (Entry<String, IndexerInfo> entry : indexerMap.entrySet()) {
//					if (!tmpConfigs.containsKey(entry.getKey())) {
//						entry.getValue().getIndexer().stop();
//						//indexerMap.remove(entry.getKey());
//					}
//				}


				// esからステータスを取得
				SearchResponse res = esClient.prepareSearch(SyncConfig.STATUS_INDEX)
										.setTypes("status", "config")
										.setSize(1000)
										.execute()
										.actionGet();

				Map<String, SyncConfig> tmpConfigs = new HashMap<>();
				Map<String, SyncStatus> tmpStatus = new HashMap<>();

				SearchHits hits = res.getHits();
				for (SearchHit hit : hits.getHits()) {
					String id = hit.getId();
					String type = hit.getType();

					if (type.equals("config")) {
						// config
						SyncConfig config = gson.fromJson(hit.getSourceAsString(), SyncConfig.class);
						//logger.debug(config.toString());
						config.setSyncName(id);
						tmpConfigs.put(id, config);
					} else {
						// status
						SyncStatus status = new SyncStatus(hit.getSource());
						//logger.debug(status.toString());
						tmpStatus.put(id, status);
					}
				}

				// TODO 要見直し
				for (Entry<String, SyncConfig> entry : tmpConfigs.entrySet()) {
					String syncName = entry.getKey();
					SyncConfig config = entry.getValue();

					CollectionExtractor extractor = null;

					if (!tmpStatus.containsKey(syncName)) {
						if (!indexerMap.containsKey(syncName)) {
							// 初期インポート(config != null && status == null)
							if (!EsUtils.isExistsIndex(esClient, config.getIndexName())) {
								extractor = new CollectionExtractor(config, null);
								config.setStatus(Status.STARTING);
							} else {
								// インポート対象のインデックスが存在
								logger.error("import index already exists.[" + config.getIndexName() + "]");
								esClient.update(EsUtils.makeStatusRequest(config, Status.START_FAILED, null));
								config.setStatus(Status.INITIAL_IMPORT_FAILED);
							}
						}
					} else {
						// config != null && status != null
						IndexerProcess indexer = indexerMap.get(syncName);
						SyncStatus statusInfo = tmpStatus.get(syncName);
						config.addSyncCount(statusInfo.getIndexCnt());
						if (indexer == null) {
							if (Status.RUNNING.equals(statusInfo.getStatus())) {
								// 起動時の再開
								extractor = new CollectionExtractor(config, statusInfo.getLastOpTime());
							}
						} else {
							if (!Status.RUNNING.equals(statusInfo.getStatus())
								&& !Status.INITIAL_IMPORTING.equals(statusInfo.getStatus())
								&& !Status.STARTING.equals(statusInfo.getStatus())) {
								// 実行中処理の停止
								logger.debug("indexer stopping.[" + syncName + "]");
								indexer.stop();
								indexerMap.remove(syncName);
							}
						}
					}

					if (extractor != null) {
						// 同期処理の開始
						BsonTimestamp ts = null;
						if (tmpStatus.containsKey(syncName)) {
							ts = tmpStatus.get(syncName).getLastOpTime();
						}
						List<AsyncProcess<SyncOperation>> procList = new ArrayList<>();
						if (ts == null) {
							procList.add(extractor);
						}
						procList.add(new OplogExtractor(config, ts));
						IndexerProcess indexerProc = new IndexerProcess(esClient, config, this,
															AsyncExecutor.execute(procList, 1, 5000));
						AsyncExecutor.execute(indexerProc);
						indexerMap.put(syncName, indexerProc);
					}
				}

				// configの存在しないindexerは停止する
				for (Entry<String, IndexerProcess> entry : indexerMap.entrySet()) {
					if (!tmpConfigs.containsKey(entry.getKey())) {
						entry.getValue().stop();
						//indexerMap.remove(entry.getKey());
					}
				}
			} catch (IndexNotFoundException infe) {
				// setting index not found.
			} catch (Exception e) {
				logger.error(e.getMessage(), e);
				//logger.error(e.getMessage());
			}

			// wait next check.
			Thread.sleep(checkInterval);
		}
	}

	/*
	 **********************************
	 * (非 Javadoc)
	 * @see info.bunji.asyncutil.AsyncProcess#postProcess()
	 **********************************
	 */
	@Override
	protected void postProcess() {
		for (IndexerProcess indexer : indexerMap.values()) {
			indexer.stop();
		}
		super.postProcess();
	}

	/**
	 **********************************
	 * 同期を開始する.
	 * @param syncName
	 * @return
	 **********************************
	 */
	public boolean startIndexer(String syncName) {
		String json = EsUtils.makeStatusJson(Status.RUNNING, null, null);
		UpdateRequest req = new UpdateRequest(SyncConfig.STATUS_INDEX, "status", syncName)
				.doc(json).consistencyLevel(WriteConsistencyLevel.ALL);
		// ステータス更新
		esClient.update(req).actionGet();
		// インデックスのリフレッシュ
		EsUtils.refreshIndex(esClient, SyncConfig.STATUS_INDEX);
		logger.debug("[{}] start Indexer.", syncName);
		return true;
	}

	/**
	 **********************************
	 * 同期を停止する.
	 * @param syncName
	 * @return
	 **********************************
	 */
	public boolean stopIndexer(String syncName) {
		IndexerProcess indexer = indexerMap.get(syncName);
		Long count = null;
		if (indexer != null) {
			count = indexer.getConfig().getSyncCount();
		}
		String json = EsUtils.makeStatusJson(Status.STOPPED, count, null);
		UpdateRequest req = new UpdateRequest(SyncConfig.STATUS_INDEX, "status", syncName)
				.doc(json).consistencyLevel(WriteConsistencyLevel.ALL);
		// ステータス更新
		esClient.update(req).actionGet();
		// インデックスのリフレッシュ
		EsUtils.refreshIndex(esClient, SyncConfig.STATUS_INDEX);
		logger.debug("[{}] stop Indexer.", syncName);
		return true;
	}

	/**
	 **********************************
	 * 同期設定を削除する.
	 * @param syncName
	 * @return
	 **********************************
	 */
	public boolean deleteIndexer(String syncName) {
		// 設定情報の削除
		BulkRequest bulkReq = BulkAction.INSTANCE.newRequestBuilder(esClient)
						.add(new DeleteRequest(SyncConfig.STATUS_INDEX).type("config").id(syncName))
						.add(new DeleteRequest(SyncConfig.STATUS_INDEX).type("status").id(syncName))
						.setConsistencyLevel(WriteConsistencyLevel.ALL)
						.request();
		esClient.bulk(bulkReq).actionGet();
		// インデックスのリフレッシュ
		EsUtils.refreshIndex(esClient, SyncConfig.STATUS_INDEX);
		logger.debug("[{}] delete Indexer.", syncName);
		return true;
	}

	/**
	 *
	 * @param syncName
	 * @return
	 * @throws InterruptedException
	 */
	public boolean resyncIndexer(String syncName) throws InterruptedException {
		Map<String, Object> config = (Map<String, Object>) getConfigs().get(syncName);
		if (config != null && config.containsKey("config")) {
			stopIndexer(syncName);
			while (true) {
				if (!isRunning(syncName)) break;
				Thread.sleep(500);
			}

			// delete index
			String indexName = ((Map<String, String>) config.get("config")).get("indexName");
			if (EsUtils.isExistsIndex(esClient, indexName)) {
				esClient.admin().indices().prepareDelete(indexName)
								.execute().actionGet();
			}

			// delete sync status
			esClient.prepareDelete(SyncConfig.STATUS_INDEX, "status", syncName)
							.setRefresh(true)
							.setConsistencyLevel(WriteConsistencyLevel.ALL)
							.execute()
							.actionGet();
			while (true) {
				if (isRunning(syncName)) break;
				logger.debug("waiting resync.");
				Thread.sleep(500);
			}
			Thread.sleep(500);
			Thread.sleep(500);
		}
		return true;
	}


	@Override
	public void onIndexerStop(String syncName) {
		indexerMap.remove(syncName);
	}

	/**
	 *
	 */
	static interface Listener extends EventListener {
		void onStop();
	}

	/**
	 ********************************************
	 *
	 ********************************************
	 */
	private static final class SyncStatus {
		private Status status;
		private BsonTimestamp lastOpTime;
		private long indexCnt;

		public SyncStatus(Map<String, Object> map) {
			this.status = Status.valueOf(Objects.toString(map.get("status"), ""));
			Map<String, Number> ts = (Map<String, Number>) map.get("lastOpTime");
			if (ts != null) {
				int sec = ts.get("seconds").intValue();
				int inc = ts.get("inc").intValue();
				this.lastOpTime = new BsonTimestamp(sec, inc);
			}
			try {
				this.setIndexCnt(Long.valueOf(map.get("indexCnt").toString()));
			} catch (Exception e) {
			}
		}

		public Status getStatus() {
			return status;
		}

		public BsonTimestamp getLastOpTime() {
			return lastOpTime;
		}

		public long getIndexCnt() {
			return indexCnt;
		}

		public void setIndexCnt(long indexCnt) {
			this.indexCnt = indexCnt;
		}

		public String toJson() {
			return EsUtils.makeStatusJson(status, indexCnt, lastOpTime);
		}

		@Override
		public String toString() {
			return ToStringBuilder.reflectionToString(this, ToStringStyle.SHORT_PREFIX_STYLE);
		}
	}
}
