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
import java.util.Collection;
import java.util.EventListener;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
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
import org.elasticsearch.client.transport.NoNodeAvailableException;
import org.elasticsearch.index.IndexNotFoundException;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.sort.SortBuilders;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.Maps;
import com.google.common.collect.Maps.EntryTransformer;

import info.bunji.asyncutil.AsyncExecutor;
import info.bunji.asyncutil.AsyncProcess;
import info.bunji.mongodb.synces.util.EsUtils;
import net.arnx.jsonic.JSON;

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

	/**
	 **********************************
	 *
	 * @param syncName
	 * @return
	 **********************************
	 */
	public boolean isRunning(String syncName) {
		return indexerMap.containsKey(syncName);
	}

	/**
	 **********************************
	 * 指定インデックスのフィールドマッピングを取得する.
	 * @param indexName 対象インデックス名
	 * @return インデックスのフィールドマッピング情報
	 **********************************
	 */
	public Map<String, Object> getMapping(String indexName) {
		return EsUtils.getMapping(esClient, indexName);
	}

	/**
	 **********************************
	 *
	 * @return
	 **********************************
	 */
	public Map<String, SyncConfig> getConfigList() {
		return getConfigList(false);
	}

	public Map<String, SyncConfig> getConfigList(boolean withAlias) {
		SearchResponse res = esClient.prepareSearch(SyncConfig.STATUS_INDEX)
				.setTypes("config", "status")
				.addSort(SortBuilders.fieldSort("_type"))
				.setSize(1000)
				.execute()
				.actionGet();

		// failed shards check
		if (res.getFailedShards() > 0) {
			logger.trace("failure shards found in config index.");
			throw new IndexNotFoundException("failed shards found.");
		}

		List<String> indexNames = new ArrayList<>();
		Map<String, SyncConfig> configMap = new TreeMap<>();
		for (SearchHit hit : res.getHits().getHits()) {
			if ("config".equals(hit.getType())) {
				SyncConfig config = JSON.decode(hit.getSourceAsString(), SyncConfig.class);
				config.setSyncName(hit.getId());
				configMap.put(hit.getId(), config);
				indexNames.add(config.getIndexName());
			} else if ("status".equals(hit.getType()) && configMap.containsKey(hit.getId())) {
				SyncConfig config = configMap.get(hit.getId());
				SyncStatus status = new SyncStatus(hit.sourceAsMap());
				config.setStatus(status.getStatus());
				config.setLastOpTime(status.getLastOpTime());
				if (indexerMap.get(config.getSyncName()) != null) {
					config.addSyncCount(indexerMap.get(config.getSyncName()).getConfig().getSyncCount());
				}

			}
		}

		if (withAlias) {
			try {
				final Map<String, Collection<String>> aliasMap = EsUtils.getIndexAliases(esClient, indexNames);
				configMap = Maps.transformEntries(configMap, new EntryTransformer<String, SyncConfig, SyncConfig>() {
					@Override
					public SyncConfig transformEntry(String key, SyncConfig value) {
						value.setAliases(aliasMap.get(value.getIndexName()));
						return value;
					}
				});
			} catch (IndexNotFoundException e) {
				// do nothing.
			}
		}

		return configMap;
	}

	/*
	 **********************************
	 * (非 Javadoc)
	 * @see info.bunji.asyncutil.AsyncProcess#execute()
	 **********************************
	 */
	@Override
	protected void execute() throws Exception {

		long interval = checkInterval;
		int retry = 0;

		while (!isInterrupted()) {
			try {
				// esからステータスを取得
				Map<String, SyncConfig> configs = getConfigList();
				for (Entry<String, SyncConfig> entry : configs.entrySet()) {
					String syncName = entry.getKey();
					SyncConfig config = entry.getValue();

					AsyncProcess<SyncOperation> extractor = null;
					IndexerProcess indexer = indexerMap.get(syncName);
					if (indexer == null) {
						if (config.getStatus() == null) {
							// initial import
							if (config.getImportCollections().isEmpty()) {
								// check exists index. if import all collections
								if (EsUtils.isExistsIndex(esClient, config.getIndexName())) {
									// インポート対象のインデックスが存在
									logger.error("[{}] import index already exists.[index:{}]", syncName, config.getIndexName());
									esClient.update(EsUtils.makeStatusRequest(config, Status.INITIAL_IMPORT_FAILED, null));
									continue;
								}
							} else {
								// check exists types. if specified collections
								if (!EsUtils.isEmptyTypes(esClient, config.getIndexName(), config.getImportCollections())) {
									// インポート対象のインデックスが空でない
									logger.error("[{}] import type already exists.[index:{}]", syncName, config.getIndexName());
									esClient.update(EsUtils.makeStatusRequest(config, Status.INITIAL_IMPORT_FAILED, null));
									continue;
								}
							}
							extractor = new CollectionExtractor(config, null);
							esClient.update(EsUtils.makeStatusRequest(config, Status.STARTING, null));

						} else if (Status.RUNNING == config.getStatus()) {
							// 起動時の再開
							extractor = new OplogExtractor(config, config.getLastOpTime());
						}
					} else {
						if (config.getStatus() != null) {
							switch (config.getStatus()) {
							case INITIAL_IMPORT_FAILED :
							case STOPPED :
								// stop indexer
								logger.debug("[{}] stopping indexer.", syncName);
								indexer.stop();
								indexerMap.remove(syncName);
								break;

							default :
								// do nothing.
								break;
							}
						}
					}

					if (extractor != null) {
						// 同期処理の開始
						BsonTimestamp ts = config.getLastOpTime();
						List<AsyncProcess<SyncOperation>> procList = new ArrayList<>();
						procList.add(extractor);
						if (extractor instanceof CollectionExtractor) {
							procList.add(new OplogExtractor(config, ts));
						}
						IndexerProcess indexerProc = new IndexerProcess(esClient, config, this,
															AsyncExecutor.execute(procList, 1, 5000));
						indexerMap.put(syncName, indexerProc);
						AsyncExecutor.execute(indexerProc);
					}
				}

				// configの存在しないindexerは停止する
				for (Entry<String, IndexerProcess> entry : indexerMap.entrySet()) {
					if (!configs.containsKey(entry.getKey())) {
						entry.getValue().stop();
						//indexerMap.remove(entry.getKey());
					}
				}

				retry = 0;
				interval = checkInterval;

			} catch (IndexNotFoundException infe) {
				// setting index not found.

			} catch (NoNodeAvailableException nnae) {
//			} catch (ElasticsearchException ee) {
				retry++;
				interval = (long) Math.min(60, Math.pow(2, retry)) * 1000;
				logger.warn("es connection error. (retry after {} ms)", interval);
			} catch (Exception e) {
				//logger.error(e.getMessage(), e);
				logger.error(e.getMessage());
			}

			// wait next check.
			Thread.sleep(interval);
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
				.doc(json).consistencyLevel(WriteConsistencyLevel.DEFAULT);
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
				.doc(json).consistencyLevel(WriteConsistencyLevel.DEFAULT);
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
						.setConsistencyLevel(WriteConsistencyLevel.DEFAULT)
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
		SyncConfig config = getConfigList().get(syncName);
		if (config != null) {
			stopIndexer(syncName);
			int count = 10;
			do {
				if (!isRunning(syncName)) break;
				Thread.sleep(500);
				count--;
			} while (count > 0);

			// delete index
			String indexName = config.getIndexName();
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

			// waiting start
			count = 10;
			do {
				if (isRunning(syncName)) break;
				logger.debug("waiting resync.");
				Thread.sleep(500);
				count--;
			} while (count > 0);
			Thread.sleep(1000);
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
		private String lastError;

		public SyncStatus(Map<String, Object> map) {
			this.status = Status.fromString(map.get("status"));
			Map<String, Number> ts = (Map<String, Number>) map.get("lastOpTime");
			if (ts != null) {
				int sec = ts.get("seconds").intValue();
				int inc = ts.get("inc").intValue();
				this.lastOpTime = new BsonTimestamp(sec, inc);
			}
		}

		public Status getStatus() {
			return status;
		}

		public BsonTimestamp getLastOpTime() {
			return lastOpTime;
		}

//		public String toJson() {
//			return EsUtils.makeStatusJson(status, null, lastOpTime);
//		}

		public String getLastError() {
			return lastError;
		}

		public void setLastError(String lastError) {
			this.lastError = lastError;
		}

		@Override
		public String toString() {
			return ToStringBuilder.reflectionToString(this, ToStringStyle.SHORT_PREFIX_STYLE);
		}
	}
}
