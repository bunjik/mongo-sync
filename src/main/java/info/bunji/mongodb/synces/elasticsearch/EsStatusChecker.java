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
package info.bunji.mongodb.synces.elasticsearch;

import java.io.IOException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.util.HashSet;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.TreeMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import org.bson.BsonTimestamp;
import org.elasticsearch.action.WriteConsistencyLevel;
import org.elasticsearch.action.bulk.BulkAction;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.delete.DeleteRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.client.transport.NoNodeAvailableException;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.settings.Settings.Builder;
import org.elasticsearch.common.transport.InetSocketTransportAddress;
import org.elasticsearch.common.transport.TransportAddress;
import org.elasticsearch.index.IndexNotFoundException;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.sort.SortBuilders;
import org.elasticsearch.shield.ShieldPlugin;

import com.google.gson.Gson;

import info.bunji.asyncutil.AsyncResult;
import info.bunji.mongodb.synces.AbstractStatusChecker;
import info.bunji.mongodb.synces.MongoEsSync;
import info.bunji.mongodb.synces.Status;
import info.bunji.mongodb.synces.SyncConfig;
import info.bunji.mongodb.synces.SyncOperation;
import info.bunji.mongodb.synces.SyncProcess;
import info.bunji.mongodb.synces.SyncStatus;

/**
 ************************************************
 * sync status checker for elasticsearch.
 * 
 * @author Fumiharu Kinoshita
 ************************************************
 */
public class EsStatusChecker extends AbstractStatusChecker<Boolean> {

	private static Properties defaultProps;

	private Client esClient;

	private int retry = 0;

	public static final String CONFIG_INDEX = ".mongosync";

	static {
		// default settings
		defaultProps = new Properties();
		defaultProps.put("es.hosts", "localhost:9300");
		defaultProps.put("es.clustername", "elasticsearch");
	}

	/**
	 **********************************
	 * constructor.
	 * @param interval check intervel
	 * @throws IOException
	 **********************************
	 */
	public EsStatusChecker(long interval) throws IOException {
		super(interval);
		
		// load setting.
		Properties prop = loadProperties(MongoEsSync.PROPERTY_NAME);

		// TODO コネクション生成は別メソッド化する？
		
		Set<TransportAddress> addresses = new HashSet<>();
		for (String host : prop.getProperty("es.hosts").split(",")) {
			String[] addr = host.split(":");
			String name = addr[0];
			String port = "9300";	// default transport port
			if (addr.length > 1) {
				port = addr[1];
			}
			addresses.add(new InetSocketTransportAddress(
						new InetSocketAddress(InetAddress.getByName(name), Integer.valueOf(port))
						));
		}

		Builder settings = Settings.settingsBuilder()
				//.put("client.transport.ignore_cluster_name", true)
				.put("cluster.name", prop.getProperty("es.clustername"))
				.put("transport.client.sniff", true);

		// es connection with shield auth.
		if (prop.containsKey("es.auth")) {
			logger.info("elasticsearch connection with authentication.");
			settings.put("shield.user", prop.getProperty("es.auth"));
		}

		esClient = TransportClient.builder()
				.addPlugin(ShieldPlugin.class)	// auth for shield plugin
				.settings(settings.build())
				.build()
				.addTransportAddresses(addresses.toArray(new InetSocketTransportAddress[0]));
	}

	/*
	 **********************************
	 * (非 Javadoc)
	 * @see info.bunji.mongodb.synces.AbstractStatusChecker#validateInitialImport(info.bunji.mongodb.synces.SyncConfig)
	 **********************************
	 */
	@Override
	protected boolean validateInitialImport(SyncConfig config) {
		boolean ret = true;
		String syncName = config.getSyncName();
		String indexName = config.getIndexName();

		if (config.getImportCollections().isEmpty()) {
			// all collection sync.
			if (EsUtils.isExistsIndex(esClient, indexName)) {
				// ERROR: target index already exists.
				logger.error("[{}] import index already exists.[index:{}]", syncName, indexName);
				ret = false;
			}
		} else {
			// selected collection sync.
			if (!EsUtils.isEmptyTypes(esClient, indexName, config.getImportCollections())) {
				// ERROR: target index type is not empty.
				logger.error("[{}] import type already exists.[index:{}]", syncName, indexName);
				ret = false;
			}
		}
		return ret;
	}

	/*
	 **********************************
	 * (non Javadoc)
	 * @see info.bunji.mongodb.synces.AbstractStatusChecker#createIndexer(info.bunji.mongodb.synces.SyncConfig, info.bunji.asyncutil.AsyncResult)
	 **********************************
	 */
	@Override
	protected SyncProcess createSyncProcess(SyncConfig config, AsyncResult<SyncOperation> syncData) {
		return new EsSyncProcess(esClient, config, this, syncData);
	}

	/*
	 **********************************
	 * (非 Javadoc)
	 * @see info.bunji.mongodb.synces.AbstractStatusChecker#doCheckStatus()
	 **********************************
	 */
	@Override
	protected boolean doCheckStatus() {

		try {
			// check status
			checkStatus();

			// reset retry count.
			retry = 0;
	
		} catch (IndexNotFoundException infe) {

			// TODO create config index.
			
		} catch (NoNodeAvailableException nnae) {
			retry++;
			long interval = (long) Math.min(60, Math.pow(2, retry)) * 1000;
			logger.warn("es connection error. (retry after " + interval + " ms)", nnae);
			try {
				Thread.sleep(interval);
			} catch (InterruptedException e) {
				; // to nothing.
			}
		} catch (Exception e) {
			logger.error(e.getMessage());
		}
		return true;
	}

	/*
	 **********************************
	 * (non Javadoc)
	 * @see info.bunji.mongodb.synces.AbstractStatusChecker#updateStatus(info.bunji.mongodb.synces.Status)
	 **********************************
	 */
	@Override
	protected void updateStatus(SyncConfig config, Status status, BsonTimestamp ts) {
		esClient.update(EsUtils.makeStatusRequest(config, status, ts)).actionGet();
		// refresh config index
		EsUtils.refreshIndex(esClient, CONFIG_INDEX);
	}

	/*
	 **********************************
	 * (non Javadoc)
	 * @see info.bunji.mongodb.synces.StatusChecker#getConfigs()
	 **********************************
	 */
	@Override
	public Map<String, SyncConfig> getConfigs() {
		// get configs from elasticsearch.
		SearchResponse res = esClient.prepareSearch(CONFIG_INDEX)
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

		Gson gson = new Gson();
		Map<String, SyncConfig> configMap = new TreeMap<>();
		for (SearchHit hit : res.getHits().getHits()) {
			String syncName = hit.getId();
			String type = hit.getType();
			if ("config".equals(type)) {
				SyncConfig config = gson.fromJson(hit.getSourceAsString(), SyncConfig.class);
				config.setSyncName(syncName);
				configMap.put(syncName, config);
			} else if ("status".equals(type) && configMap.containsKey(syncName)) {
				SyncConfig config = configMap.get(hit.getId());
				SyncStatus status = new SyncStatus(hit.sourceAsMap());
				config.setStatus(status.getStatus());
				config.setLastOpTime(status.getLastOpTime());
				if (getIndexer(config.getSyncName()) != null) {
					config.addSyncCount(getIndexer(config.getSyncName()).getConfig().getSyncCount());
				}
			}
		}
/*
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
*/
		return configMap;
	}

	/*
	 **********************************
	 * (non Javadoc)
	 * @see info.bunji.mongodb.synces.AbstractStatusChecker#resyncIndexer(java.lang.String)
	 **********************************
	 */
	@Override
	public boolean resyncIndexer(String syncName) {
		boolean ret = false;
		if (isRunning(syncName)) {
			logger.info("[{}] sync process is running. resync canceled.", syncName);
		} else {
			SyncConfig config = getConfigs().get(syncName);
			if (config != null) {
				// delete index
				String indexName = config.getIndexName();
				if (EsUtils.deleteIndex(esClient, indexName)) {
					// delete sync status
					esClient.prepareDelete(CONFIG_INDEX, "status", syncName)
									.setRefresh(true)
									.setConsistencyLevel(WriteConsistencyLevel.ALL)
									.execute()
									.actionGet();

					// waiting resync
					try {
						CountDownLatch latch = new CountDownLatch(10);
						while(latch.await(500, TimeUnit.MILLISECONDS)) {
							latch.countDown();
							if (isRunning(syncName)) {
								ret = true;
								logger.debug("[{]] resync started.", syncName);
								break;
							}
							logger.debug("[{]] waiting resync.", syncName);
						}
					} catch (InterruptedException e) {}					
				}
			}
		}
		return ret;
	}
	
	/*
	 **********************************
	 * (non Javadoc)
	 * @see info.bunji.mongodb.synces.AbstractStatusChecker#removeConfig(java.lang.String)
	 **********************************
	 */
	@Override
	public boolean removeConfig(String syncName) {
		// 設定情報の削除
		BulkRequest bulkReq = BulkAction.INSTANCE.newRequestBuilder(esClient)
						.add(new DeleteRequest(CONFIG_INDEX).type("config").id(syncName))
						.add(new DeleteRequest(CONFIG_INDEX).type("status").id(syncName))
						.request();
		esClient.bulk(bulkReq).actionGet();
		// refresh index
		EsUtils.refreshIndex(esClient, CONFIG_INDEX);
		logger.debug("[{}] deleted sync setting.", syncName);
		return true;
	}

	/*
	 **********************************
	 * (non Javadoc)
	 * @see info.bunji.mongodb.synces.AbstractStatusChecker#postProcess()
	 **********************************
	 */
	@Override
	protected void postProcess() {
		logger.info("closing elasticsearch connection.");
		esClient.close();
	}
}