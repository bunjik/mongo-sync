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
import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeSet;

import org.bson.BsonTimestamp;
import org.elasticsearch.action.admin.indices.alias.get.GetAliasesRequest;
import org.elasticsearch.action.admin.indices.exists.indices.IndicesExistsRequest;
import org.elasticsearch.action.admin.indices.mapping.get.GetMappingsResponse;
import org.elasticsearch.action.admin.indices.refresh.RefreshRequest;
import org.elasticsearch.action.support.IndicesOptions;
import org.elasticsearch.action.update.UpdateRequest;
import org.elasticsearch.client.Client;
import org.elasticsearch.cluster.metadata.AliasMetaData;
import org.elasticsearch.cluster.metadata.MappingMetaData;
import org.elasticsearch.common.collect.ImmutableOpenMap;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.json.JsonXContent;
import org.elasticsearch.index.IndexNotFoundException;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.SearchHits;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.carrotsearch.hppc.cursors.ObjectObjectCursor;

import info.bunji.mongodb.synces.Status;
import info.bunji.mongodb.synces.SyncConfig;

/**
 ************************************************
 * elasticsearch utilities.
 * @author Fumiharu Kinoshita
 ************************************************
 */
public class EsUtils {

	private static final Logger logger = LoggerFactory.getLogger(EsUtils.class);

	private EsUtils() {
		// do nothing.
	}

	/**
	 ********************************************
	 * check index exists.
	 * @param esClient elasticsearch client
	 * @param indexName index name
	 * @return true if index exists, otherwise false
	 ********************************************
	 */
	public static boolean isExistsIndex(Client esClient, String indexName) {
		return esClient.admin()
						.indices()
						.exists(new IndicesExistsRequest(indexName))
						.actionGet()
						.isExists();
	}

	/**
	 ********************************************
	 * check index type empty or not exists.
	 * @param esClient elasticsearch client
	 * @param indexName index name
	 * @param types type names
	 * @return true if empty or not exists, otherwise false
	 ********************************************
	 */
	public static boolean isEmptyTypes(Client esClient, String indexName, Collection<String> types) {
		try {
			return (esClient.prepareSearch(indexName)
							.setTypes(types.toArray(new String[types.size()]))
							.setQuery(QueryBuilders.matchAllQuery())
							.setSize(0)
							.execute()
							.actionGet()
							.getHits()
							.getTotalHits() == 0);
		} catch (IndexNotFoundException infe) {
			return true;
		}
 	}

	/**
	 ********************************************
	 * check index empty.
	 * @param esClient elasticsearch client
	 * @param indexName index name
	 * @return true if index is empty, otherwise false.
	 ********************************************
	 */
	public static boolean isEmptyIndex(Client esClient, String indexName) {
		SearchHits hits = esClient.prepareSearch(indexName)
								.setSize(0)
								.execute()
								.actionGet()
								.getHits();
		return (hits.getTotalHits() == 0);
	}

	/**
	 ********************************************
	 * get index aliases.
	 * @param esClient elasticsearch client
	 * @param indexNames index names
	 * @return alias map
	 ********************************************
	 */
	public static Map<String, Collection<String>> getIndexAliases(Client esClient, List<String> indexNames) {
		IndicesOptions indicesOptions = IndicesOptions.fromOptions(true, true, false, false);
		Iterator<ObjectObjectCursor<String, List<AliasMetaData>>> it =
				esClient.admin()
						.indices()
						.getAliases(new GetAliasesRequest()
										.indices(indexNames.toArray(new String[indexNames.size()]))
										.indicesOptions(indicesOptions))
						.actionGet()
						.getAliases()
						.iterator();

		// convert result
		Map<String, Collection<String>> aliasMap = new HashMap<>();
		while (it.hasNext()) {
			ObjectObjectCursor<String, List<AliasMetaData>> entry = it.next();
			for (AliasMetaData meta : entry.value) {
				Collection<String> aliases = aliasMap.get(entry.key);
				if (aliases == null) {
					aliases = new TreeSet<>();
					aliasMap.put(entry.key, aliases);
				}
				aliases.add(meta.alias());
			}
		}
		return aliasMap;
	}

	/**
	 **********************************
	 * get field mapping.
	 * @param esClient elasticsearch client
	 * @param indexName index name
	 * @return field mappings
	 **********************************
	 */
	public static Map<String, Object> getMapping(Client esClient, String indexName) {
		GetMappingsResponse res = esClient.admin().indices()
							.prepareGetMappings(indexName)
							.execute().actionGet();

		Map<String,Object> result = new LinkedHashMap<>();
		ImmutableOpenMap<String, MappingMetaData> mapping = res.getMappings().get(indexName);
		for (ObjectObjectCursor<String, MappingMetaData> o : mapping) {
			try {
				result.put(o.key, o.value.sourceAsMap());
			} catch (IOException ioe) {
				//ioe.printStackTrace();
			}
		}
		return result;
	}

	/**
	 ********************************************
	 * refresh index.
	 * @param esClient elasticsearch client
	 * @param indexName index name
	 * @return true if refresh successed, otherwise false
	 ********************************************
	 */
	public static boolean refreshIndex(Client esClient, String indexName) {
		if (isExistsIndex(esClient, indexName)) {
			esClient.admin().indices().refresh(new RefreshRequest(indexName)).actionGet();
		}
		return true;
	}

	/**
	 ********************************************
	 * delete index.
	 * @param esClient elasticsearch client
	 * @param indexName index name
	 * @return true if delete successed, otherwise false
	 ********************************************
	 */
	public static boolean deleteIndex(Client esClient, String indexName) {
		if (EsUtils.isExistsIndex(esClient, indexName)) {
			esClient.admin().indices().prepareDelete(indexName).execute().actionGet();
		}
		return true;
	}

//	/**
//	 ********************************************
//	 * create document insert/updete request.
//	 * @param index index name
//	 * @param type  index type
//	 * @param id    document id
//	 * @param json  document data
//	 * @return insert/updete request
//	 ********************************************
//	 */
//	public static IndexRequest makeIndexRequest(String index, String type, String id, String json) {
//		return new IndexRequest(index, type, id).source(json);
//	}

	/**
	 ********************************************
	 * create status update request.
	 * @param config sync config
	 * @param status sync status
	 * @param ts mongo oplog timmestamp
	 * @return status update request
	 ********************************************
	 */
	public static UpdateRequest makeStatusRequest(SyncConfig config, Status status, BsonTimestamp ts) {
		XContentBuilder content = makeStatusContent(status, config.getSyncCount(), ts);
		return new UpdateRequest(config.getConfigDbName(), "status", config.getSyncName())
						.doc(content)
						.docAsUpsert(true);
	}

	/**
	 ********************************************
	 * ステータス更新用のJSON文字列を生成する.
	 * @param status sync status
	 * @param indexCnt indexed count
	 * @param ts mongo oplog timmestamp
	 * @return json
	 ********************************************
	 */
	public static String makeStatusJson(Status status, Long indexCnt, BsonTimestamp ts) {
		String json = "{}";
		XContentBuilder content = makeStatusContent(status, indexCnt, ts);
		if (content != null) {
			try {
				json = content.string();
			} catch (IOException ioe) {}
		}
		return json;
	}

	/**
	 ********************************************
	 * create sync status request.
	 * @param status
	 * @param indexCnt
	 * @param ts
	 * @return
	 * @throws IOException
	 ********************************************
	 */
	private static XContentBuilder makeStatusContent(Status status, Long indexCnt, BsonTimestamp ts) {
		try {
			XContentBuilder builder = JsonXContent.contentBuilder();
			builder.startObject();
			if (status != null) {
				builder.field("status", status);
			}
			if (ts != null) {
				builder.startObject("lastOpTime")
						.field("seconds", ts.getTime())
						.field("inc", ts.getInc())
						.endObject();
			}
			if (indexCnt != null) {
				builder.field("indexCnt", indexCnt);
			}
			return builder.endObject();
		} catch (IOException ioe) {
			return null;
		}
	}
}
