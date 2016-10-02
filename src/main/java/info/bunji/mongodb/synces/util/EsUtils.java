/**
 *
 */
package info.bunji.mongodb.synces.util;

import java.io.IOException;

import org.apache.commons.lang3.StringUtils;
import org.bson.BsonTimestamp;
import org.elasticsearch.action.admin.indices.exists.indices.IndicesExistsRequest;
import org.elasticsearch.action.admin.indices.exists.indices.IndicesExistsResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.update.UpdateRequest;
import org.elasticsearch.client.Client;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.json.JsonXContent;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import info.bunji.mongodb.synces.SyncConfig;

/**
 * @author Fumiharu Kinoshita
 *
 */
public class EsUtils {

	private static final Logger logger = LoggerFactory.getLogger(EsUtils.class);

//	private static final String UPDATE_STATUS_CNT = "{ \"status\": \"%s\", \"indexCnt\": %d, \"lastOpTime\": { \"seconds\": %d, \"inc\": %d } }";
//
//	private static final String UPDATE_STATUS = "{ \"status\": \"%s\", \"lastOpTime\": { \"seconds\": %d, \"inc\": %d } }";
//
//	private static final String UPDATE_TS_ONLY = "{ \"lastOpTime\": { \"seconds\": %d, \"inc\": %d } }";
//
//	private static final String UPDATE_STATUS_ONLY = "{ \"status\": \"%s\" }";

	private EsUtils() {
		// do nothing.
	}

	/**
	 * インデックスの有無を返す.
	 * @param esClient
	 * @param indexName インデックス名
	 * @return 存在する場合はtrue、そうでない場合はfalseを返す
	 */
	public static boolean isExistsIndex(Client esClient, String indexName) {
		IndicesExistsResponse res = esClient.admin().indices().exists(new IndicesExistsRequest(indexName)).actionGet();
		return res.isExists();
	}

	/**
	 ********************************************
	 * ドキュメント登録・更新用のリクエストを生成する.
	 * @param op
	 * @return
	 ********************************************
	 */
	private UpdateRequest makeIndexRequest(String index, String type, String id, String json) {
		IndexRequest insert = new IndexRequest(index)
								.type(type)
								.id(id)
								.source(json);
		UpdateRequest update = new UpdateRequest(insert.index(), insert.type(), insert.id())
								.doc(json)
								.upsert(insert);
		return update;
	}

	/**
	 *
	 * @param status
	 * @param indexCnt
	 * @param ts
	 * @return
	 */
	public static String makeStatusJson(String status, Long indexCnt, BsonTimestamp ts) {
		String json = null;
		try {
			XContentBuilder builder = JsonXContent.contentBuilder();
			builder.startObject();
			if (!StringUtils.isEmpty(status)) {
				builder.field("status", status);
			}
			if (ts != null) {
				builder.startObject("lastOpTime")
						.field("seconds", ts.getTime())
						.field("inc", ts.getInc())
						.endObject();
			}
			builder.field("indexCnt", indexCnt);
			builder.endObject();
			json = builder.string();
		} catch (IOException ioe) {
			logger.error(ioe.getMessage());
		}
		return json;
	}

	/**
	 * ステータス更新用のリクエストを生成する.
	 * @param config
	 * @param status
	 * @param ts
	 * @return
	 */
	public static UpdateRequest makeStatusRequest(SyncConfig config, String status, BsonTimestamp ts) {
		UpdateRequest update = null;

		String json = makeStatusJson(status, config.getSyncCount(), ts);
		if (json != null) {

			IndexRequest insert = new IndexRequest(SyncConfig.STATUS_INDEX)
					.type("status")
					.id(config.getSyncName())
					.source(json);
			update = new UpdateRequest(insert.index(), insert.type(), insert.id())
					.doc(json)
					.upsert(insert);

			return update;
		}
		return update;
	}
}
