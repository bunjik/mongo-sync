package info.bunji.mongodb.synces.util;

import java.io.IOException;

import org.bson.BsonTimestamp;
import org.elasticsearch.action.admin.indices.exists.indices.IndicesExistsRequest;
import org.elasticsearch.action.admin.indices.refresh.RefreshRequest;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.update.UpdateRequest;
import org.elasticsearch.client.Client;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.json.JsonXContent;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import info.bunji.mongodb.synces.Status;
import info.bunji.mongodb.synces.SyncConfig;

/**
 ************************************************
 *
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
	 * インデックスの有無を返す.
	 * @param esClient
	 * @param indexName インデックス名
	 * @return 存在する場合はtrue、そうでない場合はfalseを返す
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
	 *
	 * @param esClient
	 * @param indexName
	 * @return
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
	 * ドキュメント登録・更新用のリクエストを生成する.
	 * @param op
	 * @return
	 ********************************************
	 */
	public static UpdateRequest makeIndexRequest(String index, String type, String id, String json) {
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
	 ********************************************
	 * ステータス更新用のリクエストを生成する.
	 * @param config
	 * @param status
	 * @param ts
	 * @return
	 ********************************************
	 */
	public static UpdateRequest makeStatusRequest(SyncConfig config, Status status, BsonTimestamp ts) {
		XContentBuilder content = makeStatusContent(status, config.getSyncCount(), ts);
		IndexRequest insert = new IndexRequest(SyncConfig.STATUS_INDEX)
				.type("status")
				.id(config.getSyncName())
				.source(content);
		return new UpdateRequest(insert.index(), insert.type(), insert.id())
				.doc(content)
				.upsert(insert);
	}

	/**
	 ********************************************
	 * ステータス更新用のJSON文字列を生成する.
	 * @param status
	 * @param indexCnt
	 * @param ts
	 * @return
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
	 *
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
