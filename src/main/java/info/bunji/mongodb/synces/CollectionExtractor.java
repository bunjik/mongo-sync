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

import java.util.Map;
import java.util.Set;

import org.bson.BasicBSONObject;
import org.bson.BsonTimestamp;
import org.bson.Document;
import org.bson.types.BSONTimestamp;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.ImmutableList;
import com.google.gson.Gson;
import com.mongodb.BasicDBObject;
import com.mongodb.MongoClient;
import com.mongodb.QueryOperators;
import com.mongodb.client.FindIterable;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoCursor;
import com.mongodb.client.MongoDatabase;

import info.bunji.asyncutil.AsyncProcess;
import info.bunji.mongodb.synces.util.DocumentUtils;

/**
 ************************************************
 * Mongodbのデータ取得処理.
 *
 * @author Fumiharu Kinoshita
 ************************************************
 */
public class CollectionExtractor extends AsyncProcess<MongoOperation> {

	private Logger logger = LoggerFactory.getLogger(getClass());

	private SyncConfig config;
	private BSONTimestamp timestamp;

	private static final long LOGGING_INTERVAL = 5000;

	/**
	 ********************************************
	 *
	 * @param client
	 * @param ts 同期開始タイムスタンプ
	 * @param config 同期設定
	 ********************************************
	 */
	CollectionExtractor(SyncConfig config, BsonTimestamp ts) {
		this.config = config;
		if (ts != null) {
			this.timestamp = new BSONTimestamp(ts.getTime(), ts.getInc());
//		} else {
//			this.timestamp = new BSONTimestamp(0, 0);
		}
	}

	/*
	 * {@inheridDoc}
	 */
	@Override
	protected void execute() throws Exception {

		Gson gson = new Gson();

		Set<String> includeFields = config.getIncludeFields();
		Set<String> excludeFields = config.getExcludeFields();
		String index = config.getIndexName();

// test error
//config.getMongoConnection().getServerList().get(0).setPort(27118);

		// 初期インポート処理
		try (MongoClient client = MongoClientService.getClient(config)) {
			if (timestamp == null) {
				logger.info("■ start initial import from db [" + config.getMongoDbName() + "]");

				// update status
				String json = String.format("{ status: \"%s\", lastOpTime: null }", Status.INITIAL_IMPORTING);
				Document statusDoc = new Document(gson.fromJson(json, Map.class));
				append(new MongoOperation(Operation.UPDATE, "status", statusDoc, config.getSyncName()));

				// 処理開始時点のoplogの最終タイムスタンプを取得しておく
				MongoCollection<Document> oplog  = client.getDatabase("local").getCollection("oplog.rs");
				Document lastOp = oplog.find().sort(new BasicDBObject("$natural", -1)).limit(1).first();
				BsonTimestamp lastOpTs = lastOp.get("ts", BsonTimestamp.class);
				timestamp = new BSONTimestamp(lastOpTs.getTime(), lastOpTs.getInc());

				// 同期対象コレクション名の一覧を取得する
				MongoDatabase db = client.getDatabase(config.getMongoDbName());
				Set<String> collectionSet = getTargetColectionList(db);

				// コレクション毎に初期同期を行う
				Object lastId = null;
				for (String collection : collectionSet) {
					logger.info("start initial import. [" + collection + "]");

					MongoCollection<Document> conn = db.getCollection(collection);
					BasicDBObject filter = getFilterForInitialImport(new BasicDBObject(), lastId);

					long count = conn.count(filter);

					// get document from collection
					long processed = 0;
					FindIterable<Document> results = conn.find(filter).sort(new BasicDBObject("_id", 1));
					for (Document doc : results) {
						// TODO:ここでドキュメント内の項目のフィルタ + $REFの参照？が必要
						Document filteredDoc = DocumentUtils.applyFieldFilter(doc, includeFields, excludeFields);
						append(new MongoOperation(Operation.INSERT, index, collection, filteredDoc, null));
						lastId = doc.get("_id");
						if ((++processed % LOGGING_INTERVAL) == 0) {
							logger.info(String.format("processing initial import. [%s(%d/%d)]", collection, processed, count));
						}
					}
					logger.info(String.format("initial import finished. [%s(total:%d)]", collection, processed));
				}

				// update status
				json = String.format("{ status: 'RUNNING', lastOpTime : { seconds: %d, inc: %d } }", timestamp.getTime(), timestamp.getInc());
				statusDoc = new Document(gson.fromJson(json, Map.class));
				append(new MongoOperation(Operation.UPDATE, "status", statusDoc, config.getSyncName()));
			}
		} catch (Throwable t) {
			config.setStatus(Status.INITIAL_IMPORT_FAILED);
			throw t;
		}
	}

	/**
	 **********************************
	 *
	 * @param filter
	 * @param id
	 * @return
	 **********************************
	 */
	private BasicDBObject getFilterForInitialImport(BasicDBObject filter, Object id) {
		if (id == null) {
			return filter;
		}
		BasicDBObject idFilter = new BasicDBObject("_id", new BasicBSONObject(QueryOperators.GT, id));
		if (filter == null || filter.equals(new BasicDBObject())) {
			return idFilter;
		}
		return new BasicDBObject(QueryOperators.AND, ImmutableList.of(filter, idFilter));
	}

	/**
	 **********************************
	 * 同期対象のコレクション名一覧を取得する.
	 * <br>
	 * 指定がない場合は、全コレクションを対象とする。
	 * @return 同期対象コレクション名の一覧
	 **********************************
	 */
	private Set<String> getTargetColectionList(MongoDatabase db) {
		Set<String> collectionSet = config.getImportCollections();
		if (collectionSet.isEmpty()) {
			// 指定がない場合は、全コレクション対象(同期開始時点のコレクションが対象)
			MongoCursor<String> it = db.listCollectionNames().iterator();
			while (it.hasNext()) {
				String name = it.next();
				if (!name.startsWith("system.")) {
					collectionSet.add(name);
				}
			}
		}
		return collectionSet;
	}

	/**
	 **********************************
	 *
	 * @param oplogDoc
	 * @return
	 **********************************
	 */
	private String getCollectionName(Document oplogDoc) {
		// 対象のDB名以降の文字列が対象
		// 暫定で最初のピリオド以降
		String namespace = oplogDoc.getString("ns");
		String collection = namespace.substring(namespace.indexOf(".") + 1);
		if (collection.equals("$cmd")) {
			Document op = oplogDoc.get("o", Document.class);
			collection = op.getString("drop");

			if (collection != null) {
//				logger.debug("drop collection [" + collection + "]");
				// TODO 別の箇所でやるべき
				oplogDoc.put("op", Operation.DROP_COLLECTION.getValue());
			}
		}
		return collection;
	}

	/*
	 **********************************
	 * {@inheridDoc}
	 **********************************
	 */
	@Override
	protected void postProcess() {
		super.postProcess();
		logger.info("extract collection finished. [" + config.getSyncName() + "]");
	}
}