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

import java.util.Set;

import org.bson.BasicBSONObject;
import org.bson.BsonTimestamp;
import org.bson.Document;
import org.bson.types.BSONTimestamp;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.ImmutableList;
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
public class CollectionExtractor extends AsyncProcess<SyncOperation> {

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
	public CollectionExtractor(SyncConfig config, BsonTimestamp ts) {
		this.config = config;
		if (ts != null) {
			this.timestamp = new BSONTimestamp(ts.getTime(), ts.getInc());
		}
	}

	/*
	 * {@inheridDoc}
	 */
	@Override
	protected void execute() throws Exception {

		Set<String> includeFields = config.getIncludeFields();
		Set<String> excludeFields = config.getExcludeFields();
		String index = config.getIndexName();
		String syncName = config.getSyncName();

		// 初期インポート処理
		try (MongoClient client = MongoClientService.getClient(config)) {
			if (timestamp == null) {
				logger.info("[{}] start initial import from db [{}]", syncName, config.getMongoDbName());

				// 処理開始時点のoplogの最終タイムスタンプを取得しておく
				MongoCollection<Document> oplog  = client.getDatabase("local").getCollection("oplog.rs");
				Document lastOp = oplog.find().sort(new BasicDBObject("$natural", -1)).limit(1).first();
				BsonTimestamp lastOpTs = lastOp.get("ts", BsonTimestamp.class);

				logger.debug("[{}] current oplog timestamp = [{}]", syncName, lastOpTs.toString());

				// 同期対象コレクション名の一覧を取得する
				MongoDatabase db = client.getDatabase(config.getMongoDbName());

				// update status
				Document statusDoc = DocumentUtils.makeStatusDocument(Status.INITIAL_IMPORTING, null, null);
				append(new SyncOperation(Operation.INSERT, "status", statusDoc, config.getSyncName()));

				// コレクション毎に初期同期を行う
				Object lastId = null;
				for (String collection : getTargetColectionList(db)) {
					logger.info("[{}] start initial import. [{}]", syncName, collection);

					MongoCollection<Document> conn = db.getCollection(collection);
					BasicDBObject filter = getFilterForInitialImport(new BasicDBObject(), lastId);

					long count = conn.count(filter);

					// get document from collection
					long processed = 0;
					FindIterable<Document> results = conn.find(filter).sort(new BasicDBObject("_id", 1));
					for (Document doc : results) {
						Document filteredDoc = DocumentUtils.applyFieldFilter(doc, includeFields, excludeFields);
						append(new SyncOperation(Operation.INSERT, index, collection, filteredDoc, null));
						if ((++processed % LOGGING_INTERVAL) == 0) {
							logger.info("[{}] processing initial import. [{}({}/{})]", syncName, collection, processed, count);
						}
					}
					logger.info("[{}] initial import finished. [{}(total:{})]", syncName, collection, processed);
				}

				// update status
				statusDoc = DocumentUtils.makeStatusDocument(Status.RUNNING, null, lastOpTs);
				append(new SyncOperation(Operation.UPDATE, "status", statusDoc, config.getSyncName()));
			}
			logger.info("[{}] extract collection finished.", syncName);
		} catch (Throwable t) {
			config.setStatus(Status.INITIAL_IMPORT_FAILED);
			logger.error("[{}] initial import failed.({})", syncName, t.getMessage(), t);
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

	/*
	 **********************************
	 * {@inheridDoc}
	 **********************************
	 */
	@Override
	protected void postProcess() {
		super.postProcess();
		//logger.info("[{}] extract collection finished.", config.getSyncName());
	}
}
