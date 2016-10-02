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

import org.bson.BsonTimestamp;
import org.bson.Document;
import org.bson.types.BSONTimestamp;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.gson.Gson;
import com.mongodb.CursorType;
import com.mongodb.MongoClient;
import com.mongodb.MongoInterruptedException;
import com.mongodb.MongoSocketException;
import com.mongodb.client.FindIterable;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.model.Filters;

import info.bunji.asyncutil.AsyncProcess;

/**
 ************************************************
 * Mongodbのoplogデータ取得処理.
 *
 * @author Fumiharu Kinoshita
 ************************************************
 */
public class OplogExtractor extends AsyncProcess<MongoOperation> {

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
	OplogExtractor(SyncConfig config, BsonTimestamp ts) {
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

		Gson gson = new Gson();

		Set<String> includeFields = config.getIncludeFields();
		Set<String> excludeFields = config.getExcludeFields();
		String index = config.getIndexName();

// test error
//config.getMongoConnection().getServerList().get(0).setPort(27118);

		// 初期インポート処理
int retryCnt = 0;
while (true) {
		try (MongoClient client = MongoClientService.getClient(config)) {
			retryCnt = 0;

			logger.info("■ start oplog sync.");

			// oplogを継続的に取得
			MongoCollection<Document> oplogCollection = client.getDatabase("local").getCollection("oplog.rs");
			MongoDatabase targetDb = client.getDatabase(config.getMongoDbName());
			FindIterable<Document> results = oplogCollection
							.find(Filters.gte("ts", timestamp))
							.cursorType(CursorType.TailableAwait)
							.noCursorTimeout(true)
							.oplogReplay(true);

			// get document from oplog
			for (Document doc : results) {

				// 同期対象のコレクションかチェックする
				String collection = getCollectionName(doc);
				if (!config.isTargetCollection(collection)) {
					continue;
				}

				Operation operation = Operation.valueOf(doc.get("op"));

				// TODO:ここでドキュメント内の項目のフィルタが必要
//				Document filteredDoc = DocumentUtils.applyFieldFilter(doc, includeFields, excludeFields);
				if (operation == Operation.INSERT) {
					append(new MongoOperation(operation, index, collection, (Document) doc.get("o"), doc.get("ts")));
				} else if (operation == Operation.DELETE) {
					append(new MongoOperation(operation, index, collection, (Document) doc.get("o"), doc.get("ts")));
				} else if (operation == Operation.UPDATE) {
					String namespace = getCollectionName(doc);
					MongoCollection<Document> extractCollection = targetDb.getCollection(namespace);

					// update時は差分データとなるのでidからドキュメントを取得する
					Document updateDoc = extractCollection.find((Document) doc.get("o2")).first();
					if (null != updateDoc) {
						//logger.trace(updateDoc.toString());
						append(new MongoOperation(operation, index, namespace, updateDoc, doc.get("ts")));
					}
				} else if (operation == Operation.DROP_COLLECTION) {
					// type(コレクション)のデータを全件削除
					//logger.debug("drop collection [" + collection + "]");
					append(new MongoOperation(Operation.DROP_COLLECTION, index, collection, null, null));
				} else {
					// 未対応の処理
					logger.debug("unsupported Operation [" + doc + "]");
				}
			}
		} catch (MongoInterruptedException mie) {
			// interrupt oplog tailable process.
			break;
		} catch (MongoSocketException mse) {
			retryCnt++;
			if (retryCnt >= 10) {
				logger.error("mongo connect failed. (cnt=" + retryCnt  + ")", mse);
				throw mse;
			}
			logger.warn("mongo connect retry. (cnt=" + retryCnt  + ")");
		} catch (Throwable t) {
			logger.error(t.getMessage(), t);
			throw t;
		}
}

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
				// TODO 別の箇所でやるべき?
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
		//super.postProcess();
		logger.info("extract oplog stopped. [" + config.getSyncName() + "]");
	}
}