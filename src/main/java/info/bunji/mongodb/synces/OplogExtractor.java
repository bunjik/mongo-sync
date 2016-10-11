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

import com.mongodb.CursorType;
import com.mongodb.MongoClient;
import com.mongodb.MongoInterruptedException;
import com.mongodb.MongoSocketException;
import com.mongodb.client.FindIterable;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.model.Filters;

import info.bunji.asyncutil.AsyncProcess;
import info.bunji.mongodb.synces.util.DocumentUtils;

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
	private MongoDatabase targetDb = null;

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

		Set<String> includeFields = config.getIncludeFields();
		Set<String> excludeFields = config.getExcludeFields();
		String index = config.getIndexName();

		// oplogからの取得処理
		int retryCnt = 0;
		while (true) {
			try (MongoClient client = MongoClientService.getClient(config)) {
				retryCnt = 0;

				logger.debug("[" + config.getSyncName() + "] start oplog sync.");

				// oplogを継続的に取得
				MongoCollection<Document> oplogCollection = client.getDatabase("local").getCollection("oplog.rs");
				targetDb = client.getDatabase(config.getMongoDbName());
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

					BsonTimestamp ts = doc.get("ts", BsonTimestamp.class);
					if (operation == Operation.INSERT) {
						Document filteredDoc = DocumentUtils.applyFieldFilter(doc.get("o", Document.class), includeFields, excludeFields);
						append(new MongoOperation(operation, index, collection, filteredDoc, ts));
					} else if (operation == Operation.DELETE) {
						append(new MongoOperation(operation, index, collection, doc.get("o", Document.class), ts));
					} else if (operation == Operation.UPDATE) {
						// update時は差分データとなるのでidからドキュメントを取得する
						String namespace = getCollectionName(doc);
						MongoCollection<Document> extractCollection = targetDb.getCollection(namespace);
						Document updateDoc = extractCollection.find(doc.get("o2", Document.class)).first();
						if (null != updateDoc) {
							Document filteredDoc = DocumentUtils.applyFieldFilter(updateDoc, includeFields, excludeFields);
							append(new MongoOperation(operation, index, namespace, filteredDoc, ts));
						}

					} else if (operation == Operation.DROP_COLLECTION) {
						// type(コレクション)のデータを全件削除
						//logger.debug("drop collection [" + collection + "]");
						append(new MongoOperation(Operation.DROP_COLLECTION, index, collection, null, null));
					} else {
						// 未対応の処理
						logger.debug("unsupported Operation [{}]", operation);
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


	private Document getUpdateDocument(String collection, Document idDoc) {
		String namespace = getCollectionName(idDoc);
		MongoCollection<Document> extractCollection = targetDb.getCollection(collection);
		return extractCollection.find((Document) idDoc.get("o2")).first();
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
		if (!namespace.startsWith(config.getMongoDbName())) {
			return null;
		}
		String collection = namespace.substring(namespace.indexOf(".") + 1);
		if (collection.equals("$cmd")) {
			Document op = oplogDoc.get("o", Document.class);
			collection = op.getString("drop");

			if (collection != null) {
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