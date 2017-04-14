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

import java.net.UnknownHostException;
import java.util.Set;

import org.bson.BsonTimestamp;
import org.bson.Document;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.mongodb.CursorType;
import com.mongodb.MongoClient;
import com.mongodb.MongoClientException;
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
 * oplog tailable process.
 * @author Fumiharu Kinoshita
 ************************************************
 */
public class OplogExtractor extends AsyncProcess<SyncOperation> {

	private Logger logger = LoggerFactory.getLogger(getClass());

	private SyncConfig config;
	private BsonTimestamp timestamp;
	private MongoDatabase targetDb = null;

	public static final int MAX_RETRY = 20;

	/**
	 ********************************************
	 * @param ts 同期開始タイムスタンプ
	 * @param config 同期設定
	 ********************************************
	 */
	public OplogExtractor(SyncConfig config, BsonTimestamp ts) {
		this.config = config;
		if (ts != null) {
			this.timestamp = new BsonTimestamp(ts.getTime(), ts.getInc());
		}
	}

	/*
	 * {@inheridDoc}
	 */
	@Override
	protected void execute() throws Exception {

		Set<String> includeFields = config.getIncludeFields();
		Set<String> excludeFields = config.getExcludeFields();
		String index = config.getDestDbName();
		String syncName = config.getSyncName();

		// oplogからの取得処理
		int retryCnt = 0;
		while (true) {
			try (MongoClient client = MongoClientService.getClient(config)) {
				retryCnt = 0;

				logger.info("[{}] starting oplog sync.", syncName);

				// check oplog timestamp outdated
				MongoCollection<Document> oplogCollection = client.getDatabase("local").getCollection("oplog.rs");
				FindIterable<Document> results;
				if (timestamp != null) {
					results = oplogCollection
							.find()
							.filter(Filters.lte("ts", timestamp))
							.sort(new Document("$natural", -1))
							.limit(1);
					if (results.first() == null) {
						throw new IllegalStateException("[" + syncName + "] oplog outdated.[" + timestamp + "]");
					}
					//logger.trace("[{}] start oplog timestamp = [{}]", config.getSyncName(), timestamp);
					config.addSyncCount(-1);	// 同期開始時に最終同期データを再度同期するため１減算しておく

					BsonTimestamp tmpTs = results.first().get("ts", BsonTimestamp.class);
					if (!tmpTs.equals(timestamp)) {
						// 一致しない場合、mongoのデータが過去に戻っている可能性があるため
						// 取得できた最新のタイムスタンプから同期を再開する
						timestamp = tmpTs;
						config.setLastOpTime(timestamp);
						Document statusDoc = DocumentUtils.makeStatusDocument(Status.RUNNING, null, timestamp);
						append(new SyncOperation(Operation.UPDATE, "status", statusDoc, config.getSyncName()));
					}
				}

				logger.info("[{}] start oplog sync. [oplog timestamp:{}]", syncName, timestamp);

				// oplogを継続的に取得
				targetDb = client.getDatabase(config.getMongoDbName());
				results = oplogCollection
								.find()
								.filter(Filters.gte("ts", timestamp))
								.sort(new Document("$natural", 1))
								.cursorType(CursorType.TailableAwait)
								.noCursorTimeout(true)
								.oplogReplay(true);

				// get document from oplog
				for (Document doc : results) {

					// check sync collection
					String collection = getCollectionName(doc);
					if (!config.isTargetCollection(collection)) {
						continue;
					}

					Operation operation = Operation.valueOf(doc.get("op"));

					timestamp = doc.get("ts", BsonTimestamp.class);
					if (operation == Operation.INSERT) {
						Document filteredDoc = DocumentUtils.applyFieldFilter(doc.get("o", Document.class), includeFields, excludeFields);
						append(new SyncOperation(operation, index, collection, filteredDoc, timestamp));
					} else if (operation == Operation.DELETE) {
						append(new SyncOperation(operation, index, collection, doc.get("o", Document.class), timestamp));
					} else if (operation == Operation.UPDATE) {
						// update時は差分データとなるのでidでドキュメントを取得する
						String namespace = getCollectionName(doc);
						MongoCollection<Document> extractCollection = targetDb.getCollection(namespace);
						Document updateDoc = extractCollection.find(doc.get("o2", Document.class)).first();
						if (null != updateDoc) {
							Document filteredDoc = DocumentUtils.applyFieldFilter(updateDoc, includeFields, excludeFields);
							append(new SyncOperation(operation, index, namespace, filteredDoc, timestamp));
						}
					} else if (operation == Operation.DROP_COLLECTION) {
						// type(コレクション)のデータを全件削除
						//logger.debug("drop collection [" + collection + "]");
						append(new SyncOperation(Operation.DROP_COLLECTION, index, collection, null, null));
					} else {
						// 未対応の処理
						logger.debug("unsupported Operation [{}]", operation);
					}
				}
			} catch (MongoClientException mce) {
				// do nothing.
			} catch (UnknownHostException | MongoSocketException mse) {
				retryCnt++;
				if (retryCnt >= MAX_RETRY) {
					logger.error(String.format("[%s] mongo connect failed. (RETRY=%d)", syncName, retryCnt), mse);
					throw mse;
				}
				long waitSec = (long) Math.min(60, Math.pow(2, retryCnt));
				logger.warn("[{}] waiting mongo connect retry. ({}/{}) [{}sec]", syncName, retryCnt, MAX_RETRY, waitSec);

				Thread.sleep(waitSec * 1000);
			} catch (MongoInterruptedException mie) {
				// interrupt oplog tailable process.
				break;
			} catch (Throwable t) {
				logger.error(String.format("[%s] error. [msg:%s](%s)", syncName, t.getMessage(), t.getClass().getSimpleName()), t);
				throw t;
			}
		}
	}

	/**
	 **********************************
	 * get collection name from oplog.
	 * @param oplogDoc
	 * @return
	 **********************************
	 */
	private String getCollectionName(Document oplogDoc) {
		// 対象のDB名以降の文字列が対象
		// 暫定で最初のピリオド以降
		String namespace = oplogDoc.getString("ns");
		if (!namespace.startsWith(config.getMongoDbName()+".")) {
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
		super.postProcess();
		logger.info("[{}] oplog sync stopped.", config.getSyncName());
	}
}
