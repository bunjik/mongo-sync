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
import java.util.HashMap;
import java.util.Map;
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
	private final Map<String,MongoCollection<Document>> cachedCollection = new HashMap<>();

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
	 ********************************************
	 * {@inheridDoc}
	 ********************************************
	 */
	@Override
	protected void execute() throws Exception {

		Set<String> includeFields = config.getIncludeFields();
		Set<String> excludeFields = config.getExcludeFields();
		String index = config.getDestDbName();
		String syncName = config.getSyncName();

int checkPoint = 0;

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
						throw new IllegalStateException("[" + syncName + "] oplog outdated.["
										+ DocumentUtils.toDateStr(timestamp) + "(" +  timestamp + ")]");
					}
					//logger.trace("[{}] start oplog timestamp = [{}]", config.getSyncName(), timestamp);
					config.addSyncCount(-1);	// 同期開始時に最終同期データを再度同期するため１減算しておく

					BsonTimestamp tmpTs = results.first().get("ts", BsonTimestamp.class);
					if (!tmpTs.equals(timestamp)) {
						// 一致しない場合、mongoのデータが過去に戻っている可能性があるため
						// 取得できた最新のタイムスタンプから同期を再開する
						timestamp = tmpTs;
					}
					config.setStatus(Status.RUNNING);
					config.setLastOpTime(timestamp);
					//append(DocumentUtils.makeStatusOperation(config));
					//append(DocumentUtils.makeStatusOperation(Status.RUNNING, config, timestamp));
					append(SyncOperation.fromConfig(config));
				}

				// oplogを継続的に取得
				targetDb = client.getDatabase(config.getMongoDbName());
				results = oplogCollection
								.find()
								.filter(Filters.gte("ts", timestamp))
								.sort(new Document("$natural", 1))
								.cursorType(CursorType.TailableAwait)
								.noCursorTimeout(true)
								.oplogReplay(true);

				logger.info("[{}] started oplog sync. [oplog {} ({})]", syncName,
											DocumentUtils.toDateStr(timestamp), timestamp);

				// get document from oplog
				for (Document oplog : results) {
					
					SyncOperation op = new SyncOperation(oplog, index);

					timestamp = op.getTimestamp();

					// check target database and collection
					if(!config.getMongoDbName().equals(op.getSrcDbName()) || !config.isTargetCollection(op.getCollection())) {
						if (++checkPoint >= 10000) {
							// 無更新が一定回数継続したらステータスの最終同期時刻のみ更新する
							config.setLastOpTime(timestamp);
							//config.setLastSyncTime(timestamp);
							//op = DocumentUtils.makeStatusOperation(config);
							op = SyncOperation.fromConfig(config);
							checkPoint = 0;		// clear check count
						} else {
							continue;
						}
					} else {
						checkPoint = 0;
					}

					if (op.isPartialUpdate()) {
						// get full document
						MongoCollection<Document> collection = getMongoCollection(op.getCollection());
						Document updateDoc = collection.find(oplog.get("o2", Document.class)).first();
						if (updateDoc == null) {
							continue;	// skip update 
						}
						op.setDoc(updateDoc);
					}

					// filter document(insert or update)
					if (op.getDoc() != null) {
						Document filteredDoc = DocumentUtils.applyFieldFilter(op.getDoc(), includeFields, excludeFields);
						if (filteredDoc.isEmpty()) {
							continue;	// no change sync fields
						}
						op.setDoc(filteredDoc);
					}

					// emit sync data
					append(op);
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
	 ********************************************
	 * 
	 * @param namespace
	 * @return
	 ********************************************
	 */
	private MongoCollection<Document> getMongoCollection(String namespace) {
		MongoCollection<Document> collection = cachedCollection.get(namespace);
		if (collection == null) {
			collection = targetDb.getCollection(namespace);
			cachedCollection.put(namespace, collection);
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
