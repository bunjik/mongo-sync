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

import java.io.IOException;
import java.util.EventListener;

import org.bson.BsonTimestamp;
import org.elasticsearch.action.bulk.BulkItemResponse;
import org.elasticsearch.action.bulk.BulkProcessor;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.delete.DeleteRequest;
import org.elasticsearch.action.update.UpdateRequest;
import org.elasticsearch.client.Client;
import org.elasticsearch.common.unit.ByteSizeUnit;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.common.unit.TimeValue;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import info.bunji.asyncutil.AsyncExecutor;
import info.bunji.asyncutil.AsyncProcess;
import info.bunji.asyncutil.AsyncResult;
import info.bunji.mongodb.synces.util.EsUtils;

/**
 ************************************************
 * elasticsearchにデータを同期するクラス.
 *
 * 同期設定単位にインスタンスされる
 *
 * @author Fumiharu Kinoshita
 ************************************************
 */
public class IndexerProcess extends AsyncProcess<Boolean>
									implements StatusChangeListener, BulkProcessor.Listener {

	private Logger logger = LoggerFactory.getLogger(getClass());

	private final Client esClient;
	private final SyncConfig config;
	private final String indexName;
	private AsyncResult<SyncOperation> operations;
	private final Listener listener;

	private BsonTimestamp oplogTs;

	private static final int DEFAULT_BUlK_ACTIONS = 5000;

	private static final long DEFAULT_BUlK_INTERVAL = 500;	// ms

	private static final long DEFAULT_BULK_SIZE = 64; // MB

	/**
	 **********************************
	 * @param esClient
	 * @param config
	 * @param operations
	 **********************************
	 */
	IndexerProcess(Client esClient, SyncConfig config, Listener listener, AsyncResult<SyncOperation> operations) {
		this.esClient = esClient;
		this.config = config;
		this.operations = operations;
		this.indexName = config.getIndexName();
		this.listener = listener;
	}

	/**
	 **********************************
	 *
	 **********************************
	 */
	@Override
	public void execute() {
		String syncName = config.getSyncName();

		while (true) {
			logger.info("[{}] start sync.", syncName);
			BulkProcessor processor = getBulkProcessor();
			try {
				for (SyncOperation op : operations) {
					//同期対象チェック
					if (SyncConfig.STATUS_INDEX.equals(op.getIndex())) {
						processor.add(makeIndexRequest(op));
						continue;
					} else if (!config.isTargetCollection(op.getCollection())) {
						continue;	// 同期対象外
					}

					// monodbの操作日時を取得
					oplogTs = op.getTimestamp();

					switch (op.getOp()) {

					// ドキュメント追加/更新時
					case INSERT:
					case UPDATE:
						config.addSyncCount();
						processor.add(makeIndexRequest(op));
						break;

					// ドキュメント削除時
					case DELETE:
						config.addSyncCount();
						processor.add(makeDeleteRequest(op));
						break;

						// コレクション削除
					case DROP_COLLECTION:
						// es2.x はtypeの削除が不可となったため、1件づつデータを削除する
						// 削除処理はoplogとの不整合を防ぐため、同期で実行する
						logger.info(op.getOp() + " index:" + indexName + " type:" + op.getCollection());
						AsyncExecutor.execute(new EsTypeDeleter(esClient, indexName, op.getCollection())).block();
						logger.debug("[{}] type deleted.[{}]", syncName, op.getCollection());

						// ステータス更新用のリクエストを追加する
						processor.add(EsUtils.makeStatusRequest(config, null, oplogTs));
						break;

					// データベース削除
					case DROP_DATABASE:
						logger.debug("[{}] not implemented yet. [{}]", syncName, op.getOp());
						break;

					// 未対応もしくは不明な操作
					default:
						logger.warn("[{}] unsupported operation. [{}/{}]", syncName, op.getCollection(), op.getOp());
						break;
					}
				}

				// stop indexer.
				processor.add(EsUtils.makeStatusRequest(config, Status.STOPPED, null));
				logger.info("[{}] indexer stopped.", syncName);
				break;

			} catch (IllegalArgumentException | IllegalStateException e) {
				logger.warn(String.format("[{}] indexer bulkProcess error.(%s)", syncName, e.getMessage()), e);
			} catch(Throwable t) {
				processor.add(EsUtils.makeStatusRequest(config, Status.STOPPED, null));
				logger.error(String.format("[{}] indexer stopped with error.(%s)", syncName, t.getMessage()), t);
				throw t;
			} finally {
				processor.flush();
				processor.close();
				logger.info("[{}] stop sync.", syncName);
			}
		}
	}

	public SyncConfig getConfig() {
		return config;
	}

	/**
	 ********************************************
	 * ドキュメント登録・更新用のリクエストを生成する.
	 * @param op
	 * @return
	 ********************************************
	 */
	private UpdateRequest makeIndexRequest(SyncOperation op) {
		return EsUtils.makeIndexRequest(op.getIndex(), op.getCollection(), op.getId(), op.getJson());
	}

	/**
	 ********************************************
	 * ドキュメント削除用のリクエストを生成する
	 * @param op
	 * @return
	 ********************************************
	 */
	private DeleteRequest makeDeleteRequest(SyncOperation op) {
		return new DeleteRequest(op.getIndex())
								.type(op.getCollection())
								.id(op.getId());
	}

	/**
	 ********************************************
	 * elasticsearchへのバルク処理を行うProcessorのインスタンスを取得する.
	 * <br>
	 * 取得したProcessorに対して更新データを追加していくだけで、追加されたデータが
	 * いずれかの条件（経過時間、格納件数、合計サイズ）を満たすと自動的にバルク処理が実行される。
	 * @return BulkProcessorのインスタンス。
	 ********************************************
	 */
	private BulkProcessor getBulkProcessor() {
		return BulkProcessor.builder(esClient, this)
							.setBulkActions(DEFAULT_BUlK_ACTIONS)
							.setBulkSize(new ByteSizeValue(DEFAULT_BULK_SIZE, ByteSizeUnit.MB))
							.setFlushInterval(TimeValue.timeValueMillis(DEFAULT_BUlK_INTERVAL))
							.setConcurrentRequests(1)
							.build();
	}

	/**
	 ********************************************
	 * バルク処理の開始前に呼び出される.
	 * @see org.elasticsearch.action.bulk.BulkProcessor.Listener#beforeBulk(long, org.elasticsearch.action.bulk.BulkRequest)
	 ********************************************
	 */
	@Override
	public void beforeBulk(long executionId, BulkRequest request) {
		// ステータス更新用のリクエストを追加する
		request.add(EsUtils.makeStatusRequest(config, null, oplogTs));

		//logger.debug("call beforeBulk() size=" + request.numberOfActions() + " ts=" + curTs);
	}

	/**
	 ********************************************
	 * バルク処理の終了時(エラーあり)に呼び出される.
	 * @see org.elasticsearch.action.bulk.BulkProcessor.Listener#afterBulk(long, org.elasticsearch.action.bulk.BulkRequest, java.lang.Throwable)
	 ********************************************
	 */
	@Override
	public void afterBulk(long executionId, BulkRequest request, Throwable failure) {
		logger.error("call afterBulk() with failure. " + failure.getMessage(), failure);
	}

	/**
	 ********************************************
	 * バルク処理の終了時に呼び出される.
	 *
	 * @see org.elasticsearch.action.bulk.BulkProcessor.Listener#afterBulk(long, org.elasticsearch.action.bulk.BulkRequest, org.elasticsearch.action.bulk.BulkResponse)
	 ********************************************
	 */
	@Override
	public void afterBulk(long executionId, BulkRequest request, BulkResponse response) {
		logger.trace(String.format("[%s] call afterBulk() size=%d, [%d ms]",
								config.getSyncName(),
								response.getItems().length,
								response.getTookInMillis()));
		for (BulkItemResponse item : response) {
			if (item.isFailed()) {
				logger.error("[{}] index:[{}], type:[{}] id:[{}] msg:[{}]",
								item.getItemId(), item.getIndex(), item.getType(), item.getId(),
								item.getFailureMessage());
			}
		}
	}

	/*
	 ********************************************
	 * (非 Javadoc)
	 * @see info.bunji.mongodb.synces.StatusChangeListener#stop()
	 ********************************************
	 */
	@Override
	public void stop() {
		try {
			// データの取得処理を終了することで、Indexerを終了させる
			operations.close();
		} catch (IOException e) {
			logger.error(e.getMessage(), e);
		}
	}

	/**
	 ********************************************
	 * イベント通知用インターフェース
	 ********************************************
	 */
	static interface Listener extends EventListener {
		/**
		 ******************************
		 * indexerの停止時に呼び出されるメソッド.
		 * @param syncName 同期設定名
		 ******************************
		 */
		void onIndexerStop(String syncName);
	}
}
