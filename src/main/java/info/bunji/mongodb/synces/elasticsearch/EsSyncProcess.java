/**
 * 
 */
package info.bunji.mongodb.synces.elasticsearch;

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

import info.bunji.asyncutil.AsyncExecutor;
import info.bunji.asyncutil.AsyncResult;
import info.bunji.mongodb.synces.Status;
import info.bunji.mongodb.synces.StatusChecker;
import info.bunji.mongodb.synces.SyncConfig;
import info.bunji.mongodb.synces.SyncOperation;
import info.bunji.mongodb.synces.SyncProcess;

/**
 * Sync process for elasticearch.
 * @author Fumiharu Kinoshita
 */
public class EsSyncProcess extends SyncProcess implements BulkProcessor.Listener {

	private final Client esClient;
	private final String indexName;

	private BulkProcessor _processor = null;

	private static final int  DEFAULT_BUlK_ACTIONS = 3000;	// size
	private static final long DEFAULT_BUlK_INTERVAL = 500;	// ms
	private static final long DEFAULT_BULK_SIZE = 64; // MB

	/**
	 **********************************
	 * @param esClient
	 * @param config
	 * @param operations
	 **********************************
	 */
	public EsSyncProcess(Client esClient, SyncConfig config, StatusChecker listener, AsyncResult<SyncOperation> operations) {
		super(config, operations);
		this.esClient = esClient;
		this.indexName = config.getIndexName();
//		this.listener = listener;
	}

	/*
	 **********************************
	 * (non Javadoc)
	 * @see info.bunji.mongodb.synces.SyncProcess#onError(java.lang.Exception)
	 **********************************
	 */
	@Override
	protected void onError(Exception e) throws Exception {
		String syncName = getConfig().getSyncName();
		if (e instanceof IllegalArgumentException || e instanceof IllegalStateException) {
			logger.warn(String.format("[%s] bulkProcess error.(%s)", syncName, e.getMessage()), e);
		} else {
			logger.error(String.format("[%s] sync stopped with error.(%s)", syncName, e.getMessage()), e);
			throw e;
		}
	}

	/*
	 **********************************
	 * (non Javadoc)
	 * @see info.bunji.asyncutil.AsyncProcess#postProcess()
	 **********************************
	 */
	@Override
	protected void postProcess() {
		if (_processor != null) {
			_processor.flush();
			_processor.close();
		}
		// update status
		esClient.update(EsUtils.makeStatusRequest(getConfig(), Status.STOPPED, null)).actionGet();
		EsUtils.refreshIndex(esClient, EsStatusChecker.CONFIG_INDEX);

		super.postProcess();
	}

	/*
	 **********************************
	 * (non Javadoc)
	 * @see info.bunji.mongodb.synces.AbstractSyncProcess#doInsert(info.bunji.mongodb.synces.SyncOperation)
	 **********************************
	 */
	@Override
	public void doInsert(SyncOperation op) {
		// ステータス更新リクエストの場合は、無条件に更新
		if (EsStatusChecker.CONFIG_INDEX.equals(op.getIndex())) {
			getBulkProcessor().add(makeIndexRequest(op));
		} else if (getConfig().isTargetCollection(op.getCollection())) {
			//同期対象チェック
			getConfig().addSyncCount();
			getBulkProcessor().add(makeIndexRequest(op));
		}
	}

	/*
	 **********************************
	 * (non Javadoc)
	 * @see info.bunji.mongodb.synces.AbstractSyncProcess#doUpdate(info.bunji.mongodb.synces.SyncOperation)
	 **********************************
	 */
	@Override
	public void doUpdate(SyncOperation op) {
		doInsert(op);
	}

	/*
	 **********************************
	 * (non Javadoc)
	 * @see info.bunji.mongodb.synces.AbstractSyncProcess#doDelete(info.bunji.mongodb.synces.SyncOperation)
	 **********************************
	 */
	@Override
	public void doDelete(SyncOperation op) {
		//同期対象チェック
		if (getConfig().isTargetCollection(op.getCollection())) {
			getConfig().addSyncCount();
			getBulkProcessor().add(makeDeleteRequest(op));
		}
	}

	/*
	 **********************************
	 * (non Javadoc)
	 * @see info.bunji.mongodb.synces.AbstractSyncProcess#doDropCollection(info.bunji.mongodb.synces.SyncOperation)
	 **********************************
	 */
	@Override
	public void doDropCollection(SyncOperation op) {
		// es2.x はtypeの削除が不可となったため、1件づつデータを削除する
		// 削除処理はoplogとの不整合を防ぐため、同期で実行する
		String syncName = getConfig().getSyncName();
		logger.info(op.getOp() + " index:" + indexName + " type:" + op.getCollection());
		AsyncExecutor.execute(new EsTypeDeleter(esClient, indexName, op.getCollection())).block();
		logger.debug("[{}] type deleted.[{}]", syncName, op.getCollection());

		// ステータス更新用のリクエストを追加する
		getBulkProcessor().add(EsUtils.makeStatusRequest(getConfig(), null, oplogTs));
	}

	/**
	 ********************************************
	 * create insert/update request.
	 * @param op
	 * @return
	 ********************************************
	 */
	private UpdateRequest makeIndexRequest(SyncOperation op) {
		return EsUtils.makeIndexRequest(op.getIndex(), op.getCollection(), op.getId(), op.getJson());
	}

	/**
	 ********************************************
	 * create delete request.
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
		if (_processor == null) {
			_processor = BulkProcessor.builder(esClient, this)
					.setBulkActions(DEFAULT_BUlK_ACTIONS)
					.setBulkSize(new ByteSizeValue(DEFAULT_BULK_SIZE, ByteSizeUnit.MB))
					.setFlushInterval(TimeValue.timeValueMillis(DEFAULT_BUlK_INTERVAL))
					.setConcurrentRequests(1)
					.build();
		}
		return _processor;
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
		request.add(EsUtils.makeStatusRequest(getConfig(), null, getCurOplogTs()));
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
	 * @see org.elasticsearch.action.bulk.BulkProcessor.Listener#afterBulk(long, org.elasticsearch.action.bulk.BulkRequest, org.elasticsearch.action.bulk.BulkResponse)
	 ********************************************
	 */
	@Override
	public void afterBulk(long executionId, BulkRequest request, BulkResponse response) {
		logger.trace(String.format("[%s] call afterBulk() size=%d, [%d ms]",
								getConfig().getSyncName(),
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
}
