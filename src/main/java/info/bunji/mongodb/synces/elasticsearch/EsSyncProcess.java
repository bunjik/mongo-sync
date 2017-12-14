/**
 *
 */
package info.bunji.mongodb.synces.elasticsearch;

import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.TreeMap;

import org.bson.BsonTimestamp;
import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.action.ActionRequest;
import org.elasticsearch.action.DocumentRequest;
import org.elasticsearch.action.bulk.BackoffPolicy;
import org.elasticsearch.action.bulk.BulkItemResponse;
import org.elasticsearch.action.bulk.BulkProcessor;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.delete.DeleteRequest;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.update.UpdateRequest;
import org.elasticsearch.client.Client;
import org.elasticsearch.common.unit.ByteSizeUnit;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.indices.IndexClosedException;

import info.bunji.asyncutil.AsyncExecutor;
import info.bunji.asyncutil.AsyncResult;
import info.bunji.mongodb.synces.MongoEsSync;
import info.bunji.mongodb.synces.Status;
import info.bunji.mongodb.synces.StatusChecker;
import info.bunji.mongodb.synces.SyncConfig;
import info.bunji.mongodb.synces.SyncOperation;
import info.bunji.mongodb.synces.SyncProcess;
import info.bunji.mongodb.synces.util.DocumentUtils;

/**
 ************************************************
 * Sync process for elasticearch.
 * @author Fumiharu Kinoshita
 ************************************************
 */
public class EsSyncProcess extends SyncProcess implements BulkProcessor. Listener {

	private final Client esClient;
	private final String indexName;

	private Map<Long, BsonTimestamp> requestOplogTs = new HashMap<>();

	private BulkProcessor _processor = null;

	private final int DEFAULT_BUlK_ACTIONS;
	private final long DEFAULT_BUlK_INTERVAL;
	private final long DEFAULT_BULK_SIZE;

	/**
	 **********************************
	 * @param esClient
	 * @param config
	 * @param operations
	 **********************************
	 */
	public EsSyncProcess(Client esClient, SyncConfig config, StatusChecker<?> listener, AsyncResult<SyncOperation> operations) {
		super(config, operations);
		this.esClient = esClient;
		this.indexName = config.getDestDbName();

		Properties prop = MongoEsSync.getSettingProperties();
		DEFAULT_BUlK_ACTIONS = Integer.valueOf(prop.getProperty("es.bulk.actions"));
		DEFAULT_BUlK_INTERVAL = Long.valueOf(prop.getProperty("es.bulk.interval"));
		DEFAULT_BULK_SIZE = Long.valueOf(prop.getProperty("es.bulk.sizeMb"));
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
		if (EsStatusChecker.CONFIG_INDEX.equals(op.getDestDbName())) {
			getBulkProcessor().add(makeIndexRequest(op));
		} else if (getConfig().isTargetCollection(op.getCollection())) {
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
		getBulkProcessor().add(makeDeleteRequest(op));
	}

	/*
	 **********************************
	 * (non Javadoc)
	 * @see info.bunji.mongodb.synces.SyncProcess#doCreateCollection(info.bunji.mongodb.synces.SyncOperation)
	 **********************************
	 */
	@Override
	protected void doCreateCollection(SyncOperation op) {
		// do nothing.
	}

	/*
	 **********************************
	 * (non Javadoc)
	 * @see info.bunji.mongodb.synces.AbstractSyncProcess#doDropCollection(info.bunji.mongodb.synces.SyncOperation)
	 **********************************
	 */
	@Override
	public void doDropCollection(SyncOperation op) {
		// 既存リクエスト分を反映
		_processor.flush();

		// es2.x以降はtypeの削除が不可となったため、1件づつデータを削除する
		// 削除処理はoplogとの不整合を防ぐため、同期で実行する
		String syncName = getConfig().getSyncName();
		logger.info(op.getOp() + " index:" + indexName + " type:" + op.getCollection());
		AsyncExecutor.execute(new EsTypeDeleteProcess(esClient, getConfig(), op.getCollection())).block();
		logger.debug("[{}] type deleted.[{}]", syncName, op.getCollection());

		// TODO ステータス更新用のリクエストを追加する
		getBulkProcessor().add(EsUtils.makeStatusRequest(getConfig(), null, oplogTs));
	}

	/**
	 ********************************************
	 * create insert/update request.
	 * @param op
	 * @return
	 ********************************************
	 */
	private IndexRequest makeIndexRequest(SyncOperation op) {
		return new IndexRequest(op.getDestDbName(), op.getCollection(), op.getId()).source(op.getJson());
	}

	/**
	 ********************************************
	 * create delete request.
	 * @param op
	 * @return
	 ********************************************
	 */
	private DeleteRequest makeDeleteRequest(SyncOperation op) {
		return new DeleteRequest(op.getDestDbName(), op.getCollection(), op.getId());
	}

	/**
	 ********************************************
	 * get elasticsearch bulk processor.
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
					.setBackoffPolicy(BackoffPolicy.exponentialBackoff())
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
		// keep oplog time per executionId
		requestOplogTs.put(executionId, getCurOplogTs());
	}

	/**
	 ********************************************
	 * バルク処理の終了時(エラーあり)に呼び出される.
	 * @see org.elasticsearch.action.bulk.BulkProcessor.Listener#afterBulk(long, org.elasticsearch.action.bulk.BulkRequest, java.lang.Throwable)
	 ********************************************
	 */
	@Override
	public void afterBulk(long executionId, BulkRequest request, Throwable failure) {

		BulkDetail detail = new BulkDetail(request);
		logger.error(String.format("[%s] bulk failure. size=[%d] oplog=[%s] op=[upsert={}/delete={}/other={}] : %s",
				getConfig().getSyncName(),
				detail.getLength(),
				DocumentUtils.toDateStr(requestOplogTs.get(executionId)),
				detail.update, detail.delete, detail.other,
				failure.getMessage()
			), failure);
		logger.trace("[{}] {}", getConfig().getSyncName(), detail);

		getConfig().addSyncCount(detail.getModified());

//		if (failure instanceof UnavailableShardsException) {
//			// shard status error.
//			// TODO if bulk process fatal error, stop sync or retry?
//			// retryするなら、今回のbulk分も再実行すべきなので、この処理内では収まらないはず
//			// 再度、最終更新分からoplogの再同期が必要で、statusの更新をしてはいけない
//		}

		// if target index closed, stop sync(not update status)
		if (failure instanceof IndexClosedException) {
			throw new ElasticsearchException(failure.getMessage(), failure);
		}

 		try {
			// try sratus update
			esClient.update(EsUtils.makeStatusRequest(getConfig(), Status.STOPPED, null)).actionGet();
			EsUtils.refreshIndex(esClient, EsStatusChecker.CONFIG_INDEX);
		} catch (Throwable t) {
			throw new ElasticsearchException(failure.getMessage(), failure);
		}
	}

	/**
	 ********************************************
	 * バルク処理の終了時に呼び出される.
	 * @see org.elasticsearch.action.bulk.BulkProcessor.Listener#afterBulk(long, org.elasticsearch.action.bulk.BulkRequest, org.elasticsearch.action.bulk.BulkResponse)
	 ********************************************
	 */
	@Override
	public void afterBulk(long executionId, BulkRequest request, BulkResponse response) {
		BulkDetail detail = new BulkDetail(response);
		if (logger.isTraceEnabled() && !detail.isEmpty()) {
			logger.debug(String.format("[%s] bulk size=[%4d] oplog=[%s] op=[upsert=%d/delete=%d/other=%d](%4dms)",
					getConfig().getSyncName(),
					detail.getLength(),
					DocumentUtils.toDateStr(requestOplogTs.get(executionId)),
					detail.update, detail.delete, detail.other,
					response.getTookInMillis()));
			logger.trace("[{}] {}", getConfig().getSyncName(), detail);
		}

		getConfig().addSyncCount(detail.getModified());

		for (BulkItemResponse item : response) {
			if (item.isFailed()) {
				logger.warn("[{}] index:[{}], type:[{}] id:[{}] msg:[{}]",
								item.getItemId(), item.getIndex(), item.getType(), item.getId(),
								item.getFailureMessage());
			}
		}
		requestOplogTs.remove(executionId);
	}

	/**
	 ********************************************
	 * 
	 ********************************************
	 */
	private static class BulkDetail {

		private int update = 0;
		private int delete = 0;
		private int other  = 0;
		//private int failed = 0;

		private Map<String, ActionCount> actionMap = new TreeMap<>();
		
		/**
		 ******************************
		 * 
		 * @param res
		 ******************************
		 */
		public BulkDetail(BulkResponse res) {
			if (res != null) {
				for (BulkItemResponse item : res) {
					String index = item.getIndex();
					String type = item.getType();
					switch (item.getOpType()) {
					case "index" :
					case "update" :
						incCount(index, type);
						break;
					case "delete" :
						decCount(index, type);
						break;
					default :
						other++;
						break;	// do nothing.
					}
				}
			}
		}

		/**
		 ******************************
		 * 
		 * @param req
		 ******************************
		 */
		public BulkDetail(BulkRequest req) {
			if (req != null) {
				for (ActionRequest<?> action : req.requests()) {
					if (action instanceof IndexRequest || action instanceof UpdateRequest) {
						String index = DocumentRequest.class.cast(action).index();
						String type = DocumentRequest.class.cast(action).type();
						incCount(index, type);
					} else if (action instanceof DeleteRequest) {
						String index = DocumentRequest.class.cast(action).index();
						String type = DocumentRequest.class.cast(action).type();
						decCount(index, type);
					} else {
						other++;
					}
				}
			}
		}
		
		public int getLength() {
			return update + delete + other;
		}

		public int getModified() {
			return update + delete;
		}

		private void incCount(final String index, final String typeName) {
			if (!EsStatusChecker.CONFIG_INDEX.equals(index)) {
				if (!actionMap.containsKey(typeName)) {
					actionMap.put(typeName, new ActionCount());
				}
				actionMap.get(typeName).add();
				update++;
			}
		}

		private void decCount(final String index, final String typeName) {
			if (!EsStatusChecker.CONFIG_INDEX.equals(index)) {
				if (!actionMap.containsKey(typeName)) {
					actionMap.put(typeName, new ActionCount());
				}
				actionMap.get(typeName).del();
				delete++;
			}
		}

		public boolean isEmpty() {
			return actionMap.isEmpty();
		}

		@Override
		public String toString() {
			return actionMap.toString();
		}
	}

	/**
	 * 
	 */
	private static class ActionCount {
		private int add = 0;
		private int del = 0;

		public void add() {
			add++;
		}

		public void del() {
			del++;
		}

		@Override
		public String toString() {
			String ret = "(-)";
			if (add != 0 && del != 0) {
				ret = String.format("(U:%d/D:%d)", add, del);
			} else if (add == 0) {
				ret = String.format("(D:%d)", del);
			} else if (del == 0) {
				ret = String.format("(U:%d)", add);
			}
			return ret;
		}
	}
}
