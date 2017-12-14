/**
 * 
 */
package info.bunji.mongodb.synces;

import java.io.IOException;
import java.util.EventListener;

import org.bson.BsonTimestamp;

import info.bunji.asyncutil.AsyncProcess;
import info.bunji.asyncutil.AsyncResult;

/**
 ************************************************
 * sync process base implementation.
 * @author Fumiharu Kinoshita
 ************************************************
 */
public abstract class SyncProcess extends AsyncProcess<Boolean> 
											implements StatusChangeListener {

	private AsyncResult<SyncOperation> operations;
	private SyncConfig config;
	protected BsonTimestamp oplogTs;

	/**
	 * @param config sync config
	 * @param operations mongodb opelations
	 */
	public SyncProcess(SyncConfig config, AsyncResult<SyncOperation> operations) {
		this.config = config;
		this.operations = operations;
	}

	/**
	 * 
	 * @param e occurred exeption
	 * @throws Exception
	 */
	protected void onError(Exception e) throws Exception {
		throw e;
	}

	@Override
	protected void execute() throws Exception {
		String syncName = getConfig().getSyncName();

		logger.info("[{}] start sync.", syncName);
		try {
			for (SyncOperation op : operations) {
				// get oplog timestamp
				oplogTs = op.getTimestamp();

				switch (op.getOp()) {

				case INSERT:
					doInsert(op);
					break;
				case UPDATE:
					doUpdate(op);
					break;
				case DELETE:
					doDelete(op);
					break;
				case DROP_COLLECTION:
					doDropCollection(op);
					break;
				case DROP_DATABASE:
					doDropDatabse(op);
					break;
				case CREATE_COLLECTION:
					doCreateCollection(op);
					break;
				default:
					logger.info("[{}] unsupported operation. [{}/{}]",
											syncName, op.getCollection(), op.getOp());
					break;
				}
			}
		} catch (Exception e) {
			onError(e);
		} finally {
			logger.info("[{}] stop sync.", syncName);
		}
	}

	/**
	 **********************************
	 * get current operation oplog timestamp.
	 * @return oplog timestamp
	 **********************************
	 */
	protected BsonTimestamp getCurOplogTs() {
		return oplogTs;
	}

	/**
	 **********************************
	 * get sync config.
	 * @return sync config
	 **********************************
	 */
	public SyncConfig getConfig() {
		return config;
	}

	/**
	 **********************************
	 * process insert operation.
	 * @param op sync operation
	 **********************************
	 */
	protected void doInsert(SyncOperation op) {
		logger.debug("unsupported operation. [{}/{}]", op.getCollection(), op.getOp());
	}

	/**
	 **********************************
	 * process update operation.
	 * @param op sync operation
	 **********************************
	 */
	protected void doUpdate(SyncOperation op) {
		logger.debug("unsupported operation. [{}/{}]", op.getCollection(), op.getOp());
	}

	/**
	 **********************************
	 * process delete operation.
	 * @param op sync operation
	 **********************************
	 */
	protected void doDelete(SyncOperation op) {
		logger.debug("unsupported operation. [{}/{}]", op.getCollection(), op.getOp());
	}

	/**
	 **********************************
	 * process collection create operation.
	 * @param op sync operation
	 **********************************
	 */
	protected void doCreateCollection(SyncOperation op) {
		logger.debug("unsupported operation. [{}/{}]", op.getCollection(), op.getOp());
	}

	/**
	 **********************************
	 * process collection drop operation.
	 * @param op sync operation
	 **********************************
	 */
	protected void doDropCollection(SyncOperation op) {
		logger.debug("unsupported operation. [{}/{}]", op.getCollection(), op.getOp());
	}

	/**
	 **********************************
	 * process database drop operation.
	 * @param op sync operation
	 **********************************
	 */
	protected void doDropDatabse(SyncOperation op) {
		logger.debug("unsupported operation. [{}/{}]", op.getCollection(), op.getOp());
	}

	/*
	 ********************************************
	 * (non Javadoc)
	 * @see info.bunji.mongodb.synces.StatusChangeListener#stop()
	 ********************************************
	 */
	@Override
	public void stop() {
		try {
			// stop mongo data extract thread..
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
	public static interface Listener extends EventListener {
		/**
		 ******************************
		 * indexerの停止時に呼び出されるメソッド.
		 * @param syncName 同期設定名
		 ******************************
		 */
		void onIndexerStop(String syncName);
	}
}
