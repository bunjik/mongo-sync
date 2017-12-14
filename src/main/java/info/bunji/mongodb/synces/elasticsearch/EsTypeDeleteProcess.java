/**
 *
 */
package info.bunji.mongodb.synces.elasticsearch;

import org.elasticsearch.action.bulk.BackoffPolicy;
import org.elasticsearch.action.bulk.BulkProcessor;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.delete.DeleteRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.common.unit.ByteSizeUnit;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.SearchHit;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import info.bunji.asyncutil.AsyncProcess;
import info.bunji.mongodb.synces.SyncConfig;

/**
 ************************************************
 * 指定されたインデックスのタイプに含まれるデータをすべて削除する.
 *
 * @author Fumiharu Kinoshita
 ************************************************
 */
public class EsTypeDeleteProcess extends AsyncProcess<Boolean> implements BulkProcessor.Listener {

	private Logger logger = LoggerFactory.getLogger(getClass());

	private Client esClient;
//	private SyncConfig config;
	private String index;
	private String type;

	public EsTypeDeleteProcess(Client esClient, SyncConfig config, String type) {
		this.esClient = esClient;
//		this.config = config;
		this.index = config.getDestDbName();
		this.type = type;
	}

	/**
	 * execute type delete.
	 */
	@Override
	protected void execute() throws Exception {
		int delCnt = 0;

		if (EsUtils.isExistsIndex(esClient, index)) {
			BulkProcessor bulkProc = BulkProcessor.builder(esClient, this)
										.setBulkActions(10000)
										.setFlushInterval(TimeValue.timeValueMillis(500))
										.setBackoffPolicy(BackoffPolicy.exponentialBackoff())
										.setBulkSize(new ByteSizeValue(64, ByteSizeUnit.MB))
										.setConcurrentRequests(2)
										.build();
			try {
				TimeValue scrollTimeout = new TimeValue(60000);

				// all _id in type
				SearchResponse response = esClient.prepareSearch(index)
											.setTypes(type)
											.setQuery(QueryBuilders.matchAllQuery())
											.setScroll(scrollTimeout)
											.setSize(3000)
											.setFetchSource(false)
											.execute().actionGet();
				while (true) {
					boolean isExists = false;
					for (SearchHit hit : response.getHits()) {
						delCnt++;
						if ((++delCnt % 5000) == 0) {
							logger.debug("deleting " + index + "/" + type + "(" + delCnt + ")");
						}
						bulkProc.add(new DeleteRequest(index, type, hit.getId()));
						isExists = true;
					}
					if (!isExists) {
						//esClient.clearScroll(new ClearScrollRequest());
						break;
					}
					response = esClient.prepareSearchScroll(response.getScrollId())
										.setScroll(scrollTimeout).execute().actionGet();
				}
			} finally {
				bulkProc.flush();
				bulkProc.close();
			}
			logger.info("index type deleted. [" + index + "/" + type + "] " +  delCnt);
		}
	}

	@Override
	public void beforeBulk(long executionId, BulkRequest request) {
		// do nothing.
	}

	@Override
	public void afterBulk(long executionId, BulkRequest request, BulkResponse response) {
//		logger.debug("type deleting {}/{} [{}] ({}ms)",
//						index, type, response.getItems(), response.getTook().getMillis());
		// do nothing.
	}

	@Override
	public void afterBulk(long executionId, BulkRequest request, Throwable failure) {
		// TODO 自動生成されたメソッド・スタブ
		
	}
}
