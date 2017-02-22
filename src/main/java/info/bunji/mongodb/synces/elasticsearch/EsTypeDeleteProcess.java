/**
 *
 */
package info.bunji.mongodb.synces.elasticsearch;

import org.elasticsearch.action.admin.indices.exists.indices.IndicesExistsRequest;
import org.elasticsearch.action.admin.indices.exists.indices.IndicesExistsResponse;
import org.elasticsearch.action.bulk.BulkRequestBuilder;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.SearchHit;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import info.bunji.asyncutil.AsyncProcess;

/**
 ************************************************
 * 指定されたインデックスのタイプに含まれるデータをすべて削除する.
 *
 * @author Fumiharu Kinoshita
 ************************************************
 */
public class EsTypeDeleteProcess extends AsyncProcess<Boolean> {

	private Logger logger = LoggerFactory.getLogger(getClass());

	private Client esClient;
	private String index;
	private String type;

	public EsTypeDeleteProcess(Client esClient, String index, String type) {
		this.esClient = esClient;
		this.index = index;
		this.type = type;
	}

	@Override
	protected void execute() throws Exception {

		// 削除対象インデックスの存在チェック
		IndicesExistsResponse res = esClient.admin().indices().exists(new IndicesExistsRequest(index)).actionGet();
		int retry = 10;
		long delcount = 0;
		try {
			while (retry > 0) {
				if (!res.isExists()) {
					break;
				}

				// 対象タイプのドキュメントを全削除
				QueryBuilder qb = QueryBuilders.matchAllQuery();
				SearchResponse scrollRes = esClient.prepareSearch(index)
						.setTypes(type)
						.setQuery(qb)
						.addFields("_id")
						.setScroll(new TimeValue(60000))
						.setSize(1000).execute().actionGet();

				BulkRequestBuilder bulkRequest = esClient.prepareBulk();
				while (true) {
					for (SearchHit hit : scrollRes.getHits().getHits()) {
						// Handle the hit...
						if ((++delcount % 5000) == 0) {
							logger.debug("deleting " + index + "/" + type + "(" + delcount + ")");
						}
						bulkRequest.add(esClient.prepareDelete(index, type, hit.getId()));
					}
					scrollRes = esClient.prepareSearchScroll(scrollRes.getScrollId())
									.setScroll(new TimeValue(6000000))
									.execute().actionGet();

					// Break condition: No hits are returned
					if (scrollRes.getHits().getHits().length == 0) {
						break;
					}
				}

				if (bulkRequest.numberOfActions() > 0) {
					BulkResponse bulkResponse = bulkRequest.execute().actionGet();
					if (bulkResponse.hasFailures()) {
						// process failures by iterating through each bulk response item
						logger.error("Bulk deletion Failed");
					}
					// TODO 本来ならマッピングを初期化すべき？
					//esClient.admin().indices().putMapping()
				}
				retry--;
			}
		} finally {
			logger.info("index type deleted. [" + index + "/" + type + "] " +  delcount);
			append(Boolean.TRUE);

			// index refresh.
			EsUtils.refreshIndex(esClient, index);
		}
	}
}
