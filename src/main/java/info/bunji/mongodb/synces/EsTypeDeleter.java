/**
 *
 */
package info.bunji.mongodb.synces;

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
 * 指定されたインデックスのタイプに含まれるデータをすべて削除する.
 *
 * @author Fumiharu Kinoshita
 */
public class EsTypeDeleter extends AsyncProcess<Boolean> {

	private Logger logger = LoggerFactory.getLogger(getClass());

	private Client esClient;
	private String index;
	private String type;

	public EsTypeDeleter(Client esClient, String index, String type) {
		this.esClient = esClient;
		this.index = index;
		this.type = type;
	}

	@Override
	protected void execute() throws Exception {

		IndicesExistsResponse res = esClient.admin().indices().exists(new IndicesExistsRequest(index)).actionGet();
		if (!res.isExists()) {
			return;
		}

		try {
			// 対象タイプのドキュメントを全削除
			QueryBuilder qb = QueryBuilders.matchAllQuery();
			SearchResponse scrollRes = esClient.prepareSearch(index)
					.setTypes(type)
					.setQuery(qb)
					.addFields("_id")
					.setScroll(new TimeValue(60000))
					.setSize(1000).execute().actionGet();

			BulkRequestBuilder bulkRequest = esClient.prepareBulk();
			long delcount = 0;
			while (true) {
				for (SearchHit hit : scrollRes.getHits().getHits()) {
					//Handle the hit...
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
			logger.info("index type deleted. [" + index + "/" + type + "] " +  delcount);
			append(Boolean.TRUE);
		} finally {
			// do nothing.
		}
	}
}
