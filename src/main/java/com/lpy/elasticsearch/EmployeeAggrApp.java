package com.lpy.elasticsearch;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.TransportAddress;
import org.elasticsearch.search.aggregations.AggregationBuilders;
import org.elasticsearch.search.aggregations.bucket.terms.LongTerms;
import org.elasticsearch.search.aggregations.bucket.terms.StringTerms;
import org.elasticsearch.search.aggregations.metrics.avg.Avg;
import org.elasticsearch.transport.client.PreBuiltTransportClient;
import org.junit.Test;

import java.net.InetAddress;
import java.util.List;

/**
 * 员工聚合分析应用程序
 * @author Administrator
 *
 */
public class EmployeeAggrApp {

	private Logger logger = LogManager.getLogger(EmployeeAggrApp.class);

	@Test
	public void test() throws Exception {
		Settings settings = Settings.builder()
				.put("cluster.name", "elasticsearch")
				.build();

		TransportClient client = new PreBuiltTransportClient(settings)
				.addTransportAddress(new TransportAddress(InetAddress.getByName("127.0.0.1"), 9300));

		aggrEmployee(client);

		client.close();
	}

	/**
	 * 创建员工信息（创建一个document）
	 *
	 * @param client
	 */
	private void aggrEmployee(TransportClient client) {
		SearchResponse response = client.prepareSearch("employee")
				.setSize(0)
				.addAggregation(AggregationBuilders.terms("group_by_country")
						.field("country")
						.subAggregation(AggregationBuilders.terms("group_by_join_date")
								.field("join_date")
								.subAggregation(AggregationBuilders.avg("avg_salary")
										.field("salary"))))
				.get();
		StringTerms groupByCountry = (StringTerms) response.getAggregations().getAsMap().get("group_by_country");
		List<StringTerms.Bucket> buckets = groupByCountry.getBuckets();
		buckets.forEach(b -> {
			logger.info("country: " + b.getKey());
			logger.info("countryCount: " + b.getDocCount());
			LongTerms groupByJoinDate = (LongTerms) b.getAggregations().getAsMap().get("group_by_join_date");
			List<LongTerms.Bucket> subBuckets = groupByJoinDate.getBuckets();
			subBuckets.forEach(sb -> {
				logger.info("\tjoinDate: " + sb.getKeyAsString());
				logger.info("\tjoinDateCount: " + sb.getDocCount());
				Avg avg = (Avg) sb.getAggregations().getAsMap().get("avg_salary");
				logger.info("\t\tavgSalary: " + avg.getValue());
			});
		});
	}
}






























































