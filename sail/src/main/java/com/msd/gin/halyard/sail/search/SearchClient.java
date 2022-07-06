package com.msd.gin.halyard.sail.search;

import java.io.IOException;

import co.elastic.clients.elasticsearch.ElasticsearchClient;
import co.elastic.clients.elasticsearch.core.SearchResponse;

public final class SearchClient {
	public static final int MAX_RESULT_SIZE = 10000;
	private final ElasticsearchClient client;
	private final String index;

	public SearchClient(ElasticsearchClient client, String index) {
		this.client = client;
		this.index = index;
	}

	public SearchResponse<SearchDocument> search(String query, int limit) throws IOException {
		return client.search(s -> s.index(index).query(q -> q.queryString(qs -> qs.query(query))).size(limit), SearchDocument.class);
	}
}
