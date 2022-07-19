package com.msd.gin.halyard.sail.search;

import java.io.IOException;

import co.elastic.clients.elasticsearch.ElasticsearchClient;
import co.elastic.clients.elasticsearch._types.GeoLocation;
import co.elastic.clients.elasticsearch.core.SearchResponse;

public final class SearchClient {
	public static final int DEFAULT_RESULT_SIZE = 10000;
	public static final int DEFAULT_FUZZINESS = 1;
	public static final int DEFAULT_PHRASE_SLOP = 0;

	private final ElasticsearchClient client;
	private final String index;

	public SearchClient(ElasticsearchClient client, String index) {
		this.client = client;
		this.index = index;
	}

	public SearchResponse<SearchDocument> search(String query, int limit, int fuzziness, int slop) throws IOException {
		return client.search(s -> s.index(index).query(q -> q.queryString(qs -> qs.query(query).defaultField(SearchDocument.LABEL_FIELD).fuzziness(Integer.toString(fuzziness)).phraseSlop(Double.valueOf(slop)))).size(limit),
				SearchDocument.class);
	}

	public SearchResponse<SearchDocument> search(double lat, double lon, double dist, String units) throws IOException {
		return client.search(s -> s.index(index).query(q -> q.geoDistance(gd -> gd.field(SearchDocument.LABEL_POINT_FIELD).location(GeoLocation.of(gl -> gl.latlon(ll -> ll.lat(lat).lon(lon)))).distance(dist + units))),
				SearchDocument.class);
	}
}
