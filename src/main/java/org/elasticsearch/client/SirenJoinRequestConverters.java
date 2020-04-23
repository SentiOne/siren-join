package org.elasticsearch.client;

import org.elasticsearch.action.search.SearchRequest;

import java.io.IOException;

public class SirenJoinRequestConverters {

	public static Request coordinateSearch(SearchRequest searchRequest) throws IOException {
		return RequestConverters.search(searchRequest, "_coordinate_search");
	}
}
