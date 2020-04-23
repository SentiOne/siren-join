package org.elasticsearch.client;

import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.rest.action.search.RestSearchAction;

import java.io.IOException;
import java.util.Map;

public class SirenJoinRequestConverters {

	public static Request coordinateSearch(SearchRequest searchRequest) throws IOException {
		Request baseRequest = RequestConverters.search(searchRequest, "_coordinate_search");
		Request request = new Request(baseRequest.getMethod(), RequestConverters.endpoint(searchRequest.indices(), searchRequest.types(), "_coordinate_search"));
		for (Map.Entry<String, String> entry : baseRequest.getParameters().entrySet()) {
			if (!RestSearchAction.TYPED_KEYS_PARAM.equals(entry.getKey())) {
				request.addParameter(entry.getKey(), entry.getValue());
			}
		}
		request.setEntity(baseRequest.getEntity());
		request.setOptions(baseRequest.getOptions());
		return request;
	}
}
