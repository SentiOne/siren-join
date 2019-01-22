package solutions.siren.join;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.*;
import org.elasticsearch.common.CheckedConsumer;
import org.elasticsearch.common.xcontent.NamedXContentRegistry;

import java.io.IOException;
import java.util.List;

import static java.util.Collections.emptySet;

public class SirenJoinRestClient extends RestHighLevelClient {

	public SirenJoinRestClient(RestClientBuilder restClientBuilder) {
		super(restClientBuilder);
	}

	public SirenJoinRestClient(RestClientBuilder restClientBuilder, List<NamedXContentRegistry.Entry> namedXContentEntries) {
		super(restClientBuilder, namedXContentEntries);
	}

	public SirenJoinRestClient(RestClient restClient, CheckedConsumer<RestClient, IOException> doClose, List<NamedXContentRegistry.Entry> namedXContentEntries) {
		super(restClient, doClose, namedXContentEntries);
	}

	public final SearchResponse coordinateSearch(SearchRequest searchRequest, RequestOptions options) throws IOException {
		return performRequestAndParseEntity(searchRequest, SirenJoinRequestConverters::coordinateSearch, options, SearchResponse::fromXContent, emptySet());
	}

	public final void coordinateSearchAsync(SearchRequest searchRequest, RequestOptions options, ActionListener<SearchResponse> listener) {
		performRequestAsyncAndParseEntity(searchRequest, SirenJoinRequestConverters::coordinateSearch, options, SearchResponse::fromXContent, listener, emptySet());
	}
}
