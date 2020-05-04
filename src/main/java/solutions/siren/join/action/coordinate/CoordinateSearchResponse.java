/**
 * Copyright (c) 2016, SIREn Solutions. All Rights Reserved.
 *
 * This file is part of the SIREn project.
 *
 * SIREn is a free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as
 * published by the Free Software Foundation, either version 3 of
 * the License, or (at your option) any later version.
 *
 * SIREn is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public
 * License along with this program. If not, see <http://www.gnu.org/licenses/>.
 */
package solutions.siren.join.action.coordinate;

import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.search.SearchResponseSections;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.search.profile.SearchProfileShardResults;
import solutions.siren.join.action.coordinate.execution.CoordinateSearchMetadata;

import java.io.IOException;

/**
 * A search response for the coordinate search action. It is a decorator around the {@link SearchResponse}
 * which injects filter join execution metadata into the response.
 * <br>
 * We use a decorator pattern instead of a subclass extension because there are many hard-coded
 * instantiations of {@link SearchResponse}, and it would have required to
 * subclass all of them to instantiate a {@link CoordinateSearchResponse} instead.
 */
public class CoordinateSearchResponse extends SearchResponse {

  private SearchResponse searchResponse;
  private CoordinateSearchMetadata coordinateSearchMetadata;

  public CoordinateSearchResponse(SearchResponse response, CoordinateSearchMetadata metadata) {
    super(new SearchResponseSections(response.getHits(), response.getAggregations(), response.getSuggest(),
            response.isTimedOut(), response.isTerminatedEarly(), new SearchProfileShardResults(response.getProfileResults()), response.getNumReducePhases()),
            response.getScrollId(), response.getTotalShards(), response.getSuccessfulShards(), response.getSkippedShards(), response.getTook().millis(), response.getShardFailures(), response.getClusters());
    this.searchResponse = response;
    this.coordinateSearchMetadata = metadata;
  }

  /**
   * Constructor for {@link CoordinateSearchAction#newResponse()} and
   * deserialization in {@link CoordinateMultiSearchResponse.Item}.
   */
  CoordinateSearchResponse(StreamInput in) throws IOException {
    super(in);

    this.coordinateSearchMetadata = new CoordinateSearchMetadata();
    this.coordinateSearchMetadata.readFrom(in);
  }

  @Override
  public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
    builder.startObject();

    coordinateSearchMetadata.toXContent(builder);
    if (searchResponse != null) {
      searchResponse.innerToXContent(builder, params);
    } else {
      super.innerToXContent(builder, params);
    }

    builder.endObject();

    return builder;
  }

  @Override
  public void writeTo(StreamOutput out) throws IOException {
    if (searchResponse != null) {
      searchResponse.writeTo(out);
    } else {
      super.writeTo(out);
    }
    this.coordinateSearchMetadata.writeTo(out);
  }

}
