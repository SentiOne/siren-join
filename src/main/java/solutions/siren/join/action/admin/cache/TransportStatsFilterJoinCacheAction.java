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
package solutions.siren.join.action.admin.cache;

import org.elasticsearch.action.FailedNodeException;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.nodes.TransportNodesAction;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;

import java.io.IOException;
import java.util.List;

public class TransportStatsFilterJoinCacheAction extends TransportNodesAction<StatsFilterJoinCacheRequest,
        StatsFilterJoinCacheResponse, StatsFilterJoinCacheNodeRequest, StatsFilterJoinCacheNodeResponse> {

  private final ClusterService clusterService;
  private final FilterJoinCacheService cacheService;

  @Inject
  public TransportStatsFilterJoinCacheAction(ThreadPool threadPool,
                                             ClusterService clusterService, FilterJoinCacheService cacheService,
                                             TransportService transportService, ActionFilters actionFilters) {
    super(StatsFilterJoinCacheAction.NAME, threadPool, clusterService, transportService,
            actionFilters, StatsFilterJoinCacheRequest::new, StatsFilterJoinCacheNodeRequest::new,
            ThreadPool.Names.MANAGEMENT, StatsFilterJoinCacheNodeResponse.class);
    this.cacheService = cacheService;
    this.clusterService = clusterService;
  }

  @Override
  protected StatsFilterJoinCacheResponse newResponse(StatsFilterJoinCacheRequest request,
                                                     List<StatsFilterJoinCacheNodeResponse> statsFilterJoinCacheNodeResponses,
                                                     List<FailedNodeException> failures) {
    return new StatsFilterJoinCacheResponse(clusterService.getClusterName(), statsFilterJoinCacheNodeResponses, failures);
  }

  @Override
  protected StatsFilterJoinCacheNodeRequest newNodeRequest(StatsFilterJoinCacheRequest request) {
    return new StatsFilterJoinCacheNodeRequest(request);
  }

  @Override
  protected StatsFilterJoinCacheNodeResponse newNodeResponse(StreamInput in) throws IOException {
    return new StatsFilterJoinCacheNodeResponse(in);
  }

  @Override
  protected StatsFilterJoinCacheNodeResponse nodeOperation(StatsFilterJoinCacheNodeRequest request) {
    logger.debug("Clearing filter join cache on node {}", clusterService.localNode());
    return new StatsFilterJoinCacheNodeResponse(clusterService.localNode(), System.currentTimeMillis(), cacheService.getStats());
  }

}
