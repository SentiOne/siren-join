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

public class TransportClearFilterJoinCacheAction extends TransportNodesAction<ClearFilterJoinCacheRequest,
        ClearFilterJoinCacheResponse, ClearFilterJoinCacheNodeRequest, ClearFilterJoinCacheNodeResponse> {

  private final ClusterService clusterService;
  private final FilterJoinCacheService cacheService;

  @Inject
  public TransportClearFilterJoinCacheAction(ThreadPool threadPool,
                                                ClusterService clusterService, FilterJoinCacheService cacheService,
                                                TransportService transportService, ActionFilters actionFilters) {
    super(ClearFilterJoinCacheAction.NAME, threadPool, clusterService, transportService,
            actionFilters, ClearFilterJoinCacheRequest::new, ClearFilterJoinCacheNodeRequest::new,
            ThreadPool.Names.MANAGEMENT, ClearFilterJoinCacheNodeResponse.class);
    this.cacheService = cacheService;
    this.clusterService = clusterService;
  }

  @Override
  protected ClearFilterJoinCacheResponse newResponse(ClearFilterJoinCacheRequest request,
                                                     List<ClearFilterJoinCacheNodeResponse> clearFilterJoinCacheNodeResponses,
                                                     List<FailedNodeException> failures) {
    return new ClearFilterJoinCacheResponse(clusterService.getClusterName(), clearFilterJoinCacheNodeResponses, failures);
  }

  @Override
  protected ClearFilterJoinCacheNodeRequest newNodeRequest(ClearFilterJoinCacheRequest request) {
    return new ClearFilterJoinCacheNodeRequest(request);
  }

  @Override
  protected ClearFilterJoinCacheNodeResponse newNodeResponse(StreamInput in) throws IOException {
    return new ClearFilterJoinCacheNodeResponse(in);
  }

  @Override
  protected ClearFilterJoinCacheNodeResponse nodeOperation(ClearFilterJoinCacheNodeRequest request) {
    logger.debug("Clearing filter join cache on node {}", clusterService.localNode());
    cacheService.clear();
    return new ClearFilterJoinCacheNodeResponse(clusterService.localNode(), System.currentTimeMillis());
  }

}
