/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.elasticsearch.node;

import org.elasticsearch.action.admin.cluster.node.stats.NodeStats;
import io.crate.common.io.IOUtils;
import org.elasticsearch.indices.IndicesService;
import org.elasticsearch.monitor.MonitorService;
import org.elasticsearch.transport.TransportService;

import java.io.Closeable;
import java.io.IOException;

public class NodeService implements Closeable {

    private final MonitorService monitorService;
    private final IndicesService indicesService;
    private final TransportService transportService;

    NodeService(MonitorService monitorService, IndicesService indicesService, TransportService transportService) {
        this.monitorService = monitorService;
        this.indicesService = indicesService;
        this.transportService = transportService;
    }

    public MonitorService getMonitorService() {
        return monitorService;
    }

    @Override
    public void close() throws IOException {
        IOUtils.close(indicesService);
    }

    public NodeStats stats() {
        return new NodeStats(transportService.getLocalNode(), System.currentTimeMillis(), monitorService.fsService().stats());
    }
}
