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

import org.elasticsearch.common.component.AbstractComponent;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.core.internal.io.IOUtils;
import org.elasticsearch.indices.IndicesService;
import org.elasticsearch.monitor.MonitorService;

import java.io.Closeable;
import java.io.IOException;

public class NodeService extends AbstractComponent implements Closeable {

    private final MonitorService monitorService;
    private final IndicesService indicesService;

    NodeService(Settings settings, MonitorService monitorService, IndicesService indicesService) {
        super(settings);
        this.monitorService = monitorService;
        this.indicesService = indicesService;
    }

    public MonitorService getMonitorService() {
        return monitorService;
    }

    @Override
    public void close() throws IOException {
        IOUtils.close(indicesService);
    }

}
