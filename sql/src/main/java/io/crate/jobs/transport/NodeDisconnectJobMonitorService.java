/*
 * Licensed to Crate under one or more contributor license agreements.
 * See the NOTICE file distributed with this work for additional
 * information regarding copyright ownership.  Crate licenses this file
 * to you under the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.  You may
 * obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
 * implied.  See the License for the specific language governing
 * permissions and limitations under the License.
 *
 * However, if you have executed another commercial license agreement
 * with Crate these terms will supersede the license and you may use the
 * software solely pursuant to the terms of the relevant commercial
 * agreement.
 */

package io.crate.jobs.transport;

import com.google.common.collect.Collections2;
import io.crate.jobs.JobContextService;
import io.crate.jobs.JobExecutionContext;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.common.component.AbstractLifecycleComponent;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.inject.Singleton;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportConnectionListener;
import org.elasticsearch.transport.TransportService;

import java.util.Collection;

/**
 * service that listens to node-disconnected-events and kills jobContexts that were started by the nodes that got disconnected
 */
@Singleton
public class NodeDisconnectJobMonitorService
    extends AbstractLifecycleComponent<NodeDisconnectJobMonitorService>
    implements TransportConnectionListener {

    private final ThreadPool threadPool;
    private final JobContextService jobContextService;
    private final TransportService transportService;

    private final static TimeValue DELAY = TimeValue.timeValueMinutes(1);

    @Inject
    public NodeDisconnectJobMonitorService(Settings settings,
                                           ThreadPool threadPool,
                                           JobContextService jobContextService,
                                           TransportService transportService) {
        super(settings);
        this.threadPool = threadPool;
        this.jobContextService = jobContextService;
        this.transportService = transportService;
    }


    @Override
    protected void doStart() {
        transportService.addConnectionListener(this);
    }

    @Override
    protected void doStop() {
        transportService.removeConnectionListener(this);
    }

    @Override
    protected void doClose() {
    }

    @Override
    public void onNodeConnected(DiscoveryNode node) {
    }

    @Override
    public void onNodeDisconnected(final DiscoveryNode node) {
        final Collection<JobExecutionContext> contexts = jobContextService.getContextsByCoordinatorNode(node.getId());
        if (contexts.isEmpty()) {
            return;
        }

        threadPool.schedule(DELAY, ThreadPool.Names.GENERIC, new Runnable() {
            @Override
            public void run() {
                jobContextService.killJobs(Collections2.transform(contexts, JobExecutionContext.TO_ID));
            }
        });
    }
}
