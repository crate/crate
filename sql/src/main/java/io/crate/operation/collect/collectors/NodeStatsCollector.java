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

package io.crate.operation.collect.collectors;

import io.crate.executor.transport.NodeStatsRequest;
import io.crate.executor.transport.NodeStatsResponse;
import io.crate.executor.transport.TransportNodeStatsAction;
import io.crate.metadata.RowCollectExpression;
import io.crate.operation.collect.CollectInputSymbolVisitor;
import io.crate.operation.collect.CrateCollector;
import io.crate.operation.collect.RowsTransformer;
import io.crate.operation.projectors.IterableRowEmitter;
import io.crate.operation.projectors.RowReceiver;
import io.crate.operation.reference.sys.node.DiscoveryNodeContext;
import io.crate.planner.node.dql.RoutedCollectPhase;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.node.DiscoveryNodes;
import org.elasticsearch.common.unit.TimeValue;

import javax.annotation.Nullable;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;

public class NodeStatsCollector implements CrateCollector {

    private final TransportNodeStatsAction transportStatTablesAction;
    private final RowReceiver rowReceiver;
    private final RoutedCollectPhase collectPhase;
    private final DiscoveryNodes nodes;
    private final CollectInputSymbolVisitor<RowCollectExpression<?, ?>> inputSymbolVisitor;
    private IterableRowEmitter emitter;

    public NodeStatsCollector(TransportNodeStatsAction transportStatTablesAction,
                              RowReceiver rowReceiver,
                              RoutedCollectPhase collectPhase,
                              DiscoveryNodes nodes,
                              CollectInputSymbolVisitor<RowCollectExpression<?, ?>> inputSymbolVisitor) {
        this.transportStatTablesAction = transportStatTablesAction;
        this.rowReceiver = rowReceiver;
        this.collectPhase = collectPhase;
        this.nodes = nodes;
        this.inputSymbolVisitor = inputSymbolVisitor;
    }

    @Override
    public void doCollect() {
        prepareCollect();
        emitter.run();
    }

    private void prepareCollect() {
        final List<DiscoveryNodeContext> discoveryNodeContexts = new ArrayList<>(nodes.size());
        final CountDownLatch counter = new CountDownLatch(nodes.size());
        for (DiscoveryNode node : nodes) {
            transportStatTablesAction.execute(node.id(),
                    new NodeStatsRequest(node.id(), null),
                    new ActionListener<NodeStatsResponse>() {
                        @Override
                        public void onResponse(NodeStatsResponse response) {
                            discoveryNodeContexts.add(response.discoveryNodeContext());
                            counter.countDown();
                        }

                        @Override
                        public void onFailure(Throwable t) {
                            discoveryNodeContexts.add(new DiscoveryNodeContext());
                            counter.countDown();
                        }
                    }, TimeValue.timeValueSeconds(5));
        }
        try {
            counter.await();
        } catch (InterruptedException e) {
            rowReceiver.fail(e);
        }

        this.emitter = new IterableRowEmitter(
                rowReceiver,
                RowsTransformer.toRowsIterable(inputSymbolVisitor, collectPhase, discoveryNodeContexts)
        );
    }

    @Override
    public void kill(@Nullable Throwable throwable) {
        emitter.kill(throwable);
    }
}
