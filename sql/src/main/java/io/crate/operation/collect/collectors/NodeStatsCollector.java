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
import io.crate.metadata.ReferenceIdent;
import io.crate.operation.collect.CrateCollector;
import io.crate.operation.collect.RowsTransformer;
import io.crate.operation.projectors.RepeatHandle;
import io.crate.operation.projectors.RowReceiver;
import io.crate.planner.node.dql.CollectPhase;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.node.DiscoveryNodes;
import org.elasticsearch.common.unit.TimeValue;

import javax.annotation.Nullable;
import java.util.List;

public class NodeStatsCollector implements CrateCollector {

    private final TransportNodeStatsAction transportStatTablesAction;
    private final RowReceiver rowReceiver;
    private final CollectPhase collectPhase;
    private final DiscoveryNodes nodes;

    public NodeStatsCollector(TransportNodeStatsAction transportStatTablesAction,
                              RowReceiver rowReceiver,
                              CollectPhase collectPhase,
                              DiscoveryNodes nodes) {
        this.transportStatTablesAction = transportStatTablesAction;
        this.rowReceiver = rowReceiver;
        this.collectPhase = collectPhase;
        this.nodes = nodes;
    }

    @Override
    public void doCollect() {
        for (DiscoveryNode node : nodes) {
            transportStatTablesAction.execute(node.id(),
                    new NodeStatsRequest(node.id(), null),
                    new ActionListener<NodeStatsResponse>() {
                        @Override
                        public void onResponse(NodeStatsResponse response) {

                        }

                        @Override
                        public void onFailure(Throwable t) {
                            rowReceiver.fail(t);
                        }
                    }, TimeValue.timeValueHours(1));
        }
//        Iterable SysInfo

//        RowsTransformer.toRowsIterable()
//        rowReceiver.finish(RepeatHandle.UNSUPPORTED);
    }

    @Override
    public void kill(@Nullable Throwable throwable) {
    }

}
