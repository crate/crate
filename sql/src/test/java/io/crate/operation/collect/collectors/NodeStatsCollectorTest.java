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

import io.crate.analyze.WhereClause;
import io.crate.analyze.symbol.Symbol;
import io.crate.executor.transport.NodeStatsRequest;
import io.crate.executor.transport.TransportNodeStatsAction;
import io.crate.metadata.Reference;
import io.crate.metadata.ReferenceIdent;
import io.crate.metadata.RowGranularity;
import io.crate.metadata.sys.SysNodesTableInfo;
import io.crate.operation.InputFactory;
import io.crate.operation.projectors.RowReceiver;
import io.crate.planner.node.dql.RoutedCollectPhase;
import io.crate.test.integration.CrateUnitTest;
import io.crate.types.DataTypes;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.common.unit.TimeValue;
import org.junit.Before;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;

import static io.crate.testing.DiscoveryNodes.newNode;
import static io.crate.testing.TestingHelpers.getFunctions;
import static org.mockito.Mockito.*;

public class NodeStatsCollectorTest extends CrateUnitTest {


    private RoutedCollectPhase collectPhase;
    private Collection<DiscoveryNode> nodes = new HashSet<>();
    private TransportNodeStatsAction transportNodeStatsAction = mock(TransportNodeStatsAction.class);
    private RowReceiver rowReceiver = mock(RowReceiver.class);

    private Reference idRef;
    private Reference nameRef;
    private Reference hostnameRef;

    @Before
    public void prepare() {
        idRef = new Reference(
            new ReferenceIdent(SysNodesTableInfo.IDENT, SysNodesTableInfo.Columns.ID),
            RowGranularity.DOC, DataTypes.STRING);
        nameRef = new Reference(
            new ReferenceIdent(SysNodesTableInfo.IDENT, SysNodesTableInfo.Columns.ID),
            RowGranularity.DOC, DataTypes.STRING);
        hostnameRef = new Reference(
            new ReferenceIdent(SysNodesTableInfo.IDENT, SysNodesTableInfo.Columns.HOSTNAME),
            RowGranularity.DOC, DataTypes.STRING);
        collectPhase = mock(RoutedCollectPhase.class);
        when(collectPhase.whereClause()).thenReturn(WhereClause.NO_MATCH);

        nodes.add(newNode("nodeOne"));
        nodes.add(newNode("nodeTwo"));
    }

    @Test
    public void testNoRequestIfNotRequired() {
        List<Symbol> toCollect = new ArrayList<>();
        toCollect.add(idRef);

        when(collectPhase.toCollect()).thenReturn(toCollect);

        NodeStatsCollector collector = new NodeStatsCollector(
            transportNodeStatsAction,
            rowReceiver,
            collectPhase,
            nodes,
            new InputFactory(getFunctions())
        );
        collector.doCollect();

        // Because only id and name are selected, transportNodesStatsAction should be never used
        verifyNoMoreInteractions(transportNodeStatsAction);

        toCollect.add(nameRef);
        collector.doCollect();
        verifyNoMoreInteractions(transportNodeStatsAction);
    }

    @Test
    public void testRequestsIfRequired() {
        List<Symbol> toCollect = new ArrayList<>();
        toCollect.add(idRef);
        toCollect.add(hostnameRef);

        when(collectPhase.toCollect()).thenReturn(toCollect);

        NodeStatsCollector collector = new NodeStatsCollector(
            transportNodeStatsAction,
            rowReceiver,
            collectPhase,
            nodes,
            new InputFactory(getFunctions())
        );
        collector.doCollect();

        // Hostnames needs to be collected so requests need to be performed
        verify(transportNodeStatsAction).execute(eq("nodeOne"), any(NodeStatsRequest.class), any(ActionListener.class),
            eq(TimeValue.timeValueMillis(3000L)));
        verify(transportNodeStatsAction).execute(eq("nodeTwo"), any(NodeStatsRequest.class), any(ActionListener.class),
            eq(TimeValue.timeValueMillis(3000L)));
        verifyNoMoreInteractions(transportNodeStatsAction);
    }
}
