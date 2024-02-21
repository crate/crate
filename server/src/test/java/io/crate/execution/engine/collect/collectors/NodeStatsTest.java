/*
 * Licensed to Crate.io GmbH ("Crate") under one or more contributor
 * license agreements.  See the NOTICE file distributed with this work for
 * additional information regarding copyright ownership.  Crate licenses
 * this file to you under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.  You may
 * obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.  See the
 * License for the specific language governing permissions and limitations
 * under the License.
 *
 * However, if you have executed another commercial license agreement
 * with Crate these terms will supersede the license and you may use the
 * software solely pursuant to the terms of the relevant commercial agreement.
 */

package io.crate.execution.engine.collect.collectors;

import static io.crate.testing.DiscoveryNodes.newNode;
import static io.crate.testing.TestingHelpers.createNodeContext;
import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.stream.Collectors;

import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.test.ESTestCase;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Answers;
import org.mockito.ArgumentCaptor;

import io.crate.analyze.OrderBy;
import io.crate.common.unit.TimeValue;
import io.crate.data.testing.BatchIteratorTester;
import io.crate.execution.dsl.phases.RoutedCollectPhase;
import io.crate.execution.engine.collect.stats.NodeStatsRequest;
import io.crate.execution.engine.collect.stats.NodeStatsResponse;
import io.crate.execution.engine.collect.stats.TransportNodeStatsAction;
import io.crate.execution.support.ActionExecutor;
import io.crate.expression.InputFactory;
import io.crate.expression.symbol.Literal;
import io.crate.expression.symbol.Symbol;
import io.crate.metadata.CoordinatorTxnCtx;
import io.crate.metadata.NodeContext;
import io.crate.metadata.Reference;
import io.crate.metadata.ReferenceIdent;
import io.crate.metadata.RowGranularity;
import io.crate.metadata.SimpleReference;
import io.crate.metadata.TransactionContext;
import io.crate.metadata.sys.SysNodesTableInfo;
import io.crate.types.DataTypes;

public class NodeStatsTest extends ESTestCase {

    private RoutedCollectPhase collectPhase;
    private Collection<DiscoveryNode> nodes = new HashSet<>();
    private TransportNodeStatsAction nodeStatsAction;
    private ActionExecutor<NodeStatsRequest, NodeStatsResponse> nodeStatesExecutor;
    private TransactionContext txnCtx = CoordinatorTxnCtx.systemTransactionContext();

    private Reference idRef;
    private Reference nameRef;
    private Reference hostnameRef;
    private NodeContext nodeCtx;

    @Before
    public void prepare() {
        nodeStatsAction = mock(TransportNodeStatsAction.class, Answers.RETURNS_MOCKS);
        nodeStatesExecutor = req -> nodeStatsAction.execute(req);

        idRef = new SimpleReference(
            new ReferenceIdent(SysNodesTableInfo.IDENT, SysNodesTableInfo.Columns.ID),
            RowGranularity.DOC,
            DataTypes.STRING,
            1,
            null
        );
        nameRef = new SimpleReference(
            new ReferenceIdent(SysNodesTableInfo.IDENT, SysNodesTableInfo.Columns.ID),
            RowGranularity.DOC,
            DataTypes.STRING,
            2,
            null
        );
        hostnameRef = new SimpleReference(
            new ReferenceIdent(SysNodesTableInfo.IDENT, SysNodesTableInfo.Columns.HOSTNAME),
            RowGranularity.DOC,
            DataTypes.STRING,
            3,
            null
        );
        collectPhase = mock(RoutedCollectPhase.class);
        when(collectPhase.where()).thenReturn(Literal.BOOLEAN_FALSE);

        nodes.add(newNode("nodeOne"));
        nodes.add(newNode("nodeTwo"));
        nodeCtx = createNodeContext();
    }

    @Test
    public void testNoRequestIfNotRequired() throws Exception {
        List<Symbol> toCollect = new ArrayList<>();
        toCollect.add(idRef);

        when(collectPhase.toCollect()).thenReturn(toCollect);

        var iterator = NodeStats.newInstance(
            nodeStatesExecutor,
            collectPhase,
            nodes,
            txnCtx,
            new InputFactory(nodeCtx)
        );
        iterator.loadNextBatch();

        // Because only id and name are selected, transportNodesStatsAction should be never used
        verifyNoMoreInteractions(nodeStatsAction);
    }

    @Test
    public void testRequestsAreIssued() throws Exception {
        List<Symbol> toCollect = new ArrayList<>();
        toCollect.add(idRef);
        toCollect.add(nameRef);
        when(collectPhase.toCollect()).thenReturn(toCollect);

        var iterator = NodeStats.newInstance(
            nodeStatesExecutor,
            collectPhase,
            nodes,
            txnCtx,
            new InputFactory(nodeCtx)
        );
        iterator.loadNextBatch();
        verifyNoMoreInteractions(nodeStatsAction);
    }

    @Test
    public void testRequestsIfRequired() throws Exception {
        List<Symbol> toCollect = new ArrayList<>();
        toCollect.add(idRef);
        toCollect.add(hostnameRef);

        when(collectPhase.toCollect()).thenReturn(toCollect);

        var iterator = NodeStats.newInstance(
            nodeStatesExecutor,
            collectPhase,
            nodes,
            txnCtx,
            new InputFactory(nodeCtx)
        );
        iterator.loadNextBatch();

        ArgumentCaptor<NodeStatsRequest> req = ArgumentCaptor.forClass(NodeStatsRequest.class);
        // Hostnames needs to be collected so requests need to be performed
        //noinspection unchecked
        verify(nodeStatsAction, times(2)).execute(req.capture());
        var capturedReq1 = req.getAllValues().get(0);
        var capturedReq2 = req.getAllValues().get(1);

        assertThat(req.getAllValues().stream()
                       .map(NodeStatsRequest::nodeId)
                       .sorted()
                       .collect(Collectors.toList())).containsExactly("nodeOne", "nodeTwo");
        assertThat(capturedReq1.timeout()).isEqualTo(TimeValue.timeValueMillis(3000L));
        assertThat(capturedReq2.timeout()).isEqualTo(TimeValue.timeValueMillis(3000L));

        verifyNoMoreInteractions(nodeStatsAction);
    }

    @Test
    public void testNodeStatsIteratorContrat() throws Exception {
        List<Symbol> toCollect = new ArrayList<>();
        toCollect.add(idRef);
        when(collectPhase.toCollect()).thenReturn(toCollect);
        when(collectPhase.where()).thenReturn(Literal.BOOLEAN_TRUE);
        when(collectPhase.orderBy()).thenReturn(new OrderBy(Collections.singletonList(idRef)));

        List<Object[]> expectedResult = Arrays.asList(
            new Object[]{"nodeOne"},
            new Object[]{"nodeTwo"}
        );
        var tester = BatchIteratorTester.forRows(() -> NodeStats.newInstance(
            nodeStatesExecutor,
            collectPhase,
            nodes,
            txnCtx,
            new InputFactory(nodeCtx)
        ));
        tester.verifyResultAndEdgeCaseBehaviour(expectedResult);
    }
}
