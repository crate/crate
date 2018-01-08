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

import io.crate.analyze.OrderBy;
import io.crate.analyze.WhereClause;
import io.crate.analyze.symbol.Symbol;
import io.crate.data.BatchIterator;
import io.crate.executor.transport.NodeStatsRequest;
import io.crate.executor.transport.TransportNodeStatsAction;
import io.crate.metadata.Reference;
import io.crate.metadata.ReferenceIdent;
import io.crate.metadata.RowGranularity;
import io.crate.metadata.sys.SysNodesTableInfo;
import io.crate.operation.InputFactory;
import io.crate.execution.dsl.phases.RoutedCollectPhase;
import io.crate.test.integration.CrateUnitTest;
import io.crate.testing.BatchIteratorTester;
import io.crate.types.DataTypes;
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.common.unit.TimeValue;
import org.junit.Before;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeoutException;

import static io.crate.testing.DiscoveryNodes.newNode;
import static io.crate.testing.TestingHelpers.getFunctions;
import static org.mockito.Mockito.any;
import static org.mockito.Mockito.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;

public class NodeStatsIteratorTest extends CrateUnitTest {

    private RoutedCollectPhase collectPhase;
    private Collection<DiscoveryNode> nodes = new HashSet<>();
    private TransportNodeStatsAction transportNodeStatsAction = mock(TransportNodeStatsAction.class);

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
    public void testNoRequestIfNotRequired() throws InterruptedException, ExecutionException, TimeoutException {
        List<Symbol> toCollect = new ArrayList<>();
        toCollect.add(idRef);

        when(collectPhase.toCollect()).thenReturn(toCollect);

        BatchIterator iterator = NodeStatsIterator.newInstance(
            transportNodeStatsAction,
            collectPhase,
            nodes,
            new InputFactory(getFunctions())
        );
        iterator.loadNextBatch();

        // Because only id and name are selected, transportNodesStatsAction should be never used
        verifyNoMoreInteractions(transportNodeStatsAction);
    }

    @Test
    public void testRequestsAreIssued() throws InterruptedException, ExecutionException, TimeoutException {
        List<Symbol> toCollect = new ArrayList<>();
        toCollect.add(idRef);
        toCollect.add(nameRef);
        when(collectPhase.toCollect()).thenReturn(toCollect);

        BatchIterator iterator = NodeStatsIterator.newInstance(
            transportNodeStatsAction,
            collectPhase,
            nodes,
            new InputFactory(getFunctions())
        );
        iterator.loadNextBatch();
        verifyNoMoreInteractions(transportNodeStatsAction);
    }

    @Test
    public void testRequestsIfRequired() throws InterruptedException, ExecutionException, TimeoutException {
        List<Symbol> toCollect = new ArrayList<>();
        toCollect.add(idRef);
        toCollect.add(hostnameRef);

        when(collectPhase.toCollect()).thenReturn(toCollect);

        BatchIterator iterator = NodeStatsIterator.newInstance(
            transportNodeStatsAction,
            collectPhase,
            nodes,
            new InputFactory(getFunctions())
        );
        iterator.loadNextBatch();

        // Hostnames needs to be collected so requests need to be performed
        verify(transportNodeStatsAction).execute(eq("nodeOne"), any(NodeStatsRequest.class), any(ActionListener.class),
            eq(TimeValue.timeValueMillis(3000L)));
        verify(transportNodeStatsAction).execute(eq("nodeTwo"), any(NodeStatsRequest.class), any(ActionListener.class),
            eq(TimeValue.timeValueMillis(3000L)));
        verifyNoMoreInteractions(transportNodeStatsAction);
    }

    @Test
    public void testNodeStatsIteratorContrat() throws Exception {
        List<Symbol> toCollect = new ArrayList<>();
        toCollect.add(idRef);
        when(collectPhase.toCollect()).thenReturn(toCollect);
        when(collectPhase.whereClause()).thenReturn(WhereClause.MATCH_ALL);
        when(collectPhase.orderBy()).thenReturn(new OrderBy(Collections.singletonList(idRef),
            new boolean[]{false}, new Boolean[]{true}));

        List<Object[]> expectedResult = Arrays.asList(
            new Object[]{new BytesRef("nodeOne")},
            new Object[]{new BytesRef("nodeTwo")}
        );
        BatchIteratorTester tester = new BatchIteratorTester(() -> NodeStatsIterator.newInstance(
            transportNodeStatsAction,
            collectPhase,
            nodes,
            new InputFactory(getFunctions())
        ));
        tester.verifyResultAndEdgeCaseBehaviour(expectedResult);
    }
}
