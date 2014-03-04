/*
 * Licensed to CRATE Technology GmbH ("Crate") under one or more contributor
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

package io.crate.executor.transport.merge;

import com.google.common.util.concurrent.SettableFuture;
import io.crate.executor.transport.distributed.DistributedRequestContextManager;
import io.crate.executor.transport.distributed.DistributedResultRequest;
import io.crate.metadata.FunctionIdent;
import io.crate.metadata.FunctionImplementation;
import io.crate.metadata.Functions;
import io.crate.operator.operations.DownstreamOperationFactory;
import io.crate.operator.operations.merge.DownstreamOperation;
import io.crate.planner.node.dql.MergeNode;
import io.crate.planner.projection.Projection;
import io.crate.planner.projection.TopNProjection;
import io.crate.planner.symbol.InputColumn;
import io.crate.planner.symbol.Symbol;
import org.apache.lucene.util.BytesRef;
import org.cratedb.DataType;
import org.cratedb.Streamer;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.common.inject.AbstractModule;
import org.elasticsearch.common.inject.ModulesBuilder;
import org.elasticsearch.common.inject.multibindings.MapBinder;
import org.elasticsearch.common.io.stream.BytesStreamInput;
import org.elasticsearch.common.io.stream.BytesStreamOutput;
import org.junit.Before;
import org.junit.Test;

import java.util.Arrays;
import java.util.UUID;

import static junit.framework.Assert.assertNotNull;
import static junit.framework.Assert.assertTrue;
import static org.hamcrest.core.Is.is;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertThat;

public class DistributedResultRequestTest {

    private Functions functions;
    private MergeNode dummyMergeNode;
    private UUID contextId;
    private Object[][] rows;

    class EmptyFunctionsModule extends AbstractModule {

        @Override
        protected void configure() {
            MapBinder.newMapBinder(binder(), FunctionIdent.class, FunctionImplementation.class);
        }
    }

    @Before
    public void setUp() {
        functions = new ModulesBuilder().add(new EmptyFunctionsModule()).createInjector().getInstance(Functions.class);

        rows = new Object[3][];
        rows[0] = new Object[] {1, new BytesRef("Arthur")};
        rows[1] = new Object[] {2, new BytesRef("Trillian")};
        rows[2] = new Object[] {3, new BytesRef("Marvin")};

        contextId = UUID.randomUUID();
        dummyMergeNode = new MergeNode("dummy", 1);
        dummyMergeNode.contextId(contextId);
        dummyMergeNode.inputTypes(Arrays.asList(DataType.INTEGER, DataType.STRING));
    }

    @Test
    public void testSerializationWithLateContext() throws Exception {
        // sender
        Streamer<?>[] streamers = new Streamer[2];
        streamers[0] = DataType.INTEGER.streamer();
        streamers[1] = DataType.STRING.streamer();

        Object[][] rows = new Object[3][];
        rows[0] = new Object[] {1, new BytesRef("Arthur")};
        rows[1] = new Object[] {2, new BytesRef("Trillian")};
        rows[2] = new Object[] {3, new BytesRef("Marvin")};

        DistributedResultRequest requestSender = new DistributedResultRequest(contextId, streamers);
        requestSender.rows(rows);

        BytesStreamOutput streamOutput = new BytesStreamOutput();
        requestSender.writeTo(streamOutput);
        // -- end sender


        // receiver
        DistributedRequestContextManager contextManager =
                new DistributedRequestContextManager(new DummyDownstreamOperationFactory(rows), functions);
        BytesStreamInput streamInput = new BytesStreamInput(streamOutput.bytes());
        DistributedResultRequest requestReceiver = new DistributedResultRequest(contextManager);
        requestReceiver.readFrom(streamInput);

        assertFalse(requestReceiver.rowsRead());
        assertNotNull(requestReceiver.memoryStream());
        assertTrue(requestReceiver.memoryStream().size() > 0);


        contextManager.addToContext(requestReceiver);
        final SettableFuture<Object[][]> result = SettableFuture.create();

        contextManager.createContext(dummyMergeNode, new ActionListener<NodeMergeResponse>() {
            @Override
            public void onResponse(NodeMergeResponse nodeMergeResponse) {
                result.set(nodeMergeResponse.rows());
            }

            @Override
            public void onFailure(Throwable e) {
            }
        });

        Object[][] receivedRows = result.get();
        assertThat(receivedRows.length, is(3));
        for (int i = 0; i < rows.length; i++) {
            assertTrue(Arrays.equals(rows[i], receivedRows[i]));
        }
    }

    @Test
    public void testSerializationWithContext() throws Exception {
        UUID contextId = UUID.randomUUID();
        MergeNode dummyMergeNode = new MergeNode();
        dummyMergeNode.contextId(contextId);
        dummyMergeNode.inputTypes(Arrays.asList(DataType.INTEGER, DataType.STRING));
        TopNProjection topNProjection = new TopNProjection(10, 0);
        topNProjection.outputs(Arrays.<Symbol>asList(new InputColumn(0), new InputColumn(1)));
        dummyMergeNode.projections(Arrays.<Projection>asList(topNProjection));

        DistributedRequestContextManager contextManager =
                new DistributedRequestContextManager(new DummyDownstreamOperationFactory(rows), functions);

        contextManager.createContext(dummyMergeNode, new NoopActionListener());

        Streamer<?>[] streamers = new Streamer[2];
        streamers[0] = DataType.INTEGER.streamer();
        streamers[1] = DataType.STRING.streamer();

        DistributedResultRequest requestSender = new DistributedResultRequest(contextId, streamers);
        requestSender.rows(rows);

        BytesStreamOutput streamOutput = new BytesStreamOutput();
        requestSender.writeTo(streamOutput);

        BytesStreamInput streamInput = new BytesStreamInput(streamOutput.bytes());

        DistributedResultRequest requestReceiver = new DistributedResultRequest(contextManager);
        requestReceiver.readFrom(streamInput);


        Object[][] receiverRows = requestReceiver.rows();
        for (int i = 0; i < rows.length; i++) {
            assertTrue(Arrays.equals(rows[i], receiverRows[i]));
        }
    }

    class NoopActionListener implements ActionListener<NodeMergeResponse> {

        @Override
        public void onResponse(NodeMergeResponse nodeMergeResponse) {
        }

        @Override
        public void onFailure(Throwable e) {
        }
    }

    class DummyDownstreamOperationFactory implements DownstreamOperationFactory<MergeNode> {

        private final Object[][] result;

        DummyDownstreamOperationFactory(Object[][] result) {
            this.result = result;
        }

        @Override
        public DownstreamOperation create(final MergeNode node) {
            return new DownstreamOperation() {
                @Override
                public boolean addRows(Object[][] rows) {
                    return true;
                }

                @Override
                public int numUpstreams() {
                    return node.numUpstreams();
                }

                @Override
                public Object[][] result() {
                    return result;
                }
            };
        }
    }
}
