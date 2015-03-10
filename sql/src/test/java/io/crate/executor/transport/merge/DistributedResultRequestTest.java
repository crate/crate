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

import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.SettableFuture;
import io.crate.Streamer;
import io.crate.breaker.RamAccountingContext;
import io.crate.core.collections.ArrayBucket;
import io.crate.core.collections.Bucket;
import io.crate.executor.transport.distributed.DistributedRequestContextManager;
import io.crate.executor.transport.distributed.DistributedResultRequest;
import io.crate.metadata.DynamicFunctionResolver;
import io.crate.metadata.FunctionIdent;
import io.crate.metadata.FunctionImplementation;
import io.crate.metadata.Functions;
import io.crate.operation.DownstreamOperation;
import io.crate.operation.DownstreamOperationFactory;
import io.crate.operation.collect.StatsTables;
import io.crate.planner.node.dql.MergeNode;
import io.crate.planner.projection.Projection;
import io.crate.planner.projection.TopNProjection;
import io.crate.planner.symbol.InputColumn;
import io.crate.planner.symbol.Symbol;
import io.crate.types.DataType;
import io.crate.types.DataTypes;
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.common.breaker.CircuitBreaker;
import org.elasticsearch.common.breaker.NoopCircuitBreaker;
import org.elasticsearch.common.inject.AbstractModule;
import org.elasticsearch.common.inject.ModulesBuilder;
import org.elasticsearch.common.inject.multibindings.MapBinder;
import org.elasticsearch.common.io.stream.BytesStreamInput;
import org.elasticsearch.common.io.stream.BytesStreamOutput;
import org.elasticsearch.common.settings.ImmutableSettings;
import org.elasticsearch.node.settings.NodeSettingsService;
import org.junit.Before;
import org.junit.Test;

import java.util.Arrays;
import java.util.UUID;

import static io.crate.testing.TestingHelpers.isRow;
import static junit.framework.TestCase.assertTrue;
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.core.Is.is;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertThat;
import static org.mockito.Mockito.mock;

public class DistributedResultRequestTest {

    private Functions functions;
    private MergeNode dummyMergeNode;
    private UUID contextId;
    private Object[][] rows;

    class EmptyFunctionsModule extends AbstractModule {

        @Override
        protected void configure() {
            MapBinder.newMapBinder(binder(), FunctionIdent.class, FunctionImplementation.class);
            MapBinder.newMapBinder(binder(), String.class, DynamicFunctionResolver.class);
        }
    }

    @Before
    public void setUp() {
        functions = new ModulesBuilder().add(new EmptyFunctionsModule()).createInjector().getInstance(Functions.class);

        rows = new Object[3][];
        rows[0] = new Object[]{1, new BytesRef("Arthur")};
        rows[1] = new Object[]{2, new BytesRef("Trillian")};
        rows[2] = new Object[]{3, new BytesRef("Marvin")};

        contextId = UUID.randomUUID();
        dummyMergeNode = new MergeNode("dummy", 1);
        dummyMergeNode.contextId(contextId);
        dummyMergeNode.inputTypes(Arrays.<DataType>asList(DataTypes.INTEGER, DataTypes.STRING));
    }

    @Test
    public void testSerializationWithLateContext() throws Exception {
        // sender
        Streamer<?>[] streamers = new Streamer[2];
        streamers[0] = DataTypes.INTEGER.streamer();
        streamers[1] = DataTypes.STRING.streamer();

        Object[][] rows = new Object[3][];
        rows[0] = new Object[]{1, new BytesRef("Arthur")};
        rows[1] = new Object[]{2, new BytesRef("Trillian")};
        rows[2] = new Object[]{3, new BytesRef("Marvin")};

        DistributedResultRequest requestSender = new DistributedResultRequest(contextId, streamers);
        requestSender.rows(new ArrayBucket(rows));

        BytesStreamOutput streamOutput = new BytesStreamOutput();
        requestSender.writeTo(streamOutput);
        // -- end sender


        // receiver
        DistributedRequestContextManager contextManager =
                new DistributedRequestContextManager(new DummyDownstreamOperationFactory(new ArrayBucket(rows)), functions,
                        new StatsTables(ImmutableSettings.EMPTY, mock(NodeSettingsService.class)),
                        new NoopCircuitBreaker(CircuitBreaker.Name.FIELDDATA));
        BytesStreamInput streamInput = new BytesStreamInput(streamOutput.bytes());
        DistributedResultRequest requestReceiver = new DistributedResultRequest(contextManager);
        requestReceiver.readFrom(streamInput);

        assertFalse(requestReceiver.rowsCanBeRead());
        assertThat(requestReceiver.rows().size(), is(3));


        contextManager.addToContext(requestReceiver);
        final SettableFuture<Bucket> result = SettableFuture.create();

        contextManager.createContext(dummyMergeNode, new ActionListener<NodeMergeResponse>() {
            @Override
            public void onResponse(NodeMergeResponse nodeMergeResponse) {
                result.set(nodeMergeResponse.rows());
            }

            @Override
            public void onFailure(Throwable e) {
            }
        });

        Bucket receivedRows = result.get();
        assertThat(receivedRows.size(), is(3));
        assertThat(receivedRows, contains(
                isRow(1, "Arthur"),
                isRow(2, "Trillian"),
                isRow(3, "Marvin")

        ));
    }

    @Test
    public void testSerializationWithContext() throws Exception {
        UUID contextId = UUID.randomUUID();
        MergeNode dummyMergeNode = new MergeNode();
        dummyMergeNode.contextId(contextId);
        dummyMergeNode.inputTypes(Arrays.<DataType>asList(DataTypes.INTEGER, DataTypes.STRING));
        TopNProjection topNProjection = new TopNProjection(10, 0);
        topNProjection.outputs(Arrays.<Symbol>asList(
                new InputColumn(0, DataTypes.INTEGER),
                new InputColumn(1, DataTypes.INTEGER)));
        dummyMergeNode.projections(Arrays.<Projection>asList(topNProjection));

        DistributedRequestContextManager contextManager =
                new DistributedRequestContextManager(new DummyDownstreamOperationFactory(new ArrayBucket(rows)), functions,
                        new StatsTables(ImmutableSettings.EMPTY, mock(NodeSettingsService.class)),
                        new NoopCircuitBreaker(CircuitBreaker.Name.FIELDDATA));

        contextManager.createContext(dummyMergeNode, new NoopActionListener());

        Streamer<?>[] streamers = new Streamer[2];
        streamers[0] = DataTypes.INTEGER.streamer();
        streamers[1] = DataTypes.STRING.streamer();

        DistributedResultRequest requestSender = new DistributedResultRequest(contextId, streamers);
        requestSender.rows(new ArrayBucket(rows));

        BytesStreamOutput streamOutput = new BytesStreamOutput();
        requestSender.writeTo(streamOutput);

        BytesStreamInput streamInput = new BytesStreamInput(streamOutput.bytes());


        DistributedResultRequest requestReceiver = new DistributedResultRequest(contextManager);
        requestReceiver.readFrom(streamInput);
        assertThat(requestReceiver.rows().size(), is(3));

        contextManager.addToContext(requestReceiver);
        assertTrue(requestReceiver.rowsCanBeRead());

        Bucket receiverRows = requestReceiver.rows();

        assertThat(receiverRows, contains(
                isRow(1, "Arthur"),
                isRow(2, "Trillian"),
                isRow(3, "Marvin")

        ));
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

        private final SettableFuture<Bucket> futureResult = SettableFuture.create();
        private final Bucket result;

        DummyDownstreamOperationFactory(Bucket result) {
            this.result = result;
        }

        @Override
        public DownstreamOperation create(final MergeNode node, RamAccountingContext ramAccountingContext) {
            return new DownstreamOperation() {
                @Override
                public boolean addRows(Bucket rows) {
                    return true;
                }

                @Override
                public int numUpstreams() {
                    return node.numUpstreams();
                }

                @Override
                public void finished() {
                    futureResult.set(result);
                }

                @Override
                public ListenableFuture<Bucket> result() {
                    return futureResult;
                }
            };
        }
    }
}
