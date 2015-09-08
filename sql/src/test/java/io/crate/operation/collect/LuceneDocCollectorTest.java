/*
 * Licensed to CRATE Technology GmbH ("Crate") under one or more contributor
 * license agreements.  See the NOTICE file distributed with this work for
 * additional information regarding copyright ownership.  Crate licenses
 * this file to you under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.  You may
 * obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
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

package io.crate.operation.collect;

import com.google.common.collect.ImmutableList;
import io.crate.action.sql.SQLBulkRequest;
import io.crate.analyze.OrderBy;
import io.crate.analyze.WhereClause;
import io.crate.breaker.RamAccountingContext;
import io.crate.core.collections.Bucket;
import io.crate.core.collections.Row;
import io.crate.integrationtests.SQLTransportIntegrationTest;
import io.crate.jobs.JobContextService;
import io.crate.jobs.JobExecutionContext;
import io.crate.metadata.*;
import io.crate.operation.operator.EqOperator;
import io.crate.operation.projectors.ProjectionToProjectorVisitor;
import io.crate.operation.projectors.RowReceiver;
import io.crate.operation.projectors.ShardProjectorChain;
import io.crate.operation.scalar.arithmetic.MultiplyFunction;
import io.crate.planner.RowGranularity;
import io.crate.planner.distribution.DistributionType;
import io.crate.planner.node.dql.CollectPhase;
import io.crate.planner.projection.Projection;
import io.crate.planner.symbol.Function;
import io.crate.planner.symbol.Literal;
import io.crate.planner.symbol.Reference;
import io.crate.planner.symbol.Symbol;
import io.crate.testing.CollectingRowReceiver;
import io.crate.testing.TestingHelpers;
import io.crate.types.DataType;
import io.crate.types.DataTypes;
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.common.breaker.CircuitBreaker;
import org.elasticsearch.common.breaker.NoopCircuitBreaker;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.index.IndexService;
import org.elasticsearch.indices.IndicesService;
import org.elasticsearch.test.ElasticsearchIntegrationTest;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.CancellationException;

import static io.crate.testing.TestingHelpers.createReference;
import static org.hamcrest.Matchers.*;
import static org.hamcrest.core.Is.is;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

@ElasticsearchIntegrationTest.ClusterScope(scope = ElasticsearchIntegrationTest.Scope.SUITE, numDataNodes = 1)
public class LuceneDocCollectorTest extends SQLTransportIntegrationTest {

    private final static Integer PAGE_SIZE = 20;
    private final static String INDEX_NAME = "countries";
    // use higher value here to be sure multiple segment reader exists during collect (not only 1)
    private final static Integer NUMBER_OF_DOCS = 10_000;
    private OrderBy orderBy;
    private JobContextService jobContextService;
    private ShardCollectService shardCollectService;

    private CollectingRowReceiver rowReceiver = new CollectingRowReceiver();

    private static final RamAccountingContext RAM_ACCOUNTING_CONTEXT =
            new RamAccountingContext("dummy", new NoopCircuitBreaker(CircuitBreaker.Name.FIELDDATA));
    private JobCollectContext jobCollectContext;

    @Rule
    public ExpectedException expectedException = ExpectedException.none();

    @Before
    public void prepare() throws Exception{
        execute("create table \""+INDEX_NAME+ "\" (" +
                " continent string, " +
                " countryName string," +
                " population integer" +
                ") clustered into 1 shards with (number_of_replicas=0)");
        refresh();
        generateData();
        IndicesService instanceFromNode = internalCluster().getDataNodeInstance(IndicesService.class);
        IndexService indexService = instanceFromNode.indexServiceSafe(INDEX_NAME);

        shardCollectService = indexService.shardInjectorSafe(0).getInstance(ShardCollectService.class);
        jobContextService = indexService.shardInjectorSafe(0).getInstance(JobContextService.class);

        ReferenceIdent ident = new ReferenceIdent(new TableIdent("doc", "countries"), "countryName");
        Reference ref = new Reference(new ReferenceInfo(ident, RowGranularity.DOC, DataTypes.STRING));
        orderBy = new OrderBy(ImmutableList.of((Symbol) ref), new boolean[]{false}, new Boolean[]{false});
    }

    @After
    public void closeContext() throws Exception {
        if (jobCollectContext != null) {
            jobCollectContext.close();
        }
    }

    private byte[] generateRowSource(String continent, String countryName, Integer population) throws IOException {
        return XContentFactory.jsonBuilder()
                .startObject()
                .field("continent", continent)
                .field("countryName", countryName)
                .field("population", population)
                .endObject()
                .bytes().toBytes();
    }

    public void generateData() throws Exception {
        BulkRequest bulkRequest = new BulkRequest();
        for (int i=0; i < NUMBER_OF_DOCS; i++) {
            IndexRequest indexRequest = new IndexRequest(INDEX_NAME, "default", String.valueOf(i));
            if (i == 0) {
                indexRequest.source(generateRowSource("Europe", "Germany", i));
            } else if (i == 1) {
                indexRequest.source(generateRowSource("Europe", "Austria", i));
            } else if (i >= 2 && i <=4) {
                indexRequest.source(generateRowSource("Europe", null, i));
            } else {
                indexRequest.source(generateRowSource("America", "USA", i));
            }
            bulkRequest.add(indexRequest);
        }
        BulkResponse response = client().bulk(bulkRequest).actionGet();
        assertFalse(response.hasFailures());
        refresh();
    }

    private LuceneDocCollector createDocCollector(OrderBy orderBy, Integer limit, List<Symbol> toCollect) throws Exception{
        return createDocCollector(orderBy, limit, toCollect, WhereClause.MATCH_ALL, PAGE_SIZE);
    }

    private LuceneDocCollector createDocCollector(OrderBy orderBy, Integer limit, List<Symbol> toCollect, WhereClause whereClause, int pageSize, RowReceiver projector) throws Exception{
        UUID jobId = UUID.randomUUID();
        CollectPhase node = new CollectPhase(
                jobId,
                0,
                "collect",
                mock(Routing.class),
                RowGranularity.DOC,
                toCollect,
                ImmutableList.<Projection>of(),
                whereClause,
                DistributionType.BROADCAST
        );
        node.orderBy(orderBy);
        node.limit(limit);

        ShardProjectorChain projectorChain = mock(ShardProjectorChain.class);
        when(projectorChain.newShardDownstreamProjector(any(ProjectionToProjectorVisitor.class))).thenReturn(projector);

        JobExecutionContext.Builder builder = jobContextService.newBuilder(jobId);
        jobCollectContext = new JobCollectContext(
                jobId, node, mock(MapSideDataCollectOperation.class), RAM_ACCOUNTING_CONTEXT, projector);
        builder.addSubContext(node.executionPhaseId(), jobCollectContext);
        jobContextService.createContext(builder);
        LuceneDocCollector collector = (LuceneDocCollector)shardCollectService.getCollector(
                node, projectorChain, jobCollectContext, 0, pageSize);
        return collector;
    }

    private LuceneDocCollector createDocCollector(OrderBy orderBy, Integer limit, List<Symbol> toCollect, WhereClause whereClause, int pageSize) throws Exception{
        return createDocCollector(orderBy, limit, toCollect, whereClause, pageSize, rowReceiver);
    }


    @Test
    public void testLimitWithoutOrder() throws Exception{
        rowReceiver.rows.clear();
        LuceneDocCollector docCollector = createDocCollector(null, 15, orderBy.orderBySymbols());
        docCollector.doCollect();
        assertThat(rowReceiver.rows.size(), is(15));
    }

    @Test
    public void testOrderedWithLimit() throws Exception{
        rowReceiver.rows.clear();
        LuceneDocCollector docCollector = createDocCollector(orderBy, 15, orderBy.orderBySymbols());
        docCollector.doCollect();
        assertThat(rowReceiver.rows.size(), is(15));
        assertThat(((BytesRef) rowReceiver.rows.get(0)[0]).utf8ToString(), is("Austria") );
        assertThat(((BytesRef) rowReceiver.rows.get(1)[0]).utf8ToString(), is("Germany") );
        assertThat(((BytesRef) rowReceiver.rows.get(2)[0]).utf8ToString(), is("USA"));
        assertThat(((BytesRef) rowReceiver.rows.get(3)[0]).utf8ToString(), is("USA"));
    }

    @Test
    public void testOrderedPauseResume() throws Exception {
        CollectingRowReceiver rowReceiver = CollectingRowReceiver.withPauseAfter(3);
        ReferenceIdent populationIdent = new ReferenceIdent(new TableIdent("doc", "countries"), "population");
        Reference population = new Reference(new ReferenceInfo(populationIdent, RowGranularity.DOC, DataTypes.INTEGER));
        OrderBy orderBy = new OrderBy(ImmutableList.of((Symbol)population), new boolean[]{false}, new Boolean[]{false});
        LuceneDocCollector docCollector = createDocCollector(orderBy, 15, ImmutableList.<Symbol>of(population), WhereClause.MATCH_ALL, PAGE_SIZE, rowReceiver);
        docCollector.doCollect(); // start collection
        assertThat(rowReceiver.rows.size(), is(3));
        docCollector.resume(false); // continue
        assertThat(rowReceiver.rows.size(), is(15));
        for (int i = 0; i < rowReceiver.rows.size();  i++) {
            assertThat((Integer)rowReceiver.rows.get(i)[0], is(i));
        }
        rowReceiver.result(); // shouldn't timeout
    }

    @Test
    public void testPauseBeforeNextTopNSearch() throws Exception {
        CollectingRowReceiver rowReceiver = CollectingRowReceiver.withPauseAfter(5);
        ReferenceIdent populationIdent = new ReferenceIdent(new TableIdent("doc", "countries"), "population");
        Reference population = new Reference(new ReferenceInfo(populationIdent, RowGranularity.DOC, DataTypes.INTEGER));
        OrderBy orderBy = new OrderBy(ImmutableList.of((Symbol)population), new boolean[]{false}, new Boolean[]{false});
        LuceneDocCollector docCollector = createDocCollector(orderBy, 20, orderBy.orderBySymbols(), WhereClause.MATCH_ALL, 5, rowReceiver);
        docCollector.doCollect();
        docCollector.resume(false);

        Bucket bucket = rowReceiver.result();
        assertThat(bucket.size(), is(20));
        int i = 0;
        for (Row row : bucket) {
            assertThat((int) row.get(0), is(i));
            i++;
        }
    }

    @SuppressWarnings("unchecked")
    @Test
    public void testUnorderedPauseResume() throws Exception {
        int pauseAfter = NUMBER_OF_DOCS - 1;
        CollectingRowReceiver rowReceiver = CollectingRowReceiver.withPauseAfter(pauseAfter);
        ReferenceIdent populationIdent = new ReferenceIdent(new TableIdent("doc", "countries"), "population");
        Reference population = new Reference(new ReferenceInfo(populationIdent, RowGranularity.DOC, DataTypes.INTEGER));
        LuceneDocCollector docCollector = createDocCollector(null, null, ImmutableList.<Symbol>of(population), WhereClause.MATCH_ALL, PAGE_SIZE, rowReceiver);
        docCollector.doCollect();
        assertThat(rowReceiver.rows.size(), is(pauseAfter));
        docCollector.resume(false);

        Bucket bucket = rowReceiver.result();
        assertThat(bucket.size(), is(NUMBER_OF_DOCS));
        assertThat(new ArrayList<>(rowReceiver.rows), containsInAnyOrder(new ArrayList() {{
            for (int i = 0; i < NUMBER_OF_DOCS; i++) {
                add(equalTo(new Object[]{i}));
            }
        }}));
    }

    @SuppressWarnings("unchecked")
    @Test
    public void testAsynchronousResume() throws Exception {
        int pauseAfter = NUMBER_OF_DOCS - 100;
        CollectingRowReceiver projector = CollectingRowReceiver.withPauseAfter(pauseAfter);
        ReferenceIdent populationIdent = new ReferenceIdent(new TableIdent("doc", "countries"), "population");
        Reference population = new Reference(new ReferenceInfo(populationIdent, RowGranularity.DOC, DataTypes.INTEGER));
        LuceneDocCollector docCollector = createDocCollector(null, null, ImmutableList.<Symbol>of(population), WhereClause.MATCH_ALL, PAGE_SIZE, projector);
        docCollector.doCollect();
        assertThat(projector.rows.size(), is(pauseAfter));
        docCollector.resume(true);
        assertThat(projector.rows.size(), lessThan(NUMBER_OF_DOCS));

        projector.result();
        assertThat(projector.rows.size(), is(NUMBER_OF_DOCS));
        assertThat(new ArrayList<>(projector.rows), containsInAnyOrder(new ArrayList() {{
            for (int i = 0; i < NUMBER_OF_DOCS; i++) {
                add(equalTo(new Object[]{i}));
            }
        }}));
    }

    @Test
    public void testKillWhilePaused() throws Exception {
        CollectingRowReceiver projector = CollectingRowReceiver.withPauseAfter(5);
        LuceneDocCollector docCollector = createDocCollector(orderBy, 15, orderBy.orderBySymbols(), WhereClause.MATCH_ALL, PAGE_SIZE, projector);
        docCollector.doCollect();
        assertThat(projector.rows.size(), is(5));

        docCollector.kill(null);

        expectedException.expect(CancellationException.class);
        projector.result();
    }

    @Test
    public void testOrderedWithLimitHigherThanPageSize() throws Exception{
        rowReceiver.rows.clear();
        LuceneDocCollector docCollector = createDocCollector(orderBy, PAGE_SIZE + 5, orderBy.orderBySymbols());
        docCollector.doCollect();
        assertThat(rowReceiver.rows.size(), is(PAGE_SIZE + 5));
        assertThat(((BytesRef) rowReceiver.rows.get(0)[0]).utf8ToString(), is("Austria") );
        assertThat(((BytesRef) rowReceiver.rows.get(1)[0]).utf8ToString(), is("Germany") );
        assertThat(((BytesRef) rowReceiver.rows.get(2)[0]).utf8ToString(), is("USA") );
        assertThat(((BytesRef) rowReceiver.rows.get(3)[0]).utf8ToString(), is("USA") );
    }

    @Test
    public void testOrderedWithoutLimit() throws Exception {
        rowReceiver.rows.clear();
        LuceneDocCollector docCollector = createDocCollector(orderBy, null, orderBy.orderBySymbols(), WhereClause.MATCH_ALL, PAGE_SIZE);
        docCollector.doCollect();
        assertThat(rowReceiver.rows.size(), is(NUMBER_OF_DOCS));
        assertThat(((BytesRef) rowReceiver.rows.get(0)[0]).utf8ToString(), is("Austria") );
        assertThat(((BytesRef) rowReceiver.rows.get(1)[0]).utf8ToString(), is("Germany") );
        assertThat(((BytesRef) rowReceiver.rows.get(2)[0]).utf8ToString(), is("USA") );
        assertThat(rowReceiver.rows.get(NUMBER_OF_DOCS -1)[0], is(nullValue()));
    }

    @Test
    public void testOrderedNullsFirstWithoutLimit() throws Exception {
        rowReceiver.rows.clear();
        ReferenceIdent ident = new ReferenceIdent(new TableIdent("doc", "countries"), "countryName");
        Reference ref = new Reference(new ReferenceInfo(ident, RowGranularity.DOC, DataTypes.STRING));
        OrderBy orderBy = new OrderBy(ImmutableList.of((Symbol)ref), new boolean[]{false}, new Boolean[]{true});
        LuceneDocCollector docCollector = createDocCollector(orderBy, null, orderBy.orderBySymbols(), WhereClause.MATCH_ALL, PAGE_SIZE);
        docCollector.doCollect();
        assertThat(rowReceiver.rows.size(), is(NUMBER_OF_DOCS));
        assertThat(rowReceiver.rows.get(0)[0], is(nullValue()));
        assertThat(rowReceiver.rows.get(1)[0], is(nullValue()));
        assertThat(rowReceiver.rows.get(2)[0], is(nullValue()));
        assertThat(((BytesRef) rowReceiver.rows.get(3)[0]).utf8ToString(), is("Austria") );
        assertThat(((BytesRef) rowReceiver.rows.get(4)[0]).utf8ToString(), is("Germany") );
        assertThat(((BytesRef) rowReceiver.rows.get(5)[0]).utf8ToString(), is("USA") );
    }

    @Test
    public void testOrderedDescendingWithoutLimit() throws Exception {
        rowReceiver.rows.clear();
        ReferenceIdent ident = new ReferenceIdent(new TableIdent("doc", "countries"), "countryName");
        Reference ref = new Reference(new ReferenceInfo(ident, RowGranularity.DOC, DataTypes.STRING));
        OrderBy orderBy = new OrderBy(ImmutableList.of((Symbol)ref), new boolean[]{true}, new Boolean[]{false});
        LuceneDocCollector docCollector = createDocCollector(orderBy, null, orderBy.orderBySymbols(), WhereClause.MATCH_ALL, PAGE_SIZE);
        docCollector.doCollect();
        assertThat(rowReceiver.rows.size(), is(NUMBER_OF_DOCS));
        assertThat(rowReceiver.rows.get(NUMBER_OF_DOCS - 1)[0], is(nullValue()));
        assertThat(rowReceiver.rows.get(NUMBER_OF_DOCS - 2)[0], is(nullValue()));
        assertThat(rowReceiver.rows.get(NUMBER_OF_DOCS - 3)[0], is(nullValue()));
        assertThat(((BytesRef) rowReceiver.rows.get(NUMBER_OF_DOCS - 4)[0]).utf8ToString(), is("Austria") );
        assertThat(((BytesRef) rowReceiver.rows.get(NUMBER_OF_DOCS - 5)[0]).utf8ToString(), is("Germany") );
        assertThat(((BytesRef) rowReceiver.rows.get(NUMBER_OF_DOCS - 6)[0]).utf8ToString(), is("USA") );
    }

    @Test
    public void testOrderedDescendingNullsFirstWithoutLimit() throws Exception {
        rowReceiver.rows.clear();
        ReferenceIdent ident = new ReferenceIdent(new TableIdent("doc", "countries"), "countryName");
        Reference ref = new Reference(new ReferenceInfo(ident, RowGranularity.DOC, DataTypes.STRING));
        OrderBy orderBy = new OrderBy(ImmutableList.of((Symbol)ref), new boolean[]{true}, new Boolean[]{true});
        LuceneDocCollector docCollector = createDocCollector(orderBy, null, orderBy.orderBySymbols(), WhereClause.MATCH_ALL, PAGE_SIZE);
        docCollector.doCollect();
        assertThat(rowReceiver.rows.size(), is(NUMBER_OF_DOCS));
        assertThat(rowReceiver.rows.get(0)[0], is(nullValue()));
        assertThat(rowReceiver.rows.get(1)[0], is(nullValue()));
        assertThat(rowReceiver.rows.get(2)[0], is(nullValue()));
        assertThat(((BytesRef) rowReceiver.rows.get(NUMBER_OF_DOCS - 1)[0]).utf8ToString(), is("Austria") );
        assertThat(((BytesRef) rowReceiver.rows.get(NUMBER_OF_DOCS - 2)[0]).utf8ToString(), is("Germany") );
        assertThat(((BytesRef) rowReceiver.rows.get(NUMBER_OF_DOCS - 3)[0]).utf8ToString(), is("USA") );
    }

    @Test
    public void testOrderForNonSelected() throws Exception {
        rowReceiver.rows.clear();
        ReferenceIdent countriesIdent = new ReferenceIdent(new TableIdent("doc", "countries"), "countryName");
        Reference countries = new Reference(new ReferenceInfo(countriesIdent, RowGranularity.DOC, DataTypes.STRING));

        ReferenceIdent populationIdent = new ReferenceIdent(new TableIdent("doc", "countries"), "population");
        Reference population = new Reference(new ReferenceInfo(populationIdent, RowGranularity.DOC, DataTypes.INTEGER));

        OrderBy orderBy = new OrderBy(ImmutableList.of((Symbol)population), new boolean[]{true}, new Boolean[]{true});

        LuceneDocCollector docCollector = createDocCollector(orderBy, null, ImmutableList.of((Symbol)countries));
        docCollector.doCollect();
        assertThat(rowReceiver.rows.size(), is(NUMBER_OF_DOCS));
        assertThat(rowReceiver.rows.get(0).length, is(1));
        assertThat(((BytesRef) rowReceiver.rows.get(NUMBER_OF_DOCS - 6)[0]).utf8ToString(), is("USA") );
        assertThat(rowReceiver.rows.get(NUMBER_OF_DOCS - 5)[0], is(nullValue()));
        assertThat(rowReceiver.rows.get(NUMBER_OF_DOCS - 4)[0], is(nullValue()));
        assertThat(rowReceiver.rows.get(NUMBER_OF_DOCS - 3)[0], is(nullValue()));
        assertThat(((BytesRef) rowReceiver.rows.get(NUMBER_OF_DOCS - 2)[0]).utf8ToString(), is("Austria") );
        assertThat(((BytesRef) rowReceiver.rows.get(NUMBER_OF_DOCS - 1)[0]).utf8ToString(), is("Germany") );
    }

    @Test
    public void testOrderByScalar() throws Exception {
        rowReceiver.rows.clear();
        Reference population = createReference("population", DataTypes.INTEGER);
        Function scalarFunction = new Function(
                new FunctionInfo(
                        new FunctionIdent(MultiplyFunction.NAME, Arrays.<DataType>asList(DataTypes.INTEGER, DataTypes.INTEGER)),
                        DataTypes.LONG),
                Arrays.asList(population, Literal.newLiteral(-1))
        );

        OrderBy orderBy = new OrderBy(ImmutableList.of((Symbol)scalarFunction), new boolean[]{false}, new Boolean[]{false});
        LuceneDocCollector docCollector = createDocCollector(orderBy, null, ImmutableList.of((Symbol)population));
        docCollector.doCollect();
        assertThat(rowReceiver.rows.size(), is(NUMBER_OF_DOCS));
        assertThat(((Integer) rowReceiver.rows.get(NUMBER_OF_DOCS - 2)[0]), is(1) );
        assertThat(((Integer) rowReceiver.rows.get(NUMBER_OF_DOCS - 1)[0]), is(0) );
    }

    @Test
    public void testMultiOrdering() throws Exception {
        execute("create table test (x integer, y integer) clustered into 1 shards with (number_of_replicas=0)");
        ensureYellow();
        SQLBulkRequest request = new SQLBulkRequest("insert into test values (?, ?)",
                new Object[][]{
                    new Object[]{2, 3},
                    new Object[]{2, 1},
                    new Object[]{2, null},
                    new Object[]{1, null},
                    new Object[]{1, 2},
                    new Object[]{1, 1},
                    new Object[]{1, 0},
                    new Object[]{1, null}
                }
        );
        sqlExecutor.exec(request);
        execute("refresh table test");

        IndicesService instanceFromNode = internalCluster().getDataNodeInstance(IndicesService.class);
        IndexService indexService = instanceFromNode.indexServiceSafe("test");

        ShardCollectService shardCollectService = indexService.shardInjectorSafe(0).getInstance(ShardCollectService.class);
        JobContextService jobContextService = indexService.shardInjectorSafe(0).getInstance(JobContextService.class);

        ReferenceIdent xIdent = new ReferenceIdent(new TableIdent("doc", "test"), "x");
        Reference x = new Reference(new ReferenceInfo(xIdent, RowGranularity.DOC, DataTypes.INTEGER));

        ReferenceIdent yIdent = new ReferenceIdent(new TableIdent("doc", "test"), "y");
        Reference y = new Reference(new ReferenceInfo(yIdent, RowGranularity.DOC, DataTypes.INTEGER));

        OrderBy orderBy = new OrderBy(ImmutableList.<Symbol>of(x, y), new boolean[]{false, false}, new Boolean[]{false, false});

        CollectPhase node = new CollectPhase(
                UUID.randomUUID(),
                0,
                "collect",
                mock(Routing.class),
                RowGranularity.DOC,
                orderBy.orderBySymbols(),
                ImmutableList.<Projection>of(),
                WhereClause.MATCH_ALL,
                DistributionType.BROADCAST
        );
        node.orderBy(orderBy);

        JobExecutionContext.Builder builder = jobContextService.newBuilder(node.jobId());
        builder.addSubContext(node.executionPhaseId(),
                new JobCollectContext(node.jobId(), node, mock(MapSideDataCollectOperation.class), RAM_ACCOUNTING_CONTEXT, rowReceiver));
        jobContextService.createContext(builder);

        ShardProjectorChain projectorChain = mock(ShardProjectorChain.class);
        when(projectorChain.newShardDownstreamProjector(any(ProjectionToProjectorVisitor.class))).thenReturn(rowReceiver);

        JobCollectContext jobCollectContext = jobContextService.getContext(node.jobId()).getSubContext(node.executionPhaseId());
        LuceneDocCollector collector = (LuceneDocCollector)shardCollectService.getCollector(
                node, projectorChain, jobCollectContext, 0, PAGE_SIZE);
        collector.doCollect();
        jobCollectContext.close();
        assertThat(rowReceiver.rows.size(), is(8));

        String expected = "1| 0\n" +
                "1| 1\n" +
                "1| 2\n" +
                "1| NULL\n" +
                "1| NULL\n" +
                "2| 1\n" +
                "2| 3\n" +
                "2| NULL\n";
        assertEquals(expected, TestingHelpers.printedTable(rowReceiver.result()));

        rowReceiver = new CollectingRowReceiver();
        when(projectorChain.newShardDownstreamProjector(any(ProjectionToProjectorVisitor.class))).thenReturn(rowReceiver);
        // Nulls first
        builder = jobContextService.newBuilder(node.jobId());
        builder.addSubContext(node.executionPhaseId(),
                new JobCollectContext(node.jobId(), node, mock(MapSideDataCollectOperation.class), RAM_ACCOUNTING_CONTEXT, rowReceiver));
        jobContextService.createContext(builder);
        jobCollectContext = jobContextService.getContext(node.jobId()).getSubContext(node.executionPhaseId());

        orderBy = new OrderBy(ImmutableList.<Symbol>of(x, y), new boolean[]{false, false}, new Boolean[]{false, true});
        node.orderBy(orderBy);
        collector = (LuceneDocCollector)shardCollectService.getCollector(
                node, projectorChain, jobCollectContext, 0, PAGE_SIZE);
        collector.doCollect();
        jobCollectContext.close();

        expected = "1| NULL\n" +
                   "1| NULL\n" +
                   "1| 0\n" +
                   "1| 1\n" +
                   "1| 2\n" +
                   "2| NULL\n" +
                   "2| 1\n" +
                   "2| 3\n";
        assertEquals(expected, TestingHelpers.printedTable(rowReceiver.result()));
    }

    @Test
    public void testMinScoreQuery() throws Exception {
        // where _score = 1.1
        Reference minScore_ref = new Reference(
                new ReferenceInfo(new ReferenceIdent(null, "_score"), RowGranularity.DOC, DataTypes.DOUBLE));

        Function function = new Function(new FunctionInfo(
                new FunctionIdent(EqOperator.NAME, Arrays.<DataType>asList(DataTypes.DOUBLE, DataTypes.DOUBLE)),
                DataTypes.BOOLEAN),
                Arrays.asList(minScore_ref, Literal.newLiteral(1.1))
        );
        WhereClause whereClause = new WhereClause(function);
        LuceneDocCollector docCollector = createDocCollector(null, null, orderBy.orderBySymbols(), whereClause, PAGE_SIZE);
        docCollector.doCollect();
        assertThat(rowReceiver.rows.size(), is(0));
        jobCollectContext.close();

        // where _score = 1.0
        rowReceiver.rows.clear();
        function = new Function(new FunctionInfo(
                new FunctionIdent(EqOperator.NAME, Arrays.<DataType>asList(DataTypes.DOUBLE, DataTypes.DOUBLE)),
                DataTypes.BOOLEAN),
                Arrays.asList(minScore_ref, Literal.newLiteral(1.0))
        );
        whereClause = new WhereClause(function);
        docCollector = createDocCollector(null, null, orderBy.orderBySymbols(), whereClause, PAGE_SIZE);
        docCollector.doCollect();
        assertThat(rowReceiver.rows.size(), is(NUMBER_OF_DOCS));
    }
}
