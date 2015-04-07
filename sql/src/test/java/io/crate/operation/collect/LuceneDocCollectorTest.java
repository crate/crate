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
import io.crate.Constants;
import io.crate.analyze.OrderBy;
import io.crate.analyze.WhereClause;
import io.crate.breaker.RamAccountingContext;
import io.crate.integrationtests.SQLTransportIntegrationTest;
import io.crate.metadata.*;
import io.crate.operation.operator.EqOperator;
import io.crate.operation.projectors.CollectingProjector;
import io.crate.operation.projectors.ProjectionToProjectorVisitor;
import io.crate.operation.scalar.arithmetic.MultiplyFunction;
import io.crate.planner.RowGranularity;
import io.crate.planner.node.dql.CollectNode;
import io.crate.planner.symbol.Function;
import io.crate.planner.symbol.Literal;
import io.crate.planner.symbol.Reference;
import io.crate.planner.symbol.Symbol;
import io.crate.test.integration.CrateIntegrationTest;
import io.crate.types.DataType;
import io.crate.types.DataTypes;
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.common.breaker.CircuitBreaker;
import org.elasticsearch.common.breaker.NoopCircuitBreaker;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.index.service.IndexService;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.indices.IndicesService;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.UUID;

import static io.crate.testing.TestingHelpers.createReference;
import static org.hamcrest.Matchers.is;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

@CrateIntegrationTest.ClusterScope(scope = CrateIntegrationTest.Scope.SUITE, numNodes = 1)
public class LuceneDocCollectorTest extends SQLTransportIntegrationTest {

    private final static String INDEX_NAME = "countries";
    private final static Integer NUMBER_OF_DOCS = 25000;
    private ShardId shardId = new ShardId(INDEX_NAME, 0);
    private OrderBy orderBy;
    private CollectContextService collectContextService;
    private ShardCollectService shardCollectService;

    private CollectingProjector collectingProjector = new CollectingProjector();

    private static final RamAccountingContext RAM_ACCOUNTING_CONTEXT =
            new RamAccountingContext("dummy", new NoopCircuitBreaker(CircuitBreaker.Name.FIELDDATA));

    @Before
    public void prepare() throws Exception{
        execute("create table \""+INDEX_NAME+ "\" (" +
                " continent string, " +
                " countryName string," +
                " population integer" +
                ") clustered into 1 shards with (number_of_replicas=0)");
        refresh(client());
        generateData();
        IndicesService instanceFromNode = cluster().getInstanceFromFirstNode(IndicesService.class);
        IndexService indexService = instanceFromNode.indexServiceSafe(INDEX_NAME);

        shardCollectService = indexService.shardInjector(0).getInstance(ShardCollectService.class);
        collectContextService = indexService.shardInjector(0).getInstance(CollectContextService.class);

        ReferenceIdent ident = new ReferenceIdent(new TableIdent("doc", "countries"), "countryName");
        Reference ref = new Reference(new ReferenceInfo(ident, RowGranularity.DOC, DataTypes.STRING));
        orderBy = new OrderBy(ImmutableList.of((Symbol)ref), new boolean[]{false}, new Boolean[]{false});
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
            } else {
                indexRequest.source(generateRowSource("America", "USA", i));
            }
            bulkRequest.add(indexRequest);
        }
        BulkResponse response = client().bulk(bulkRequest).actionGet();
        assertFalse(response.hasFailures());
        refresh(client());
    }

    private LuceneDocCollector createDocCollector(OrderBy orderBy, Integer limit, List<Symbol> toCollect) throws Exception{
        return createDocCollector(orderBy, limit, toCollect, WhereClause.MATCH_ALL);
    }

    private LuceneDocCollector createDocCollector(OrderBy orderBy, Integer limit, List<Symbol> toCollect, WhereClause whereClause) throws Exception{
        CollectNode node = new CollectNode();
        node.whereClause(whereClause);
        node.orderBy(orderBy);
        node.limit(limit);
        node.jobId(UUID.randomUUID());
        node.toCollect(toCollect);
        node.maxRowGranularity(RowGranularity.DOC);

        ShardProjectorChain projectorChain = mock(ShardProjectorChain.class);
        when(projectorChain.newShardDownstreamProjector(any(ProjectionToProjectorVisitor.class))).thenReturn(collectingProjector);

        int jobSearchContextId = 0;
        JobCollectContext jobCollectContext = collectContextService.acquireContext(node.jobId().get());
        jobCollectContext.registerJobContextId(shardId, jobSearchContextId);
        LuceneDocCollector collector = (LuceneDocCollector)shardCollectService.getCollector(node, projectorChain, jobCollectContext, 0);
        return collector;
    }

    @Test
    public void testLimitWithoutOrder() throws Exception{
        collectingProjector.rows.clear();
        LuceneDocCollector docCollector = createDocCollector(null, 5000, orderBy.orderBySymbols());
        docCollector.doCollect(RAM_ACCOUNTING_CONTEXT);
        assertThat(collectingProjector.rows.size(), is(5000));
    }

    @Test
    public void testOrderedWithLimit() throws Exception{
        collectingProjector.rows.clear();
        LuceneDocCollector docCollector = createDocCollector(orderBy, 5000, orderBy.orderBySymbols());
        docCollector.doCollect(RAM_ACCOUNTING_CONTEXT);
        assertThat(collectingProjector.rows.size(), is(5000));
        assertThat(((BytesRef)collectingProjector.rows.get(0)[0]).utf8ToString(), is("Austria") );
        assertThat(((BytesRef)collectingProjector.rows.get(1)[0]).utf8ToString(), is("Germany") );
        assertThat(((BytesRef)collectingProjector.rows.get(2)[0]).utf8ToString(), is("USA") );
        assertThat(((BytesRef)collectingProjector.rows.get(3)[0]).utf8ToString(), is("USA") );
    }

    @Test
    public void testOrderedWithLimitHigherThanPageSize() throws Exception{
        collectingProjector.rows.clear();
        LuceneDocCollector docCollector = createDocCollector(orderBy, Constants.PAGE_SIZE + 5000, orderBy.orderBySymbols());
        docCollector.doCollect(RAM_ACCOUNTING_CONTEXT);
        assertThat(collectingProjector.rows.size(), is(Constants.PAGE_SIZE + 5000));
        assertThat(((BytesRef)collectingProjector.rows.get(0)[0]).utf8ToString(), is("Austria") );
        assertThat(((BytesRef)collectingProjector.rows.get(1)[0]).utf8ToString(), is("Germany") );
        assertThat(((BytesRef)collectingProjector.rows.get(2)[0]).utf8ToString(), is("USA") );
        assertThat(((BytesRef)collectingProjector.rows.get(3)[0]).utf8ToString(), is("USA") );
    }

    @Test
    public void testOrderedWithoutLimit() throws Exception {
        collectingProjector.rows.clear();
        LuceneDocCollector docCollector = createDocCollector(orderBy, null, orderBy.orderBySymbols());
        docCollector.doCollect(RAM_ACCOUNTING_CONTEXT);
        assertThat(collectingProjector.rows.size(), is(NUMBER_OF_DOCS));
        assertThat(((BytesRef)collectingProjector.rows.get(0)[0]).utf8ToString(), is("Austria") );
        assertThat(((BytesRef)collectingProjector.rows.get(1)[0]).utf8ToString(), is("Germany") );
        assertThat(((BytesRef)collectingProjector.rows.get(2)[0]).utf8ToString(), is("USA") );
    }

    @Test
    public void testOrderForNonSelected() throws Exception {
        collectingProjector.rows.clear();
        ReferenceIdent countriesIdent = new ReferenceIdent(new TableIdent("doc", "countries"), "countryName");
        Reference countries = new Reference(new ReferenceInfo(countriesIdent, RowGranularity.DOC, DataTypes.STRING));

        ReferenceIdent populationIdent = new ReferenceIdent(new TableIdent("doc", "countries"), "population");
        Reference population = new Reference(new ReferenceInfo(populationIdent, RowGranularity.DOC, DataTypes.INTEGER));

        OrderBy orderBy = new OrderBy(ImmutableList.of((Symbol)population), new boolean[]{true}, new Boolean[]{true});

        LuceneDocCollector docCollector = createDocCollector(orderBy, null, ImmutableList.of((Symbol)countries));
        docCollector.doCollect(RAM_ACCOUNTING_CONTEXT);
        assertThat(collectingProjector.rows.size(), is(NUMBER_OF_DOCS));
        assertThat(collectingProjector.rows.get(0).length, is(1));
        assertThat(((BytesRef)collectingProjector.rows.get(NUMBER_OF_DOCS - 3)[0]).utf8ToString(), is("USA") );
        assertThat(((BytesRef)collectingProjector.rows.get(NUMBER_OF_DOCS - 2)[0]).utf8ToString(), is("Austria") );
        assertThat(((BytesRef)collectingProjector.rows.get(NUMBER_OF_DOCS - 1)[0]).utf8ToString(), is("Germany") );
    }

    @Test
    public void testOrderByScalar() throws Exception {
        collectingProjector.rows.clear();
        Reference population = createReference("population", DataTypes.INTEGER);
        Function scalarFunction = new Function(
                new FunctionInfo(
                        new FunctionIdent(MultiplyFunction.NAME, Arrays.<DataType>asList(DataTypes.INTEGER, DataTypes.INTEGER)),
                        DataTypes.LONG),
                Arrays.<Symbol>asList(population, Literal.newLiteral(-1))
        );

        OrderBy orderBy = new OrderBy(ImmutableList.of((Symbol)scalarFunction), new boolean[]{false}, new Boolean[]{false});
        LuceneDocCollector docCollector = createDocCollector(orderBy, null, ImmutableList.of((Symbol)population));
        docCollector.doCollect(RAM_ACCOUNTING_CONTEXT);
        assertThat(collectingProjector.rows.size(), is(NUMBER_OF_DOCS));
        assertThat(((Integer)collectingProjector.rows.get(NUMBER_OF_DOCS - 2)[0]), is(1) );
        assertThat(((Integer)collectingProjector.rows.get(NUMBER_OF_DOCS - 1)[0]), is(0) );
    }

    @Test
    public void testMinScoreQuery() throws Exception {
        collectingProjector.rows.clear();
        // where _score = 1.1
        Reference minScore_ref = new Reference(
                new ReferenceInfo(new ReferenceIdent(null, "_score"), RowGranularity.DOC, DataTypes.DOUBLE));

        Function function = new Function(new FunctionInfo(
                new FunctionIdent(EqOperator.NAME, Arrays.<DataType>asList(DataTypes.DOUBLE, DataTypes.DOUBLE)),
                DataTypes.BOOLEAN),
                Arrays.<Symbol>asList(minScore_ref, Literal.newLiteral(1.1))
        );
        WhereClause whereClause = new WhereClause(function);
        LuceneDocCollector docCollector = createDocCollector(null, null, orderBy.orderBySymbols(), whereClause);
        docCollector.doCollect(RAM_ACCOUNTING_CONTEXT);
        assertThat(collectingProjector.rows.size(), is(0));

        // where _score = 1.0
        collectingProjector.rows.clear();
        function = new Function(new FunctionInfo(
                new FunctionIdent(EqOperator.NAME, Arrays.<DataType>asList(DataTypes.DOUBLE, DataTypes.DOUBLE)),
                DataTypes.BOOLEAN),
                Arrays.<Symbol>asList(minScore_ref, Literal.newLiteral(1.0))
        );
        whereClause = new WhereClause(function);
        docCollector = createDocCollector(null, null, orderBy.orderBySymbols(), whereClause);
        docCollector.doCollect(RAM_ACCOUNTING_CONTEXT);
        assertThat(collectingProjector.rows.size(), is(NUMBER_OF_DOCS));
    }
}
