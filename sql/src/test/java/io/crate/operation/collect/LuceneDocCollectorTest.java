package io.crate.operation.collect;


import com.google.common.collect.ImmutableList;
import io.crate.analyze.OrderBy;
import io.crate.analyze.WhereClause;
import io.crate.breaker.RamAccountingContext;
import io.crate.integrationtests.SQLTransportIntegrationTest;
import io.crate.lucene.LuceneQueryBuilder;
import io.crate.metadata.Functions;
import io.crate.metadata.ReferenceIdent;
import io.crate.metadata.ReferenceInfo;
import io.crate.metadata.TableIdent;
import io.crate.operation.Input;
import io.crate.operation.RowDownstream;
import io.crate.operation.operator.OperatorModule;
import io.crate.operation.projectors.CollectingProjector;
import io.crate.operation.reference.doc.lucene.LuceneCollectorExpression;
import io.crate.operation.reference.doc.lucene.OrderByCollectorExpression;
import io.crate.planner.RowGranularity;
import io.crate.planner.node.dql.CollectNode;
import io.crate.planner.symbol.Reference;
import io.crate.planner.symbol.Symbol;
import io.crate.test.integration.CrateIntegrationTest;
import io.crate.types.DataTypes;
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.cache.recycler.CacheRecycler;
import org.elasticsearch.cache.recycler.PageCacheRecycler;
import org.elasticsearch.common.breaker.CircuitBreaker;
import org.elasticsearch.common.breaker.NoopCircuitBreaker;
import org.elasticsearch.common.inject.ModulesBuilder;
import org.elasticsearch.common.util.BigArrays;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.index.service.IndexService;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.indices.IndicesService;
import org.elasticsearch.threadpool.ThreadPool;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.util.List;

import static org.hamcrest.Matchers.is;

@CrateIntegrationTest.ClusterScope(scope = CrateIntegrationTest.Scope.SUITE, numNodes = 1)
public class LuceneDocCollectorTest extends SQLTransportIntegrationTest {

    private final static String INDEX_NAME = "countries";
    private final static Integer NUMBER_OF_DOCS = 35000;
    private ThreadPool threadPool;
    private ShardId shardId = new ShardId(INDEX_NAME, 0);
    private LuceneQueryBuilder builder;
    private IndexService indexService;
    private CacheRecycler cacheRecycler;
    private PageCacheRecycler pageCacheRecycler;
    private BigArrays bigArrays;
    private Functions functions;
    private OrderBy orderBy;
    private OrderByCollectorExpression orderByCollectorExpr;


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

        functions = new ModulesBuilder()
                .add(new OperatorModule()).createInjector().getInstance(Functions.class);
        builder = new LuceneQueryBuilder(functions);
        IndicesService instanceFromNode = cluster().getInstanceFromNode(IndicesService.class);
        indexService = instanceFromNode.indexServiceSafe(INDEX_NAME);
        bigArrays = indexService.injector().getInstance(BigArrays.class);
        cacheRecycler = indexService.injector().getInstance(CacheRecycler.class);
        pageCacheRecycler = indexService.injector().getInstance(PageCacheRecycler.class);
        threadPool = new ThreadPool(getClass().getSimpleName());

        ReferenceIdent ident = new ReferenceIdent(new TableIdent("doc", "countries"), "countryName");
        Reference ref = new Reference(new ReferenceInfo(ident, RowGranularity.DOC, DataTypes.STRING));
        orderBy = new OrderBy(ImmutableList.of((Symbol)ref), new boolean[]{false}, new Boolean[]{false});
        orderByCollectorExpr = new OrderByCollectorExpression(ref, orderBy);
    }

    private LuceneDocCollector createDocCollector(OrderBy orderBy, Integer limit, LuceneCollectorExpression expression) throws Exception{
        return createDocCollector(orderBy, limit, expression, collectingProjector);
    }

    private LuceneDocCollector createDocCollector(OrderBy orderBy, Integer limit, LuceneCollectorExpression expression, RowDownstream downstreamProjector) throws Exception{
        CollectNode node = new CollectNode();
        node.whereClause(WhereClause.MATCH_ALL);
        node.orderBy(orderBy);
        if(limit != null) {
            node.limit(limit);
        }
        node.whereClause(WhereClause.MATCH_ALL);
        return new LuceneDocCollector(threadPool,
                clusterService(),
                builder,
                shardId,
                indexService,
                null,
                cacheRecycler,
                pageCacheRecycler,
                bigArrays,
                (List) ImmutableList.<Input>of(expression),
                (List)ImmutableList.of(expression),
                functions,
                node,
                downstreamProjector);
    }

    @Test
    public void testOrderedWithLimit() throws Exception{
        collectingProjector.rows.clear();
        LuceneDocCollector docCollector = createDocCollector(orderBy, 5000, orderByCollectorExpr);
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
        LuceneDocCollector docCollector = createDocCollector(orderBy, LuceneDocCollector.PAGE_SIZE + 5000, orderByCollectorExpr);
        docCollector.doCollect(RAM_ACCOUNTING_CONTEXT);
        assertThat(collectingProjector.rows.size(), is(LuceneDocCollector.PAGE_SIZE + 5000));
        assertThat(((BytesRef)collectingProjector.rows.get(0)[0]).utf8ToString(), is("Austria") );
        assertThat(((BytesRef)collectingProjector.rows.get(1)[0]).utf8ToString(), is("Germany") );
        assertThat(((BytesRef)collectingProjector.rows.get(2)[0]).utf8ToString(), is("USA") );
        assertThat(((BytesRef)collectingProjector.rows.get(3)[0]).utf8ToString(), is("USA") );
    }

    @Test
    public void testOrderedWithoutLimit() throws Exception {
        collectingProjector.rows.clear();
        LuceneDocCollector docCollector = createDocCollector(orderBy, null, orderByCollectorExpr);
        docCollector.doCollect(RAM_ACCOUNTING_CONTEXT);
        assertThat(collectingProjector.rows.size(), is(NUMBER_OF_DOCS));
        assertThat(((BytesRef)collectingProjector.rows.get(0)[0]).utf8ToString(), is("Austria") );
        assertThat(((BytesRef)collectingProjector.rows.get(1)[0]).utf8ToString(), is("Germany") );
        assertThat(((BytesRef)collectingProjector.rows.get(2)[0]).utf8ToString(), is("USA") );
    }

    @After
    public void cleanUp() {
        threadPool.shutdownNow();
    }
}
