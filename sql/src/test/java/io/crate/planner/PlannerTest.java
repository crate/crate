package io.crate.planner;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import io.crate.analyze.Analysis;
import io.crate.analyze.Analyzer;
import io.crate.metadata.*;
import io.crate.metadata.doc.DocSchemaInfo;
import io.crate.metadata.sys.MetaDataSysModule;
import io.crate.metadata.sys.SysShardsTableInfo;
import io.crate.metadata.table.TestingTableInfo;
import io.crate.metadata.table.SchemaInfo;
import io.crate.metadata.table.TableInfo;
import io.crate.operator.aggregation.impl.AggregationImplModule;
import io.crate.planner.node.MergeNode;
import io.crate.planner.node.PlanNode;
import io.crate.planner.node.CollectNode;
import io.crate.planner.symbol.Function;
import io.crate.sql.parser.SqlParser;
import io.crate.sql.tree.Statement;
import org.cratedb.DataType;
import org.elasticsearch.cluster.ClusterService;
import org.elasticsearch.common.inject.Injector;
import org.elasticsearch.common.inject.ModulesBuilder;
import org.h2.command.dml.Merge;
import org.junit.Before;
import org.junit.Test;

import java.util.Iterator;
import java.util.Map;
import java.util.Set;

import static org.hamcrest.CoreMatchers.instanceOf;
import static org.hamcrest.core.Is.is;
import static org.junit.Assert.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class PlannerTest {

    private Injector injector;
    private Analyzer analyzer;

    class TestShardsTableInfo extends SysShardsTableInfo {

        Routing routing = new Routing(ImmutableMap.<String, Map<String, Set<Integer>>>builder()
                .put("nodeOne", ImmutableMap.<String, Set<Integer>>of("t1", ImmutableSet.of(1, 2)))
                .put("nodeTow", ImmutableMap.<String, Set<Integer>>of("t1", ImmutableSet.of(3, 4)))
                .build());

        public TestShardsTableInfo() {
            super(null);
        }

        @Override
        public Routing getRouting(Function whereClause) {
            return routing;
        }
    }

    class TestSysModule extends MetaDataSysModule {

        @Override
        protected void bindTableInfos() {
            tableInfoBinder.addBinding(TestShardsTableInfo.IDENT.name()).toInstance(
                    new TestShardsTableInfo());
        }
    }

    class TestModule extends MetaDataModule {

        @Override
        protected void configure() {
            ClusterService clusterService = mock(ClusterService.class);
            bind(ClusterService.class).toInstance(clusterService);
            super.configure();
        }

        @Override
        protected void bindReferences() {

        }

        @Override
        protected void bindSchemas() {
            super.bindSchemas();
            SchemaInfo schemaInfo = mock(SchemaInfo.class);
            TableIdent userTableIdent = new TableIdent(null, "users");
            TableInfo userTableInfo = TestingTableInfo.builder(userTableIdent, RowGranularity.DOC)
                    .add("name", DataType.STRING, null)
                    .add("id", DataType.LONG, null)
                    .build();
            when(schemaInfo.getTableInfo(userTableIdent.name())).thenReturn(userTableInfo);
            schemaBinder.addBinding(DocSchemaInfo.NAME).toInstance(schemaInfo);

        }
    }

    @Before
    public void setUp() throws Exception {
        injector = new ModulesBuilder()
                .add(new TestModule())
                .add(new TestSysModule())
                .add(new AggregationImplModule())
                .createInjector();
        analyzer = injector.getInstance(Analyzer.class);
    }

    @Test
    public void testGlobalAggregationPlan() throws Exception {
        Statement statement = SqlParser.createStatement("select count(name) from users");

        Analysis analysis = analyzer.analyze(statement);
        Planner planner = new Planner();
        Plan plan = planner.plan(analysis);
        Iterator<PlanNode> iterator = plan.iterator();

        PlanNode planNode = iterator.next();
        assertThat(planNode, instanceOf(CollectNode.class));
        CollectNode collectNode = (CollectNode)planNode;

        assertThat(collectNode.outputTypes().get(0), is(DataType.NULL));

        planNode = iterator.next();
        assertThat(planNode, instanceOf(MergeNode.class));
        MergeNode mergeNode = (MergeNode)planNode;

        assertThat(mergeNode.inputTypes().get(0), is(DataType.NULL));
        assertThat(mergeNode.outputTypes().get(0), is(DataType.LONG));

        PlanPrinter pp = new PlanPrinter();
        System.out.println(pp.print(plan));
    }

    @Test
    public void testShardPlan() throws Exception {
        Statement statement = SqlParser.createStatement("select id from sys.shards order by id limit 10");
        // TODO: add where clause
        Analysis analysis = analyzer.analyze(statement);
        Planner planner = new Planner();
        Plan plan = planner.plan(analysis);
        PlanPrinter pp = new PlanPrinter();
        System.out.println(pp.print(plan));
    }

    @Test
    public void testESSearchPlan() throws Exception {
        Statement statement = SqlParser.createStatement("select name from users order by id limit 10");
        // TODO: add where clause
        Analysis analysis = analyzer.analyze(statement);
        Planner planner = new Planner();
        Plan plan = planner.plan(analysis);
        PlanPrinter pp = new PlanPrinter();
        System.out.println(pp.print(plan));
    }


}
