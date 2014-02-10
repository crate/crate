package io.crate.planner;

import com.google.common.collect.ImmutableMap;
import io.crate.analyze.Analysis;
import io.crate.analyze.Analyzer;
import io.crate.metadata.*;
import io.crate.operator.aggregation.impl.AggregationImplModule;
import io.crate.sql.parser.SqlParser;
import io.crate.sql.tree.Statement;
import org.cratedb.DataType;
import org.elasticsearch.common.inject.Injector;
import org.elasticsearch.common.inject.ModulesBuilder;
import org.junit.Before;
import org.junit.Test;

import java.util.HashMap;
import java.util.Map;
import java.util.Set;

public class PlannerTest {

    private Injector injector;
    private Analyzer analyzer;

    class TestReferenceResolver implements ReferenceResolver {

        private final Map<ReferenceIdent, ReferenceInfo> infos = new HashMap<>();

        @Override
        public ReferenceInfo getInfo(ReferenceIdent ident) {
            return infos.get(ident);
        }

        @Override
        public ReferenceImplementation getImplementation(ReferenceIdent ident) {
            return null;
        }

        public ReferenceInfo register(String schema, String table, String column, DataType type, RowGranularity granularity) {
            ReferenceInfo info = new ReferenceInfo(new ReferenceIdent(new TableIdent(schema, table), column),
                    granularity, type);
            infos.put(info.ident(), info);
            return info;
        }

    }

    class TestMetaDataModule extends MetaDataModule {

        @Override
        protected void bindRoutings() {
            Map<String, Map<String, Set<Integer>>> locations = ImmutableMap.<String, Map<String, Set<Integer>>>builder()
                    .put("nodeOne", ImmutableMap.<String, Set<Integer>>of())
                    .put("nodeTwo", ImmutableMap.<String, Set<Integer>>of())
                    .build();
            final Routing routing = new Routing(locations);

            Routings routings = new Routings() {

                @Override
                public Routing getRouting(TableIdent tableIdent) {
                    return routing;
                }
            };
            bind(Routings.class).toInstance(routings);
        }

        @Override
        protected void bindReferences() {
            TestReferenceResolver rr = new TestReferenceResolver();
            rr.register(null, "users", "name", DataType.STRING, RowGranularity.DOC);
            rr.register(null, "users", "id", DataType.LONG, RowGranularity.DOC);
            rr.register("sys", "shards", "id", DataType.INTEGER, RowGranularity.SHARD);

            bind(ReferenceResolver.class).toInstance(rr);
        }
    }


    @Before
    public void setUp() throws Exception {
        injector = new ModulesBuilder()
                .add(new TestMetaDataModule())
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
