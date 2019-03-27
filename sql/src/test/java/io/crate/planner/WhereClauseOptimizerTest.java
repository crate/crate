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

package io.crate.planner;

import io.crate.analyze.QueriedTable;
import io.crate.expression.eval.EvaluatingNormalizer;
import io.crate.metadata.PartitionName;
import io.crate.metadata.RelationName;
import io.crate.metadata.RowGranularity;
import io.crate.metadata.doc.DocTableInfo;
import io.crate.test.integration.CrateDummyClusterServiceUnitTest;
import io.crate.testing.SQLExecutor;
import org.junit.Before;
import org.junit.Test;

import static io.crate.testing.SymbolMatchers.isLiteral;
import static io.crate.testing.TestingHelpers.isDocKey;
import static java.util.Collections.singletonList;
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.is;

public class WhereClauseOptimizerTest extends CrateDummyClusterServiceUnitTest{

    private SQLExecutor e;

    @Before
    public void setUpExecutor() throws Exception {
        e = SQLExecutor.builder(clusterService)
            .addTable("create table bystring (name string primary key, score double) " +
                      "clustered by (name) ")
            .addTable("create table clustered_by_only (x int) clustered by (x)")
            .addPartitionedTable(
                "create table parted (" +
                "   id int," +
                "   date timestamp with time zone" +
                ") partitioned by (date)"
            )
            .addPartitionedTable(
                "create table parted_pk (" +
                "   id int primary key, " +
                "   date timestamp with time zone primary key" +
                ") partitioned by (date)",
                new PartitionName(new RelationName("doc", "parted_pk"), singletonList("1395874800000")).asIndexName(),
                new PartitionName(new RelationName("doc", "parted_pk"), singletonList("1395961200000")).asIndexName(),
                new PartitionName(new RelationName("doc", "parted_pk"), singletonList(null)).asIndexName()
            )
            .build();
    }

    private WhereClauseOptimizer.DetailedQuery optimize(String statement) {
        QueriedTable queriedTable = e.normalize(statement);
        EvaluatingNormalizer normalizer = new EvaluatingNormalizer(
            e.functions(),
            RowGranularity.CLUSTER,
            null,
            queriedTable.tableRelation()
        );
        return WhereClauseOptimizer.optimize(
            normalizer,
            queriedTable.where().queryOrFallback(),
            ((DocTableInfo) queriedTable.tableRelation().tableInfo()),
            e.getPlannerContext(clusterService.state()).transactionContext()
        );
    }

    @Test
    public void testFilterOn_IdDoesNotProduceDocKeysIfTableHasOnlyAClusteredByDefinition() {
        WhereClauseOptimizer.DetailedQuery query = optimize(
            "select * from clustered_by_only where _id = '1'");
        assertThat(query.docKeys().isPresent(), is(false));
    }

    @Test
    public void testFilterOn_IdOnPartitionedTableDoesNotResultInDocKeys() {
        WhereClauseOptimizer.DetailedQuery query = optimize(
            "select * from parted where _id = '1'");
        assertThat(query.docKeys().isPresent(), is(false));
    }

    @Test
    public void testFilterOnPartitionColumnAndPrimaryKeyResultsInDocKeys() {
        WhereClauseOptimizer.DetailedQuery query = optimize(
            "select * from parted_pk where id = 1 and date = 1395874800000");
        assertThat(query.docKeys().toString(), is("Optional[DocKeys{1, 1395874800000}]"));
        assertThat(query.partitions(), empty());
    }

    @Test
    public void testClusteredByValueContainsComma() throws Exception {
        WhereClauseOptimizer.DetailedQuery query = optimize(
            "select * from bystring where name = 'a,b,c'");
        assertThat(query.clusteredBy(), contains(isLiteral("a,b,c")));
        assertThat(query.docKeys().get().size(), is(1));
        assertThat(query.docKeys().get().getOnlyKey(), isDocKey("a,b,c"));
    }

    @Test
    public void testEmptyClusteredByValue() throws Exception {
        WhereClauseOptimizer.DetailedQuery query = optimize("select * from bystring where name = ''");
        assertThat(query.clusteredBy(), contains(isLiteral("")));
        assertThat(query.docKeys().get().getOnlyKey(), isDocKey(""));
    }

    @Test
    public void testFilterOnClusteredByColumnDoesNotResultInDocKeysSimpleEq() {
        WhereClauseOptimizer.DetailedQuery query = optimize(
            "select * from clustered_by_only where x = 10");
        assertThat(query.clusteredBy(), contains(isLiteral(10)));
        assertThat(query.docKeys().isPresent(), is(false));
    }

    @Test
    public void testFilterOnClusteredByColumnDoesNotResultInDocKeysSimpleEqOr() {
        WhereClauseOptimizer.DetailedQuery query = optimize(
            "select * from clustered_by_only where x = 10 or x = 20");
        assertThat(query.clusteredBy(), containsInAnyOrder(isLiteral(10), isLiteral(20)));
        assertThat(query.docKeys().isPresent(), is(false));
    }

    @Test
    public void testFilterOnClusteredByColumnDoesNotResultInDocKeysIn() {
        WhereClauseOptimizer.DetailedQuery query = optimize(
            "select * from clustered_by_only where x in (10, 20)");
        assertThat(query.clusteredBy(), containsInAnyOrder(isLiteral(10), isLiteral(20)));
        assertThat(query.docKeys().isPresent(), is(false));
    }

    @Test
    public void testFilterOnPKAndVersionResultsInDocKeys() {
        WhereClauseOptimizer.DetailedQuery query = optimize(
            "select * from bystring where name = 'foo' and _version = 2");
        assertThat(query.docKeys().toString(), is("Optional[DocKeys{foo, 2}]"));
    }
}
