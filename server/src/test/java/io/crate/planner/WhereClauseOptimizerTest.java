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

package io.crate.planner;

import static io.crate.testing.Asserts.assertThat;
import static io.crate.testing.Asserts.isLiteral;
import static java.util.Collections.singletonList;
import static org.assertj.core.api.Assertions.assertThat;

import java.util.List;

import org.junit.Before;
import org.junit.Test;

import io.crate.analyze.AnalyzedDeleteStatement;
import io.crate.analyze.AnalyzedStatement;
import io.crate.analyze.QueriedSelectRelation;
import io.crate.analyze.relations.DocTableRelation;
import io.crate.expression.eval.EvaluatingNormalizer;
import io.crate.expression.symbol.Symbol;
import io.crate.metadata.PartitionName;
import io.crate.metadata.RelationName;
import io.crate.metadata.RowGranularity;
import io.crate.test.integration.CrateDummyClusterServiceUnitTest;
import io.crate.testing.SQLExecutor;

public class WhereClauseOptimizerTest extends CrateDummyClusterServiceUnitTest {

    private SQLExecutor e;

    @Before
    public void setUpExecutor() throws Exception {
        e = SQLExecutor.of(clusterService)
            .addTable("create table t_pk(a string primary key)")
            .addTable("create table bystring (name string primary key, score double) " +
                      "clustered by (name) ")
            .addTable("create table clustered_by_only (x int) clustered by (x)")
            .addTable(
                "create table parted (" +
                "   id int," +
                "   date timestamp with time zone" +
                ") partitioned by (date)"
            )
            .addTable(
                "create table parted_pk (" +
                "   id int primary key, " +
                "   date timestamp with time zone primary key" +
                ") partitioned by (date)",
                new PartitionName(new RelationName("doc", "parted_pk"), List.of("1395874800000")).asIndexName(),
                new PartitionName(new RelationName("doc", "parted_pk"), List.of("1395961200000")).asIndexName(),
                new PartitionName(new RelationName("doc", "parted_pk"), singletonList(null)).asIndexName()
            )
            .addTable("""
                create table partdatebin (
                    id int,
                    ts timestamp,
                    month as date_bin(INTERVAL '28' DAY, ts, 0)
                ) partitioned by (month)
                """,
                new PartitionName(new RelationName("doc", "partdatebin"), List.of("1676352000000")).asIndexName(),
                new PartitionName(new RelationName("doc", "partdatebin"), List.of("1687767893000")).asIndexName()
            )
            // Important, ts_month has a type different from date_trunc return type to provoke implicit cast addition
            .addTable("""
                create table partdatetrunc (
                    ts TIMESTAMP WITHOUT TIME ZONE,
                    ts_month TIMESTAMP as date_trunc('month', ts)
                ) partitioned by (ts_month)
                """,
                new PartitionName(new RelationName("doc", "partdatetrunc"), List.of("1676352000000")).asIndexName(),
                new PartitionName(new RelationName("doc", "partdatetrunc"), List.of("1687767893000")).asIndexName()
            )
            // Important, ts_month doesn't have type declared and expression is just a CAST.
            .addTable("""
                create table partcast (
                    ts TIMESTAMP WITHOUT TIME ZONE,
                    ts_month as cast(ts as TIMESTAMP WITH TIME ZONE)
                ) partitioned by (ts_month)
                """,
                new PartitionName(new RelationName("doc", "partcast"), List.of("1676352000000")).asIndexName(),
                new PartitionName(new RelationName("doc", "partcast"), List.of("1687767893000")).asIndexName()
            );
    }

    private WhereClauseOptimizer.DetailedQuery optimize(String statement) {
        AnalyzedStatement stmt = e.analyze(statement);
        DocTableRelation table;
        Symbol where;
        if (stmt instanceof QueriedSelectRelation qsr) {
            table = (DocTableRelation) qsr.from().getFirst();
            where = qsr.where();
        } else if (stmt instanceof AnalyzedDeleteStatement ads) {
            table = ads.relation();
            where = ads.query();
        } else {
            throw new IllegalArgumentException("Neither a select or a delete statement is provided");
        }

        EvaluatingNormalizer normalizer = new EvaluatingNormalizer(
            e.nodeCtx,
            RowGranularity.CLUSTER,
            null,
            table
        );
        return WhereClauseOptimizer.optimize(
            normalizer,
            where,
            table.tableInfo(),
            e.getPlannerContext().transactionContext(),
            e.nodeCtx,
            e.getPlannerContext().timeoutToken()
        );
    }

    @Test
    public void testFilterOn_IdDoesNotProduceDocKeysIfTableHasOnlyAClusteredByDefinition() {
        WhereClauseOptimizer.DetailedQuery query = optimize(
            "select * from clustered_by_only where _id = '1'");
        assertThat(query.docKeys().isPresent()).isFalse();
    }

    @Test
    public void testFilterOn_IdOnPartitionedTableDoesNotResultInDocKeys() {
        WhereClauseOptimizer.DetailedQuery query = optimize(
            "select * from parted where _id = '1'");
        assertThat(query.docKeys().isPresent()).isFalse();
    }

    @Test
    public void testFilterOnPartitionColumnAndPrimaryKeyResultsInDocKeys() {
        WhereClauseOptimizer.DetailedQuery query = optimize(
            "select * from parted_pk where id = 1 and date = 1395874800000");
        assertThat(query.docKeys()).hasToString("Optional[DocKeys{1, 1395874800000::bigint}]");
        assertThat(query.partitions()).hasSize(1);
        assertThat(query.partitions().get(0)).satisfiesExactly(isLiteral(1395874800000L));
    }

    @SuppressWarnings("OptionalGetWithoutIsPresent")
    @Test
    public void testClusteredByValueContainsComma() throws Exception {
        WhereClauseOptimizer.DetailedQuery query = optimize(
            "select * from bystring where name = 'a,b,c'");
        assertThat(query.clusteredBy()).satisfiesExactly(isLiteral("a,b,c"));
        assertThat(query.docKeys()).isPresent();
        assertThat(query.docKeys().get()).hasSize(1);
        assertThat(query.docKeys().get().getOnlyKey()).isDocKey("a,b,c");
    }

    @SuppressWarnings("OptionalGetWithoutIsPresent")
    @Test
    public void testEmptyClusteredByValue() throws Exception {
        WhereClauseOptimizer.DetailedQuery query = optimize("select * from bystring where name = ''");
        assertThat(query.clusteredBy()).satisfiesExactly(isLiteral(""));
        assertThat(query.docKeys()).isPresent();
        assertThat(query.docKeys().get().getOnlyKey()).isDocKey("");
    }

    @Test
    public void testFilterOnClusteredByColumnDoesNotResultInDocKeysSimpleEq() {
        WhereClauseOptimizer.DetailedQuery query = optimize(
            "select * from clustered_by_only where x = 10");
        assertThat(query.clusteredBy()).satisfiesExactly(isLiteral(10));
        assertThat(query.docKeys().isPresent()).isFalse();
    }

    @Test
    public void testFilterOnClusteredByColumnDoesNotResultInDocKeysSimpleEqOr() {
        WhereClauseOptimizer.DetailedQuery query = optimize(
            "select * from clustered_by_only where x = 10 or x = 20");
        assertThat(query.clusteredBy()).satisfiesExactlyInAnyOrder(isLiteral(10), isLiteral(20));
        assertThat(query.docKeys().isPresent()).isFalse();
    }

    @Test
    public void testFilterOnClusteredByColumnDoesNotResultInDocKeysIn() {
        WhereClauseOptimizer.DetailedQuery query = optimize(
            "select * from clustered_by_only where x in (10, 20)");
        assertThat(query.clusteredBy()).satisfiesExactlyInAnyOrder(isLiteral(10), isLiteral(20));
        assertThat(query.docKeys().isPresent()).isFalse();
    }

    @Test
    public void testFilterOnPKAndVersionResultsInDocKeys() {
        WhereClauseOptimizer.DetailedQuery query = optimize(
            "select * from bystring where name = 'foo' and _version = 2");
        assertThat(query.docKeys()).hasToString("Optional[DocKeys{'foo', 2::bigint}]");
    }

    @Test
    public void test_filter_on_pk_combined_with_AND_results_in_dockeys() {
        WhereClauseOptimizer.DetailedQuery query = optimize(
            "select * from bystring where name = 'foo' and score = 2.0 or (name = 'bar' and score = 1.0)");
        assertThat(query.docKeys()).hasToString("Optional[DocKeys{'bar'; 'foo'}]");
    }

    @Test
    public void test_expands_filter_on_date_bin_generated_column() throws Exception {
        WhereClauseOptimizer.DetailedQuery query = optimize(
            "select * from partdatebin where ts > '2023-05-01'");
        assertThat(query.query()).isSQL(
            "((doc.partdatebin.ts > 1682899200000::bigint) AND (month >= 1681344000000::bigint))"
        );
    }

    @Test
    public void test_expands_filter_when_generated_column_and_generated_expression_have_different_types() {
        // date_trunc has return type TIMESTAMP WITH TIME ZONE.
        // Generated column ts_month has type TIMESTAMP ==> TIMESTAMP WITHOUT TIME ZONE.
        // Validate that despite the implicit cast, filter is expanded.
        WhereClauseOptimizer.DetailedQuery query = optimize(
            "select * from partdatetrunc where ts > '2023-05-01'");
        assertThat(query.query()).isSQL(
            "((doc.partdatetrunc.ts > 1682899200000::bigint) AND (ts_month >= 1682899200000::bigint))"
        );
    }

    @Test
    public void test_doesnt_expand_filter_when_generated_column_has_no_type_and_expression_is_cast() {
        WhereClauseOptimizer.DetailedQuery query = optimize(
            "select * from partcast where ts > '2023-05-01'");
        assertThat(query.query()).isSQL(
            "(doc.partcast.ts > 1682899200000::bigint)"
        );
    }

    @Test
    public void test_filter_on_pk_with_implicit_cast() {
        WhereClauseOptimizer.DetailedQuery query = optimize("select * from t_pk where a = 10 OR a = true");
        assertThat(query.docKeys()).hasToString("Optional[DocKeys{'10'; 't'}]");
        query = optimize("delete from t_pk where a = 10 OR a = true");
        assertThat(query.docKeys()).hasToString("Optional[DocKeys{'10'; 't'}]");
    }

    @Test
    public void test_filter_on_id_with_implicit_cast() {
        WhereClauseOptimizer.DetailedQuery query = optimize("select * from t_pk where _id = 10 OR _id = true");
        assertThat(query.docKeys()).hasToString("Optional[DocKeys{'10'; 't'}]");
        query = optimize("delete from t_pk where _id = 10 OR _id = true");
        assertThat(query.docKeys()).hasToString("Optional[DocKeys{'10'; 't'}]");
    }
}
