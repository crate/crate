/*
 * Licensed to Crate.io GmbH ("Crate") under one or more contributor
 * license agreements. See the NOTICE file distributed with this work for
 * additional information regarding copyright ownership. Crate licenses
 * this file to you under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License. You may
 * obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations
 * under the License.
 *
 * However, if you have executed another commercial license agreement
 * with Crate these terms will supersede the license and you may use the
 * software solely pursuant to the terms of the relevant commercial agreement.
 */

package io.crate.integrationtests;

import static io.crate.testing.Asserts.assertSQLError;
import static org.assertj.core.api.Assertions.assertThat;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.node.DiscoveryNodes;
import org.elasticsearch.test.IntegTestCase;
import org.junit.Test;

import io.crate.testing.UseRandomizedOptimizerRules;

/**
 * Tests that the storage identifiers (oids) Lucene knows about surface as column names in
 * everything a user gets to see.
 *
 * <p>
 * These cases go through Lucene instead of asserting on hard coded messages like the unit tests
 * in {@code StorageIdentsTest} do, so a change in the format of the Lucene messages shows up as
 * a failure here instead of passing silently.
 * </p>
 */
@IntegTestCase.ClusterScope(numDataNodes = 1)
public class StorageIdentsIntegrationTest extends IntegTestCase {

    private static final String TOO_LONG_FOR_LUCENE = "a".repeat(40000);

    @Test
    public void test_indexing_error_names_the_column_instead_of_the_oid() {
        execute("create table indexed_tbl (network text)");

        assertSQLError(() -> execute(
                "insert into indexed_tbl (network) values (?)",
                new Object[] { TOO_LONG_FOR_LUCENE }))
            .hasMessageContaining("field=\"network\"");
    }

    @Test
    public void test_doc_values_error_names_the_column_instead_of_the_oid() {
        execute("create table unindexed_tbl (network text index off)");

        assertSQLError(() -> execute(
                "insert into unindexed_tbl (network) values (?)",
                new Object[] { TOO_LONG_FOR_LUCENE }))
            .hasMessageContaining("DocValuesField \"network\"");
    }

    @Test
    @UseRandomizedOptimizerRules(0)
    @SuppressWarnings("unchecked")
    public void test_explain_analyze_names_the_column_instead_of_the_oid() {
        execute("create table explain_tbl (ts timestamp with time zone)");
        execute("insert into explain_tbl (ts) values (1741790715000)");
        execute("refresh table explain_tbl");

        execute("explain analyze select ts from explain_tbl where ts < 1741790716773");

        Map<String, Object> analysis = (Map<String, Object>) response.rows()[0][0];
        Map<String, Object> executeAnalysis = (Map<String, Object>) analysis.get("Execute");
        List<String> queryDescriptions = new ArrayList<>();
        DiscoveryNodes nodes = clusterService().state().nodes();
        for (DiscoveryNode node : nodes) {
            if (executeAnalysis.get(node.getId()) instanceof Map<?, ?> timings
                && timings.get("QueryBreakdown") instanceof List<?> queryBreakdown) {
                for (Object entry : queryBreakdown) {
                    queryDescriptions.add((String) ((Map<?, ?>) entry).get("QueryDescription"));
                }
            }
        }

        assertThat(queryDescriptions).isNotEmpty();
        assertThat(queryDescriptions).anySatisfy(
            description -> assertThat(description).contains("ts:"));
    }
}
