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

package io.crate.expression.scalar;


import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.util.List;

import org.apache.lucene.search.Query;
import org.elasticsearch.Version;
import org.junit.Test;

import io.crate.testing.QueryTester;

public class KnnMatchTest extends ScalarTestCase {

    @Test
    public void test_knn_match_evaluate_results_in_an_error() throws Exception {
        assertThatThrownBy(() -> {
            assertEvaluate(
                "knn_match([1.0, 2.0, 3.14]::float_vector(3), [1.0, 2.0, 3.14]::float_vector(3), 5)", true);
        }).isExactlyInstanceOf(UnsupportedOperationException.class);
    }

    @Test
    public void test_knn_query_builder() throws Exception {
        String createTable = "create table tbl (x float_vector(4))";
        QueryTester.Builder builder = new QueryTester.Builder(
            THREAD_POOL,
            clusterService,
            Version.CURRENT,
            createTable
        );
        float[] vector1 = new float[] { 200.2f, 300.4f, 500.6f, 700.8f };
        float[] vector2 = new float[] { 0.2f, 0.5f, 0.7f, 0.8f };
        builder.indexValue("x", vector1);
        builder.indexValue("x", vector2);
        try (QueryTester tester = builder.build()) {
            Query query = tester.toQuery("knn_match(x, [1.2, 3.4, 5.6, 7.8], 10)");
            assertThat(query.toString()).isEqualTo("KnnFloatVectorQuery:x[1.2,...][10]");

            List<Object> result = tester.runQuery("x", "knn_match(x, [200, 300, 500, 700], 1)");
            assertThat(result).containsExactly(
                vector1
            );
        }
    }
}

