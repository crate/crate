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

package io.crate.lucene;

import static io.crate.testing.Asserts.assertThat;

import org.apache.lucene.search.Query;
import org.junit.Test;

public class DistanceQueryBuilderTest extends LuceneQueryBuilderTest {

    @Test
    public void test_distance_lt_matches_points_within_distance() {
        Query query = convert("distance(point, 'POINT (10 20)') < 120000");
        assertThat(query).hasToString("point:20.0,10.0 +/- 120000.0 meters");
    }

    @Test
    public void test_distance_gt_and_gte_exclude_null_points() {
        Query query = convert("distance(point, 'POINT (10 20)') > 120000");
        assertThat(query).hasToString("+FieldExistsQuery [field=point] -point:20.0,10.0 +/- 120000.0 meters");

        query = convert("distance(point, 'POINT (10 20)') >= 120000");
        assertThat(query).hasToString("+FieldExistsQuery [field=point] -point:20.0,10.0 +/- 120000.0 meters");
    }

    @Test
    public void test_distance_gte_zero_matches_all_non_null_points() {
        Query query = convert("distance(point, 'POINT (10 20)') >= 0");
        assertThat(query).hasToString("FieldExistsQuery [field=point]");
    }

    @Test
    public void test_distance_is_null_matches_null_points() {
        Query query = convert("distance(point, 'POINT (10 20)') IS NULL");
        assertThat(query).hasToString("+*:* -FieldExistsQuery [field=point]");

        query = convert("distance(point, 'POINT (10 20)') IS NOT NULL");
        assertThat(query).hasToString("FieldExistsQuery [field=point]");

    }

    @Test
    public void test_distance_to_null_point_uses_generic_function_query() {
        // distance(point, NULL) is NULL for every row
        Query query = convert("distance(point, null) IS NULL");
        assertThat(query).isExactlyInstanceOf(GenericFunctionQuery.class);

        query = convert("distance(point, null) IS NOT NULL");
        assertThat(query).hasToString("+*:* -(distance(_doc['point'], NULL) IS NULL)");

        query = convert("distance(point, null) > 10");
        assertThat(query).isExactlyInstanceOf(GenericFunctionQuery.class);
    }
}
