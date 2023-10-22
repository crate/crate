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
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import org.apache.lucene.document.ShapeField;
import org.apache.lucene.search.ConstantScoreQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.spatial.prefix.WithinPrefixTreeQuery;
import org.junit.Test;

public class WithinQueryBuilderTest extends LuceneQueryBuilderTest {

    @Test
    public void test_prefix_tree_backed_geo_shape_match_within() throws Exception {
        Query query = convert("match(shape, 'POLYGON ((30 10, 40 40, 20 40, 10 20, 30 10))') using within");
        assertThat(query).isExactlyInstanceOf(WithinPrefixTreeQuery.class);
    }

    @Test
    public void test_bkd_tree_backed_geo_shape_match_within() {
        Query query = convert("match(bkd_shape, 'POLYGON ((30 10, 40 40, 20 40, 10 20, 30 10))') using within");
        assertThat(query).isExactlyInstanceOf(ConstantScoreQuery.class);
        Query bkdQuery = ((ConstantScoreQuery) query).getQuery();
        assertThat(bkdQuery).extracting("queryRelation").isEqualTo(ShapeField.QueryRelation.WITHIN);
    }

    @Test
    public void testWithinFunctionTooFewPoints() throws Exception {
        assertThatThrownBy(
                () -> convert("within(point, {type='LineString', coordinates=[[0.0, 0.0], [1.0, 1.0]]})"))
                .isExactlyInstanceOf(IllegalArgumentException.class)
                .hasMessage("at least 4 polygon points required");
    }

    @Test
    public void testWithinFunctionWithShapeReference() throws Exception {
        // shape references cannot use the inverted index, so use generic function here
        Query eqWithinQuery = convert("within(point, shape)");
        assertThat(eqWithinQuery).isExactlyInstanceOf(GenericFunctionQuery.class);
    }

    @Test
    public void testWithinRectangleWithDatelineCrossing() throws Exception {
        // dateline crossing happens in a geo context when a polygon rectangle is wider than 180 degrees
        Query eqWithinQuery = convert("within(point, 'POLYGON((-95.0 10.0, -95.0 20.0, 95.0 20.0, 95.0 10.0, -95.0 10.0))')");
        assertThat(eqWithinQuery).hasToString(
            "LatLonPointQuery: field=point:[[10.0, 180.0] [20.0, 180.0] [20.0, 95.0] [10.0, 95.0] [10.0, 180.0] [10.0, -180.0] [10.0, -95.0] [20.0, -95.0] [20.0, -180.0] [10.0, -180.0] [10.0, 180.0] ,]");
    }

    @Test
    public void test_point_within_rectangle_with_explicit_bool_equality_check_optimizes_to_lat_lon_point_query() throws Exception {
        Query query = convert("within(point, 'POLYGON ((30 10, 40 40, 20 40, 10 20, 30 10))') = true");
        assertThat(query).hasToString(
            "LatLonPointQuery: field=point:[[40.0, 40.0] [40.0, 20.0] [20.0, 10.0] [10.0, 30.0] [40.0, 40.0] ,]");
    }
}
