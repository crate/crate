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

import org.apache.lucene.search.Query;
import org.apache.lucene.spatial.prefix.WithinPrefixTreeQuery;
import org.junit.Test;

import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.core.Is.is;

public class WithinQueryBuilderTest extends LuceneQueryBuilderTest {

    @Test
    public void testGeoShapeMatchWithin() throws Exception {
        Query query = convert("match(shape, 'POLYGON ((30 10, 40 40, 20 40, 10 20, 30 10))') using within");
        assertThat(query, instanceOf(WithinPrefixTreeQuery.class));
    }

    @Test
    public void testWithinFunctionTooFewPoints() throws Exception {
        expectedException.expect(IllegalArgumentException.class);
        expectedException.expectMessage("at least 4 polygon points required");
        convert("within(point, {type='LineString', coordinates=[[0.0, 0.0], [1.0, 1.0]]})");
    }

    @Test
    public void testWithinFunctionWithShapeReference() throws Exception {
        // shape references cannot use the inverted index, so use generic function here
        Query eqWithinQuery = convert("within(point, shape)");
        assertThat(eqWithinQuery, instanceOf(GenericFunctionQuery.class));
    }

    @Test
    public void testWithinRectangleWithDatelineCrossing() throws Exception {
        // dateline crossing happens in a geo context when a polygon rectangle is wider than 180 degrees
        Query eqWithinQuery = convert("within(point, 'POLYGON((-95.0 10.0, -95.0 20.0, 95.0 20.0, 95.0 10.0, -95.0 10.0))')");
        assertThat(eqWithinQuery.toString(), is("LatLonPointQuery: field=point:[[10.0, 180.0] [20.0, 180.0] [20.0, 95.0] [10.0, 95.0] [10.0, 180.0] [10.0, -180.0] [10.0, -95.0] [20.0, -95.0] [20.0, -180.0] [10.0, -180.0] [10.0, 180.0] ,]"));
    }
}
