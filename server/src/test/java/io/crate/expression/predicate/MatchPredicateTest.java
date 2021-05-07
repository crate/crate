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

package io.crate.expression.predicate;

import org.elasticsearch.test.ESTestCase;
import io.crate.types.DataTypes;
import org.junit.Test;

import static org.hamcrest.core.Is.is;

public class MatchPredicateTest extends ESTestCase {

    @Test
    public void testGetStringDefaultMatchType() throws Exception {
        assertThat(MatchPredicate.getMatchType(null, DataTypes.STRING), is("best_fields"));
    }

    @Test
    public void testGetValidStringMatchType() throws Exception {
        assertThat(MatchPredicate.getMatchType("most_fields", DataTypes.STRING), is("most_fields"));
    }

    @Test
    public void testGetValidGeoMatchType() throws Exception {
        expectedException.expect(IllegalArgumentException.class);
        expectedException.expectMessage("invalid MATCH type 'contains' for type 'geo_shape'");
        MatchPredicate.getMatchType("contains", DataTypes.GEO_SHAPE);
    }

    @Test
    public void testGetGeoShapeDefaultMatchType() throws Exception {
        assertThat(MatchPredicate.getMatchType(null, DataTypes.GEO_SHAPE), is("intersects"));
    }

    @Test
    public void testGetDefaultMatchTypeForInvalidType() throws Exception {
        expectedException.expect(IllegalArgumentException.class);
        expectedException.expectMessage("No default matchType found for dataType: integer");
        MatchPredicate.getMatchType(null, DataTypes.INTEGER);
    }

    @Test
    public void testGetMatchTypeForInvalidType() throws Exception {
        expectedException.expect(IllegalArgumentException.class);
        expectedException.expectMessage("No match type for dataType: integer");
        MatchPredicate.getMatchType("foo", DataTypes.INTEGER);
    }

    @Test
    public void testInvalidStringMatchType() {
        expectedException.expect(IllegalArgumentException.class);
        expectedException.expectMessage("invalid MATCH type 'foo' for type 'text'");
        MatchPredicate.getMatchType("foo", DataTypes.STRING);
    }

    @Test
    public void testInvalidGeoShapeMatchType() throws Exception {
        expectedException.expect(IllegalArgumentException.class);
        expectedException.expectMessage("invalid MATCH type 'foo' for type 'geo_shape'");
        MatchPredicate.getMatchType("foo", DataTypes.GEO_SHAPE);
    }
}
