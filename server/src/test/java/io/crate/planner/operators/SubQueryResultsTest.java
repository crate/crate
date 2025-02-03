/*
 * Licensed to Crate.io GmbH ("Crate") under one or more contributor
 * license agreements.  See the NOTICE file distributed with this work for
 * additional information regarding copyright ownership.  Crate licenses
 * this file to you under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.  You may
 * obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
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

package io.crate.planner.operators;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.Map;

import org.elasticsearch.test.ESTestCase;
import org.junit.Test;

import io.crate.expression.symbol.SelectSymbol;
import io.crate.testing.DummyRelation;
import io.crate.types.DataTypes;

public class SubQueryResultsTest extends ESTestCase {

    @Test
    public void test_merges_values_of_both_sides() {
        SelectSymbol selectSymbol1 = new SelectSymbol(
            new DummyRelation("x"),
            DataTypes.STRING_ARRAY,
            SelectSymbol.ResultType.SINGLE_COLUMN_SINGLE_VALUE,
            false);
        SelectSymbol selectSymbol2 = new SelectSymbol(
            new DummyRelation("y"),
            DataTypes.STRING_ARRAY,
            SelectSymbol.ResultType.SINGLE_COLUMN_SINGLE_VALUE,
            false);

        SubQueryResults subQueryResults1 = new SubQueryResults(Map.of(selectSymbol1, "foo"));
        SubQueryResults subQueryResults2 = new SubQueryResults(Map.of(selectSymbol2, "bar"));

        SubQueryResults merged = SubQueryResults.merge(subQueryResults1, subQueryResults2);

        assertThat(merged.getSafe(selectSymbol1)).isEqualTo("foo");
        assertThat(merged.getSafe(selectSymbol2)).isEqualTo("bar");
    }
}
