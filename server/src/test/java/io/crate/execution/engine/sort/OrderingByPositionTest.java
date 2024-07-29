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

package io.crate.execution.engine.sort;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.Arrays;
import java.util.Comparator;
import java.util.List;

import org.elasticsearch.test.ESTestCase;
import org.junit.Test;

import io.crate.common.collections.Ordering;
import io.crate.types.DataTypes;

public class OrderingByPositionTest extends ESTestCase {

    @Test
    public void testOrderByAscNullsFirst() throws Exception {
        Comparator<Object[]> ordering =
            OrderingByPosition.arrayOrdering(DataTypes.INTEGER, 0, false, true);

        assertThat(ordering.compare(new Object[]{1}, new Object[]{null})).isEqualTo(1);
    }

    @Test
    public void testOrderByAscNullsLast() throws Exception {
        Comparator<Object[]> ordering =
            OrderingByPosition.arrayOrdering(DataTypes.INTEGER, 0, false, false);

        assertThat(ordering.compare(new Object[]{1}, new Object[]{null})).isEqualTo(-1);
    }

    @Test
    public void testOrderByDescNullsLast() throws Exception {
        Comparator<Object[]> ordering =
            OrderingByPosition.arrayOrdering(DataTypes.INTEGER, 0, true, false);

        assertThat(ordering.compare(new Object[]{1}, new Object[]{null})).isEqualTo(-1);
        assertThat(ordering.compare(new Object[]{1}, new Object[]{2})).isEqualTo(1);
    }

    @Test
    public void testOrderByDescNullsFirst() throws Exception {
        Comparator<Object[]> ordering =
            OrderingByPosition.arrayOrdering(DataTypes.INTEGER, 0, true, true);

        assertThat(ordering.compare(new Object[]{1}, new Object[]{null})).isEqualTo(1);
    }

    @Test
    public void testOrderByAsc() throws Exception {
        Comparator<Object[]> ordering = OrderingByPosition.arrayOrdering(DataTypes.INTEGER, 0, false, true);

        assertThat(ordering.compare(new Object[]{1}, new Object[]{2})).isEqualTo(-1);
    }

    @Test
    public void testMultipleOrderBy() throws Exception {
        var orderings = Arrays.asList(
            OrderingByPosition.arrayOrdering(DataTypes.INTEGER, 1, false, false),
            OrderingByPosition.arrayOrdering(DataTypes.INTEGER, 0, false, false)
        );
        Comparator<Object[]> ordering = Ordering.compound(orderings);

        assertThat(ordering.compare(new Object[]{0, 0}, new Object[]{4, 0})).isEqualTo(-1);
        assertThat(ordering.compare(new Object[]{4, 0}, new Object[]{1, 1})).isEqualTo(-1);
        assertThat(ordering.compare(new Object[]{5, 1}, new Object[]{2, 2})).isEqualTo(-1);
        assertThat(ordering.compare(new Object[]{5, 1}, new Object[]{2, 2})).isEqualTo(-1);
    }

    @Test
    public void testSingleOrderByPositionResultsInNonCompoundOrdering() throws Exception {
        Comparator<Object[]> ordering = OrderingByPosition.arrayOrdering(
            List.of(DataTypes.INTEGER),
            new int[]{0},
            new boolean[]{false},
            new boolean[]{false}
        );
        assertThat(ordering).isExactlyInstanceOf(NullAwareComparator.class);
    }
}
