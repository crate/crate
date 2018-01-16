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

package io.crate.execution.engine.sort;

import com.google.common.collect.Ordering;
import io.crate.test.integration.CrateUnitTest;
import org.hamcrest.Matchers;
import org.junit.Test;

import java.util.Arrays;

import static org.hamcrest.core.Is.is;

/**
 * NOTE: The given <p>reverse</p> boolean is always reversed by the {@link OrderingByPosition} so the comparator is
 * working correctly while used on queue implementations where elements are popped from the end of the queue
 * while iterating. So all tests should also be read reversed ;).
 */
public class OrderingByPositionTest extends CrateUnitTest {

    @Test
    public void testOrderByAscNullsFirst() throws Exception {
        Ordering<Object[]> ordering = OrderingByPosition.arrayOrdering(0, false, true);

        assertThat(ordering.compare(new Object[]{1}, new Object[]{null}), is(-1));
    }

    @Test
    public void testOrderByAscNullsLast() throws Exception {
        Ordering<Object[]> ordering = OrderingByPosition.arrayOrdering(0, false, false);

        assertThat(ordering.compare(new Object[]{1}, new Object[]{null}), is(1));
    }

    @Test
    public void testOrderByDescNullsLast() throws Exception {
        Ordering<Object[]> ordering = OrderingByPosition.arrayOrdering(0, true, false);

        assertThat(ordering.compare(new Object[]{1}, new Object[]{null}), is(1));
        assertThat(ordering.compare(new Object[]{1}, new Object[]{2}), is(-1));
    }

    @Test
    public void testOrderByDescNullsFirst() throws Exception {
        Ordering<Object[]> ordering = OrderingByPosition.arrayOrdering(0, true, true);

        assertThat(ordering.compare(new Object[]{1}, new Object[]{null}), is(-1));
    }

    @Test
    public void testOrderByAsc() throws Exception {
        Ordering<Object[]> ordering = OrderingByPosition.arrayOrdering(0, true, null);

        assertThat(ordering.compare(new Object[]{1}, new Object[]{2}), is(-1));
    }

    @Test
    public void testMultipleOrderBy() throws Exception {
        Ordering<Object[]> ordering = Ordering.compound(Arrays.asList(
            OrderingByPosition.arrayOrdering(1, false, null),
            OrderingByPosition.arrayOrdering(0, false, null)
        ));

        assertThat(ordering.compare(new Object[]{0, 0}, new Object[]{4, 0}), is(1));
        assertThat(ordering.compare(new Object[]{4, 0}, new Object[]{1, 1}), is(1));
        assertThat(ordering.compare(new Object[]{5, 1}, new Object[]{2, 2}), is(1));
        assertThat(ordering.compare(new Object[]{5, 1}, new Object[]{2, 2}), is(1));
    }

    @Test
    public void testSingleOrderByPositionResultsInNonCompoundOrdering() throws Exception {
        Ordering<Object[]> ordering = OrderingByPosition.arrayOrdering(
            new int[]{0}, new boolean[]{false}, new Boolean[]{null});
        assertThat(ordering, Matchers.instanceOf(OrderingByPosition.class));
    }
}
