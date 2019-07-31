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

package io.crate.sql.tree;

import org.junit.Test;

import java.util.Comparator;
import java.util.List;
import java.util.function.Function;

import static io.crate.sql.tree.FrameBound.Type.FOLLOWING;
import static io.crate.sql.tree.WindowFrame.Mode.RANGE;
import static io.crate.sql.tree.WindowFrame.Mode.ROWS;
import static org.hamcrest.core.Is.is;
import static org.junit.Assert.assertThat;

public class OffsetFollowingFrameBoundTest {

    @Test
    public void test_following_end_in_range_mode() {
        Comparator<Integer> comparing = Comparator.comparing(x -> x);
        var rows = List.of(1, 2, 3, 6, 7);
        int pStart = 1;
        int pEnd = 5;
        Function<Integer, Integer> getOffset = row -> 2;
        Function<Integer, Integer> getOrderingValue = row -> row;
        int currentRowIdx = 1;
        int frameEnd = FOLLOWING.getEnd(
            RANGE,
            pStart,
            pEnd,
            currentRowIdx,
            getOffset,
            getOrderingValue,
            Integer::sum,
            comparing,
            null,
            rows
        );
        assertThat(frameEnd, is(3));
    }

    @Test
    public void test_following_end_in_rows_mode() {
        Comparator<Integer> comparing = Comparator.comparing(x -> x);
        var rows = List.of(1, 2, 3, 6, 7);
        int pStart = 1;
        int pEnd = 5;
        Function<Integer, Integer> getOffset = row -> 2;
        Function<Integer, Integer> getOrderingValue = row -> row;
        int currentRowIdx = 1;
        int frameEnd = FOLLOWING.getEnd(
            ROWS,
            pStart,
            pEnd,
            currentRowIdx,
            getOffset,
            getOrderingValue,
            Integer::sum,
            comparing,
            null,
            rows
        );
        assertThat(frameEnd, is(4));
    }
}
