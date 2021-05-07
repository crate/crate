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

package io.crate.execution.engine.window;

import com.carrotsearch.randomizedtesting.annotations.Repeat;
import io.crate.common.collections.Lists2;
import io.crate.data.Input;
import io.crate.data.Row;
import io.crate.execution.engine.collect.CollectExpression;
import io.crate.execution.engine.sort.OrderingByPosition;
import io.crate.metadata.functions.Signature;
import org.elasticsearch.test.ESTestCase;
import org.hamcrest.Matchers;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.concurrent.TimeUnit;

import static io.crate.execution.engine.window.WindowFunctionBatchIterator.sortAndComputeWindowFunctions;
import static org.hamcrest.Matchers.contains;

public class WindowFunctionBatchIteratorTest extends ESTestCase {

    @Test
    public void testWindowFunctionComputation() throws Exception {
        Iterable<Object[]> result = sortAndComputeWindowFunctions(
            Arrays.asList(
                new Object[]{"a", 1, null},
                new Object[]{"b", 7, null},
                new Object[]{"a", 4, null},
                new Object[]{"a", 8, null},
                new Object[]{"b", 2, null}
            ),
            (partitionStart, partitionEnd, currentIndex, sortedRows) -> 0,
            (partitionStart, partitionEnd, currentIndex, sortedRows) -> currentIndex,
            OrderingByPosition.arrayOrdering(0, false, false),
            OrderingByPosition.arrayOrdering(1, false, false),
            2,
            () -> 1,
            Runnable::run,
            List.of(new WindowFunction() {
                @Override
                public Object execute(int idxInPartition,
                                      WindowFrameState currentFrame,
                                      List<? extends CollectExpression<Row, ?>> expressions,
                                      Input... args) {
                    return idxInPartition + 1;
                }

                @Override
                public Signature signature() {
                    return null;
                }

                @Override
                public Signature boundSignature() {
                    return null;
                }
            }),
            Collections.emptyList(),
            new Input[][] { new Input[0] }
        ).get(5, TimeUnit.SECONDS);
        assertThat(
            result,
            contains(
                new Object[] { "a", 1, 1 },
                new Object[] { "a", 4, 2 },
                new Object[] { "a", 8, 3 },
                new Object[] { "b", 2, 1 },
                new Object[] { "b", 7, 2 }
            )
        );
    }

    @Test
    @Repeat (iterations = 100)
    public void testOptimizedFindFirstNonPeerMatchesBehaviorOfTrivial() {
        int length = randomIntBetween(10, 1000);
        ArrayList<Integer> numbers = new ArrayList<>(length);
        for (int i = 0; i < length; i++) {
            numbers.add(randomInt());
        }
        Comparator<Integer> comparingInt = Comparator.comparingInt(x -> x);
        numbers.sort(comparingInt);
        int begin = randomIntBetween(0, length - 1);
        int end = randomIntBetween(begin, length - 1);

        int expectedPosition = findFirstNonPeerTrivial(numbers, begin, end, comparingInt);
        assertThat(
            "Expected firstNonPeer position=" + expectedPosition + " for " + numbers + "[" + begin + ":" + end + "]",
            expectedPosition,
            Matchers.is(Lists2.findFirstNonPeer(numbers, begin, end, comparingInt)
            )
        );
    }

    private static <T> int findFirstNonPeerTrivial(List<T> rows, int begin, int end, Comparator<T> cmp) {
        T fst = rows.get(begin);
        for (int i = begin + 1; i < end; i++) {
            if (cmp.compare(fst, rows.get(i)) != 0) {
                return i;
            }
        }
        return end;
    }
}
