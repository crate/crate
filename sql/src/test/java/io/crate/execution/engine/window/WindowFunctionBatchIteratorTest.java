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

package io.crate.execution.engine.window;

import io.crate.data.Input;
import io.crate.data.Row;
import io.crate.execution.engine.collect.CollectExpression;
import io.crate.metadata.FunctionInfo;
import org.junit.Test;

import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;

import static io.crate.execution.engine.window.WindowFunctionBatchIterator.computeWindowFunctions;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.contains;

public class WindowFunctionBatchIteratorTest {

    @Test
    public void testWindowFunctionComputation() {
        List<Object[]> list = computeWindowFunctions(
            Arrays.asList(
                new Object[]{"a", 1, null},
                new Object[]{"b", 7, null},
                new Object[]{"a", 4, null},
                new Object[]{"a", 8, null},
                new Object[]{"b", 2, null}
            ),
            Comparator.comparing(cells -> (Comparable) cells[0]),
            Comparator.comparing(cells -> (Comparable) cells[1]),
            2,
            Arrays.asList(new WindowFunction() {
                @Override
                public Object execute(int idxInPartition,
                                      WindowFrameState currentFrame,
                                      List<? extends CollectExpression<Row, ?>> expressions,
                                      Input... args) {
                    return idxInPartition + 1;
                }

                @Override
                public FunctionInfo info() {
                    return null;
                }
            }),
            Collections.emptyList(),
            new Input[][] { new Input[0] }
        );
        assertThat(
            list,
            contains(
                new Object[] { "a", 1, 1 },
                new Object[] { "a", 4, 2 },
                new Object[] { "a", 8, 3 },
                new Object[] { "b", 2, 1 },
                new Object[] { "b", 7, 2 }
            )
        );
    }
}
