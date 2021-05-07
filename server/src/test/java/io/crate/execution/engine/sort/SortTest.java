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

import com.carrotsearch.randomizedtesting.annotations.Repeat;
import org.elasticsearch.test.ESTestCase;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import static org.hamcrest.Matchers.is;

public class SortTest extends ESTestCase {

    private ExecutorService executor;

    @Before
    public void setUpExecutor() throws Exception {
        executor = Executors.newFixedThreadPool(4);
    }

    @After
    public void tearDownExecutor() throws Exception {
        executor.shutdown();
        executor.awaitTermination(5, TimeUnit.SECONDS);
    }

    @Test
    @Repeat(iterations = 50)
    public void testParallelSortYieldsTheSameResultAsListSort() throws Exception {
        int length = randomIntBetween(2, 1000);
        ArrayList<Integer> numbers = new ArrayList<>();
        for (int i = 0; i < length; i++) {
            numbers.add(randomInt());
        }
        var unsortedNumbers = new ArrayList<>(numbers);
        numbers.sort(Comparator.comparingInt(x -> x));
        assertThat(
            numbers,
            is(Sort.parallelSort(
                unsortedNumbers,
                Comparator.comparingInt(x -> x),
                randomIntBetween(1, 1000),
                randomIntBetween(1, 4),
                executor
                ).get(5, TimeUnit.SECONDS))
        );
    }
}
