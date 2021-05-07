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

package io.crate.execution.engine.fetch;

import com.carrotsearch.hppc.IntArrayList;
import com.carrotsearch.randomizedtesting.RandomizedRunner;
import org.junit.Test;
import org.junit.runner.RunWith;

import static org.elasticsearch.test.ESTestCase.randomIntBetween;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;

@RunWith(RandomizedRunner.class)
public class FetchCollectorTest {

    @Test
    public void test_sequential_docs_ids() {
        int start = randomIntBetween(0, Short.MAX_VALUE);
        IntArrayList sequential = new IntArrayList(10);
        for (int i = start; i < start + 10; i++) {
            sequential.add(i);
        }
        assertThat(FetchCollector.isSequential(sequential), is(true));

        IntArrayList nonSequential = new IntArrayList();
        nonSequential.add(10);
        nonSequential.add(2);
        nonSequential.add(48);
        assertThat(FetchCollector.isSequential(nonSequential), is(false));
    }
}
