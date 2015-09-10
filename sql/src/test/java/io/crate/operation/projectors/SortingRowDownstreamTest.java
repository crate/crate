/*
 * Licensed to CRATE Technology GmbH ("Crate") under one or more contributor
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

package io.crate.operation.projectors;

import com.carrotsearch.randomizedtesting.annotations.Repeat;
import com.google.common.collect.ImmutableList;
import io.crate.core.collections.Bucket;
import io.crate.core.collections.Row;
import io.crate.test.integration.CrateUnitTest;
import io.crate.testing.CollectingRowReceiver;
import io.crate.testing.RandomRows;
import io.crate.testing.RowSender;
import io.crate.testing.TestingHelpers;
import org.elasticsearch.common.util.concurrent.EsExecutors;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.util.Collections;
import java.util.List;
import java.util.concurrent.ThreadPoolExecutor;

import static org.hamcrest.Matchers.is;

public class SortingRowDownstreamTest extends CrateUnitTest {

    private ThreadPoolExecutor executor;

    @Before
    public void prepare() throws Exception {
        executor = EsExecutors.newFixed(5, 10, EsExecutors.daemonThreadFactory(getTestName()));
    }

    @After
    public void cleanUp() throws Exception {
        executor.shutdownNow();
    }

    @Test
    public void testBlockingSortingQueuedRowDownstreamThreaded() throws Exception {
        CollectingRowReceiver receiver = new CollectingRowReceiver();
        BlockingSortingQueuedRowDownstream projector = new BlockingSortingQueuedRowDownstream(
                receiver,
                1,
                new int[]{0},
                new boolean[]{false},
                new Boolean[]{null}
        );


        List<RowSender> upstreams = ImmutableList.of(
                new RowSender(new RandomRows(30), projector.newRowReceiver(), executor),
                new RowSender(new RandomRows(30), projector.newRowReceiver(), executor),
                new RowSender(new RandomRows(7), projector.newRowReceiver(), executor),
                new RowSender(new RandomRows(13), projector.newRowReceiver(), executor),
                new RowSender(Collections.<Row>emptyList(), projector.newRowReceiver(), executor)
        );
        for (RowSender upstream : upstreams) {
            executor.execute(upstream);
        }

        Bucket result = receiver.result();
        assertThat(result.size(), is(80));
        assertThat(result, TestingHelpers.isSorted(0));

        for (RowSender upstream : upstreams) {
            assertThat(upstream.isPaused(), is(false));
        }
    }

}
