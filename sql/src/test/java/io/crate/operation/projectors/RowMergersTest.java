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

package io.crate.operation.projectors;

import io.crate.operation.RowDownstream;
import io.crate.test.integration.CrateUnitTest;
import io.crate.testing.CollectingRowReceiver;
import io.crate.testing.RowSender;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import static org.hamcrest.core.Is.is;

public class RowMergersTest extends CrateUnitTest {

    private ExecutorService executorService;

    @Before
    public void initExecutorService() throws Exception {
        executorService = Executors.newFixedThreadPool(3);
    }

    @After
    public void stopExecutorService() throws Exception {
        executorService.shutdown();
        executorService.awaitTermination(30, TimeUnit.SECONDS);
    }

    @Test
    public void testRowMergerPauseResume() throws Exception {
        CollectingRowReceiver rowReceiver = CollectingRowReceiver.withPauseAfter(3);
        RowDownstream rowDownstream = RowMergers.passThroughRowMerger(rowReceiver);
        final RowSender r1 = new RowSender(RowSender.rowRange(0, 20), rowDownstream.newRowReceiver(), executorService);
        final RowSender r2 = new RowSender(RowSender.rowRange(0, 20), rowDownstream.newRowReceiver(), executorService);
        final RowSender r3 = new RowSender(RowSender.rowRange(0, 20), rowDownstream.newRowReceiver(), executorService);

        r1.run();
        r2.run();
        r3.run();

        assertBusy(new Runnable() {
            @Override
            public void run() {
                assertThat(r1.numPauses(), is(1));
                assertThat(r2.numPauses(), is(1));
                assertThat(r3.numPauses(), is(1));
            }
        });
        assertThat(rowReceiver.rows.size(), is(3));

        rowReceiver.resumeUpstream(true);
        assertThat(rowReceiver.result().size(), is(60));
    }

    @Test
    public void testPauseResumeWithOneEmptyUpstream() throws Exception {
        CollectingRowReceiver rowReceiver = CollectingRowReceiver.withPauseAfter(3);
        RowDownstream rowDownstream = RowMergers.passThroughRowMerger(rowReceiver);
        final RowSender r1 = new RowSender(RowSender.rowRange(0, 20), rowDownstream.newRowReceiver(), executorService);
        final RowSender r2 = new RowSender(RowSender.rowRange(0, 0), rowDownstream.newRowReceiver(), executorService);
        final RowSender r3 = new RowSender(RowSender.rowRange(0, 20), rowDownstream.newRowReceiver(), executorService);
        r1.run();
        r2.run();
        r3.run();

        assertBusy(new Runnable() {
            @Override
            public void run() {
                assertThat(r1.numPauses(), is(1));
                assertThat(r3.numPauses(), is(1));
                assertThat(r2.numPauses(), is(0));
            }
        });
        assertThat(rowReceiver.rows.size(), is(3));
        rowReceiver.resumeUpstream(true);
        assertThat(rowReceiver.result().size(), is(40));
    }
}
