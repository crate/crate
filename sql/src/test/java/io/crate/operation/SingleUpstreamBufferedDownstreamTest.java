/*
 * Licensed to CRATE.IO GmbH ("Crate") under one or more contributor
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

package io.crate.operation;

import io.crate.core.collections.Row;
import io.crate.core.collections.RowN;
import io.crate.jobs.ExecutionState;
import io.crate.operation.projectors.Requirement;
import io.crate.operation.projectors.Requirements;
import io.crate.operation.projectors.RowReceiver;
import io.crate.operation.projectors.SingleUpstreamBufferedDownstream;
import io.crate.test.integration.CrateUnitTest;
import io.crate.testing.CollectingRowReceiver;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

import static org.hamcrest.Matchers.is;

public class SingleUpstreamBufferedDownstreamTest extends CrateUnitTest {

    @Rule
    public ExpectedException expectedException = ExpectedException.none();

    private void produceRows(RowReceiver rowReceiver, int numRows) {
        RowN rowN = new RowN(1);
        for (int i = 0; i < numRows; i++) {
            rowN.cells(new Object[]{i});
            rowReceiver.setNextRow(rowN);
        }
    }

    @Test
    public void testRepeat() throws Exception {
        CollectingRowReceiver rowReceiver = new CollectingRowReceiver();
        SingleUpstreamBufferedDownstream rowDownstream = new SingleUpstreamBufferedDownstream(rowReceiver);
        RowReceiver singleUpstreamRowReceiver = rowDownstream.newRowReceiver();

        int numRows = 10;
        produceRows(singleUpstreamRowReceiver, numRows);
        singleUpstreamRowReceiver.finish();
        assertThat(rowReceiver.rows.size(), is(numRows));

        rowDownstream.repeat();
        assertThat(rowReceiver.rows.size(), is(numRows * 2));
    }

    @Test
    public void testRepeatIfNotFinished() throws Exception {
        expectedException.expect(IllegalStateException.class);
        expectedException.expectMessage("finished must be called before repeat");
        SingleUpstreamBufferedDownstream upstreamBufferedDownstream = new SingleUpstreamBufferedDownstream(new CollectingRowReceiver());
        produceRows(upstreamBufferedDownstream, 10);
        upstreamBufferedDownstream.repeat();
    }

    @Test
    public void testSetNextRowIfAlreadyFinished() throws Exception {
        expectedException.expect(IllegalStateException.class);
        expectedException.expectMessage("already finished");
        SingleUpstreamBufferedDownstream upstreamBufferedDownstream = new SingleUpstreamBufferedDownstream(new CollectingRowReceiver());
        produceRows(upstreamBufferedDownstream, 10);
        upstreamBufferedDownstream.finish();
        produceRows(upstreamBufferedDownstream, 10);
    }

    @Test
    public void testNewReceiverOnlyOnceAllowed() throws Exception {
        expectedException.expect(IllegalStateException.class);
        expectedException.expectMessage("newRowReceiver called more than once");

        RowDownstream downstream = new SingleUpstreamBufferedDownstream(new CollectingRowReceiver());
        downstream.newRowReceiver();
        downstream.newRowReceiver();
    }

    @Test
    public void testUpstreamCallsAreForwarded() throws Exception {
        final AtomicBoolean pauseCalled = new AtomicBoolean(false);
        final AtomicBoolean resumeCalled = new AtomicBoolean(false);
        RowUpstream rowUpstream = new RowUpstream() {
            @Override
            public void pause() {
                pauseCalled.set(true);
            }

            @Override
            public void resume(boolean async) {
                resumeCalled.set(true);
            }

            @Override
            public void repeat() {

            }
        };

        final AtomicReference<RowUpstream> passedUpstream = new AtomicReference<>();
        RowReceiver dummyRowReceiver = new RowReceiver() {
            @Override
            public boolean setNextRow(Row row) {
                return false;
            }

            @Override
            public void finish() {
            }

            @Override
            public void fail(Throwable throwable) {
            }

            @Override
            public void prepare(ExecutionState executionState) {
            }

            @Override
            public void setUpstream(RowUpstream rowUpstream) {
                passedUpstream.set(rowUpstream);
            }

            @Override
            public Set<Requirement> requirements() {
                return Requirements.NO_REQUIREMENTS;
            }
        };
        SingleUpstreamBufferedDownstream upstreamBufferedDownstream = new SingleUpstreamBufferedDownstream(dummyRowReceiver);
        upstreamBufferedDownstream.setUpstream(rowUpstream);
        passedUpstream.get().pause();
        assertThat(pauseCalled.get(), is(true));
        passedUpstream.get().resume(false);
        assertThat(resumeCalled.get(), is(true));
    }

}
