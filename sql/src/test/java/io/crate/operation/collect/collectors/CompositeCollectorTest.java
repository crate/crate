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

package io.crate.operation.collect.collectors;

import io.crate.data.Bucket;
import io.crate.data.Row;
import io.crate.operation.collect.CrateCollector;
import io.crate.operation.collect.RowsCollector;
import io.crate.operation.projectors.RepeatHandle;
import io.crate.operation.projectors.RowReceiver;
import io.crate.test.integration.CrateUnitTest;
import io.crate.testing.CollectingRowReceiver;
import io.crate.testing.RowGenerator;
import io.crate.testing.TestingHelpers;
import org.junit.Test;

import javax.annotation.Nullable;
import java.util.Arrays;

import static org.hamcrest.core.Is.is;
import static org.mockito.Matchers.isNull;
import static org.mockito.Mockito.*;

public class CompositeCollectorTest extends CrateUnitTest {


    private CrateCollector.Builder mockBuilder(final CrateCollector mock) {
        return new CrateCollector.Builder() {
            @Override
            public CrateCollector build(RowReceiver rowReceiver) {
                return mock;
            }
        };
    }

    @Test
    public void testKill() throws Exception {
        CollectingRowReceiver rr = new CollectingRowReceiver();
        CrateCollector.Builder builder = new CrateCollector.Builder() {

            @Override
            public CrateCollector build(final RowReceiver rowReceiver) {
                return new CrateCollector() {
                    @Override
                    public void doCollect() {
                        rowReceiver.finish(RepeatHandle.UNSUPPORTED);
                    }

                    @Override
                    public void kill(@Nullable Throwable throwable) {
                        throw new AssertionError("Kill should not be called on finished collector");
                    }
                };
            }
        };

        CrateCollector c2 = mock(CrateCollector.class);
        CrateCollector c3 = mock(CrateCollector.class);

        CompositeCollector collector = new CompositeCollector(Arrays.asList(builder, mockBuilder(c2), mockBuilder(c3)), rr);

        collector.doCollect();
        collector.kill(null);

        verify(c2, times(1)).doCollect();
        verify(c2, times(1)).kill(isNull(Throwable.class));
        verify(c3, never()).doCollect();
        verify(c3, never()).kill(isNull(Throwable.class));
    }

    @Test
    public void testStop() throws Exception {
        CollectingRowReceiver rr = CollectingRowReceiver.withLimit(3);

        Iterable<Row> leftRows = RowGenerator.range(0, 5);
        Iterable<Row> rightRows = RowGenerator.range(5, 10);

        CrateCollector.Builder c1 = RowsCollector.builder(leftRows);
        CrateCollector.Builder c2 = RowsCollector.builder(rightRows);

        CompositeCollector collector = new CompositeCollector(Arrays.asList(c1, c2), rr);
        collector.doCollect();

        Bucket result = rr.result();
        assertThat(TestingHelpers.printedTable(result), is("0\n1\n2\n"));
    }
}
