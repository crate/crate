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

package io.crate.data;

import io.crate.testing.BatchIteratorTester;
import io.crate.testing.BatchSimulatingIterator;
import io.crate.testing.RowGenerator;
import org.junit.Test;

import java.util.Collections;

public class MergeCountBatchIteratorTest {

    private Iterable<Row> rows = RowGenerator.range(0, 10);

    @Test
    public void testMergeCountBatchIterator() throws Exception {
        BatchIteratorTester tester = new BatchIteratorTester(
            () -> MergeCountBatchIterator.newInstance(RowsBatchIterator.newInstance(rows)),
            Collections.singletonList(new Object[] { 45L })
        );
        tester.run();
    }

    @Test
    public void testMergeCountBatchIteratorWithBatchedSource() throws Exception {
        BatchIteratorTester tester = new BatchIteratorTester(
            () -> {
                return MergeCountBatchIterator.newInstance(
                    new BatchSimulatingIterator(RowsBatchIterator.newInstance(rows), 2, 5));
            },
            Collections.singletonList(new Object[] { 45L })
        );
        tester.run();
    }
}
