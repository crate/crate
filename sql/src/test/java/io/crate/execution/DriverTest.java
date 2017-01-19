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

package io.crate.execution;

import io.crate.core.collections.Bucket;
import io.crate.core.collections.CollectionBucket;
import io.crate.testing.CollectingRowReceiver;
import io.crate.testing.TestingHelpers;
import org.junit.Test;

import java.util.Arrays;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.core.Is.is;

public class DriverTest {

    @Test
    public void testRun() throws Exception {
        DataSource dataSource = new StaticDataSource(
            new CollectionBucket(Arrays.asList(
                new Object[] { 1 },
                new Object[] { 2 },
                new Object[] { 3 },
                new Object[] { 4 },
                new Object[] { 5 },
                new Object[] { 6 },
                new Object[] { 7 },
                new Object[] { 8 },
                new Object[] { 9 },
                new Object[] { 10 }
            ))
        );
        CollectingRowReceiver receiver = new CollectingRowReceiver();

        Driver driver = new Driver(dataSource, receiver);
        driver.run();

        Bucket rows = receiver.result();
        assertThat(
            TestingHelpers.printedTable(rows),
            is("1\n2\n3\n4\n5\n6\n7\n8\n9\n10\n"));
    }

}
