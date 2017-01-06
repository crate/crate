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
import io.crate.core.collections.Buckets;
import io.crate.core.collections.CollectionBucket;
import io.crate.core.collections.Row1;
import io.crate.testing.TestingHelpers;
import org.junit.Test;

import java.util.Arrays;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;

public class NLDataSourceTest {

    @Test
    public void testNLSinglePage() throws Exception {
        DataSource left = new StaticDataSource(
            new CollectionBucket(Arrays.asList(
                new Object[] { 1 },
                new Object[] { 2 },
                new Object[] { 3 }
            ))
        );
        DataSource right = new StaticDataSource(
            new CollectionBucket(Arrays.asList(
                new Object[] { 10 },
                new Object[] { 20 }
            ))
        );

        NLDataSource nlDataSource = new NLDataSource(left, right);
        RowReceiver rr = new RowReceiver();
        Driver driver = new Driver(nlDataSource, rr);
        driver.run();

        assertThat(TestingHelpers.printedTable(rr.result()),
            is("1| 10\n" +
               "1| 20\n" +
               "2| 10\n" +
               "2| 20\n" +
               "3| 10\n" +
               "3| 20\n"
            ));
    }

    @Test
    public void testNLMultiplePages() throws Exception {
        DataSource left = new StaticDataSource(
            Buckets.of(new Row1(1)),
            Buckets.of(new Row1(2)));

        DataSource right = new StaticDataSource(
            new CollectionBucket(Arrays.asList(
                new Object[] { 10 },
                new Object[] { 20 }
            )),
            Buckets.of(new Row1(30))
        );

        NLDataSource nlDataSource = new NLDataSource(left, right);

        RowReceiver rr = new RowReceiver();
        Driver driver = new Driver(nlDataSource, rr);
        driver.run();

        Bucket result = rr.result();
        assertThat(TestingHelpers.printedTable(result),
            is("1| 10\n" +
               "1| 20\n" +
               "1| 30\n" +
               "2| 10\n" +
               "2| 20\n" +
               "2| 30\n"
            ));
    }

}
