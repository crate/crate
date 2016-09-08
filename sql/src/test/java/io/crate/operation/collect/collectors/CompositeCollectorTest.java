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

import io.crate.core.collections.Bucket;
import io.crate.core.collections.CollectionBucket;
import io.crate.core.collections.Row;
import io.crate.operation.RowDownstream;
import io.crate.operation.collect.RowsCollector;
import io.crate.testing.CollectingRowReceiver;
import io.crate.testing.RowSender;
import io.crate.testing.TestingHelpers;
import org.junit.Test;

import java.util.Arrays;

import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertThat;

public class CompositeCollectorTest {

    @Test
    public void testRepeatEmitsRowsInTheSameOrder() throws Exception {
        CompositeCollector.Builder builder = new CompositeCollector.Builder();
        RowDownstream.Factory factory = builder.rowDownstreamFactory();

        CollectingRowReceiver rr = new CollectingRowReceiver();
        RowDownstream rowDownstream = factory.create(rr);

        Iterable<Row> leftRows = RowSender.rowRange(0, 15);
        Iterable<Row> rightRows = RowSender.rowRange(10, 30);

        RowsCollector c1 = new RowsCollector(rowDownstream.newRowReceiver(), leftRows);
        RowsCollector c2 = new RowsCollector(rowDownstream.newRowReceiver(), rightRows);

        CompositeCollector collector = builder.build(Arrays.asList(c1, c2));
        collector.doCollect();

        Bucket result = rr.result();
        assertThat(TestingHelpers.printedTable(result),
            is("0\n1\n2\n3\n4\n5\n6\n7\n8\n9\n10\n11\n12\n13\n14\n10\n11\n12\n13\n14\n15\n16\n17\n18\n19\n20\n21\n22\n23\n24\n25\n26\n27\n28\n29\n"));

        rr.repeatUpstream();
        assertThat(TestingHelpers.printedTable(new CollectionBucket(rr.rows)),
            is("0\n1\n2\n3\n4\n5\n6\n7\n8\n9\n10\n11\n12\n13\n14\n10\n11\n12\n13\n14\n15\n16\n17\n18\n19\n20\n21\n22\n23\n24\n25\n26\n27\n28\n29\n" +
               "0\n1\n2\n3\n4\n5\n6\n7\n8\n9\n10\n11\n12\n13\n14\n10\n11\n12\n13\n14\n15\n16\n17\n18\n19\n20\n21\n22\n23\n24\n25\n26\n27\n28\n29\n"));
    }
}
