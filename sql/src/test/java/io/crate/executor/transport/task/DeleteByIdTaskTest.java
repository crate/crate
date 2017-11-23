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

package io.crate.executor.transport.task;

import com.carrotsearch.hppc.IntIntHashMap;
import io.crate.executor.transport.ShardResponse;
import org.junit.Test;

import java.util.ArrayList;

import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertThat;

public class DeleteByIdTaskTest {

    @Test
    public void testBulkRowCountGeneration() throws Exception {
        ArrayList<ShardResponse> responses = new ArrayList<>();
        ShardResponse resp1 = new ShardResponse();
        resp1.add(0, null);
        resp1.add(2, null);
        responses.add(resp1);
        ShardResponse resp2 = new ShardResponse();
        resp2.add(1, null);
        responses.add(resp2);
        ShardResponse resp3 = new ShardResponse();
        resp2.add(3, null);
        responses.add(resp3);

        IntIntHashMap resultIdxByLocation = new IntIntHashMap();
        resultIdxByLocation.put(0, 0);
        resultIdxByLocation.put(2, 1);
        resultIdxByLocation.put(1, 0);
        resultIdxByLocation.put(3, 1);

        long[] rowCounts = DeleteByIdTask.getRowCounts(responses, 2, resultIdxByLocation);

        assertThat(rowCounts.length, is(2));
        assertThat(rowCounts[0], is(2L));
        assertThat(rowCounts[1], is(2L));
    }
}
