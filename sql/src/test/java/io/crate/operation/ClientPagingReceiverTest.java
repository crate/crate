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

package io.crate.operation;

import com.google.common.util.concurrent.MoreExecutors;
import com.google.common.util.concurrent.SettableFuture;
import io.crate.action.sql.FetchProperties;
import io.crate.core.collections.Bucket;
import io.crate.core.collections.Row;
import io.crate.core.collections.Row1;
import io.crate.executor.TaskResult;
import io.crate.testing.RowSender;
import io.crate.testing.TestingHelpers;
import io.crate.types.DataType;
import io.crate.types.DataTypes;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.TimeUnit;

import static org.hamcrest.core.Is.is;
import static org.junit.Assert.assertThat;

public class ClientPagingReceiverTest {

    private Iterable<Row> rows(int n) {
        List<Row> rows = new ArrayList<>(n);
        for (int i = 0; i < n; i++) {
            rows.add(new Row1(i));
        }
        return rows;
    }

    @Test
    public void testTriggersFutureIfFetchSizeIsReached() throws Exception {
        SettableFuture<TaskResult> future = SettableFuture.create();
        ClientPagingReceiver receiver = new ClientPagingReceiver(
            new FetchProperties(2, false), future, Collections.<DataType>singletonList(DataTypes.INTEGER));
        RowSender rowSender = new RowSender(rows(5), receiver, MoreExecutors.directExecutor());
        rowSender.run();

        TaskResult taskResult = future.get(2, TimeUnit.SECONDS); // shouldn't timeout
        assertThat(TestingHelpers.printedTable(taskResult.rows()), is("0\n1\n"));
        assertThat(rowSender.numPauses(), is(1));
    }

    @Test
    public void testFetchMoreAfterFirstResult() throws Exception {
        SettableFuture<TaskResult> future = SettableFuture.create();
        FetchProperties fetchProperties = new FetchProperties(2, false);
        ClientPagingReceiver receiver = new ClientPagingReceiver(
            fetchProperties, future, Collections.<DataType>singletonList(DataTypes.INTEGER));
        RowSender rowSender = new RowSender(rows(5), receiver, MoreExecutors.directExecutor());
        rowSender.run();

        final SettableFuture<Bucket> fetchFuture = SettableFuture.create();
        receiver.fetch(fetchProperties, new ClientPagingReceiver.FetchCallback() {

            @Override
            public void onResult(Bucket rows, boolean isLast) {
                fetchFuture.set(rows);
            }

            @Override
            public void onError(Throwable t) {

            }
        });
        fetchFuture.get(2, TimeUnit.SECONDS); // shouldn't timeout
    }
}
