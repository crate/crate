/*
 * Licensed to CRATE Technology GmbH ("Crate") under one or more contributor
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

package io.crate.stress;

import io.crate.TimestampFormat;
import io.crate.action.sql.SQLBulkResponse;
import io.crate.concurrent.Threaded;
import org.apache.commons.lang3.RandomStringUtils;
import org.junit.Test;

import java.util.concurrent.atomic.AtomicInteger;

import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.nullValue;

public class ConcurrentInsertNewPartitionsStressTest extends AbstractIntegrationStressTest {

    private static final long TS = TimestampFormat.parseTimestampString("2015-01-01");

    private static final AtomicInteger PARTITION_INDEX = new AtomicInteger(0);

    @Override
    public void prepareFirst() throws Exception {
        execute("create table motiondata (\n" +
                "  d string,\n" +
                "  device_id string,\n" +
                "  ts timestamp,\n" +
                "  ax double,\n" +
                "  primary key (d, device_id, ts)\n" +
                ")\n" +
                "partitioned by (d)\n" +
                "clustered by (device_id)");
        ensureGreen();
    }

    private Object[] getRandomObject() {
        int partitionIdx = PARTITION_INDEX.getAndIncrement();
        return new Object[]{
                String.valueOf(partitionIdx),
                RandomStringUtils.randomAlphabetic(1),
                TS + partitionIdx,
                5.0};
    }


    @Threaded(count=4)
    @Test
    public void testCreateNewPartitionsWithBulkInsert() throws Exception {
        long inserted = 0;
        long errors = 0;

        int numRows = 50;
        Object[][] bulkArgs = new Object[numRows][];
        for (int i = 0; i < numRows; i++) {
            bulkArgs[i] = getRandomObject();
        }
        SQLBulkResponse response = execute("insert into motiondata (d, device_id, ts, ax) values (?,?,?,?)", bulkArgs);
        for (SQLBulkResponse.Result result : response.results()) {
            assertThat(result.errorMessage(), is(nullValue()));
            if (result.rowCount() < 0) {
                errors++;
            } else {
                inserted += result.rowCount();
            }
        }
        assertThat(errors, is(0L));
        assertThat(inserted, is((long)numRows));
    }
}
