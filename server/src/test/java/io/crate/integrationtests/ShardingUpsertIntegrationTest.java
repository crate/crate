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

package io.crate.integrationtests;

import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.test.ESIntegTestCase;
import org.junit.Test;

import java.nio.file.Paths;

@ESIntegTestCase.ClusterScope(numDataNodes = 1)
public class ShardingUpsertIntegrationTest extends SQLTransportIntegrationTest {

    @Override
    protected Settings nodeSettings(int nodeOrdinal) {
        return Settings.builder()
            .put(super.nodeSettings(nodeOrdinal))
            .put("thread_pool.write.queue_size", 1)
            .build();
    }

    @Test
    public void testCopyFromWithLimitedBulkSize() throws Exception {
        execute(
            "create table contributors (" +
            "   id integer," +
            "   day_joined timestamp with time zone," +
            "   bio string," +
            "   name string ," +
            "   address object(strict) as (" +
            "       country string," +
            "       city string " +
            "   )" +
            ") clustered into 1 shards with (number_of_replicas=0)");
        ensureYellow();

        String copyFilePath = Paths.get(getClass().getResource("/essetup/data/best_practice").toURI()).toUri().toString();
        execute("copy contributors from ? with (bulk_size = 1)", new Object[]{
            copyFilePath + "data_import.json"});
        assertEquals(150L, response.rowCount());
        refresh();
        execute("select id, day_joined, name from contributors");
        assertEquals(150L, response.rowCount());
    }
}
