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

import io.crate.action.sql.SQLActionException;
import org.elasticsearch.cluster.metadata.MetaDataService;
import org.elasticsearch.test.ElasticsearchIntegrationTest;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.rules.TemporaryFolder;

import java.io.File;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Semaphore;

@ElasticsearchIntegrationTest.ClusterScope(scope = ElasticsearchIntegrationTest.Scope.SUITE, numDataNodes = 1)
public class BulkSettingsTest extends SQLTransportIntegrationTest {

    @Rule
    public ExpectedException expectedException = ExpectedException.none();

    @Rule
    public TemporaryFolder folder = new TemporaryFolder();

    @Test
    public void testBulkRequestPartitionedTimeoutCopy() throws Exception {
        execute("create table ttt (id int primary key, count long) partitioned by (id) with (number_of_replicas=0)");
        ensureYellow();

        File tmpFolder = folder.newFolder("folder");
        File file = new File(tmpFolder, "copyfrom.json");

        List<String> lines = Arrays.asList("{\"id\": 1, \"count\": 0}", "{\"id\":2, \"count\":0}", "{\"id\":3, \"count\":0}");
        Files.write(file.toPath(), lines, StandardCharsets.UTF_8);

        execute("set global transient bulk.partition_creation_timeout='1ms'");

        expectedException.expect(SQLActionException.class);
        expectedException.expectMessage("failed to process cluster event (acquire index lock) within 1ms");

        // locks are acquired here to make sure that the indices operation cannot finish within 1ms
        String masterName = internalCluster().getMasterName();
        MetaDataService metaDataService = internalCluster().getInstance(MetaDataService.class, masterName);
        Map<Semaphore, Collection<String>> semaphoreCollectionMap = metaDataService.indexMetaDataLocks(Arrays.asList(
                ".partitioned.ttt.04134", ".partitioned.ttt.04136", ".partitioned.ttt.04132"));
        for (Semaphore semaphore : semaphoreCollectionMap.keySet()) {
            semaphore.acquire();
        }

        try {
            execute("copy ttt from ?", new Object[]{file.getAbsolutePath()});
        } finally {
            execute("reset global bulk.partition_creation_timeout");
            for (Semaphore semaphore : semaphoreCollectionMap.keySet()) {
                semaphore.release();
            }
        }
    }
}
