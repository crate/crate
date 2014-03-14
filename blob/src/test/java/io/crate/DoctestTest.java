/*
 * Licensed to CRATE Technology GmbH ("Crate") under one or more contributor
 * license agreements.  See the NOTICE file distributed with this work for
 * additional information regarding copyright ownership.  Crate licenses
 * this file to you under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.  You may
 * obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
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

package io.crate;

import io.crate.blob.v2.BlobIndices;
import io.crate.core.NumberOfReplicas;
import io.crate.rest.CrateRestFilter;
import io.crate.test.integration.CrateIntegrationTest;
import io.crate.test.integration.DoctestClusterTestCase;
import org.elasticsearch.common.settings.ImmutableSettings;
import org.elasticsearch.common.settings.Settings;
import org.junit.Test;


@CrateIntegrationTest.ClusterScope(numNodes = 2, scope = CrateIntegrationTest.Scope.SUITE)
public class DoctestTest extends DoctestClusterTestCase {

    @Override
    protected Settings nodeSettings(int nodeOrdinal) {
        // enable es api for integration tests
        return ImmutableSettings.builder()
                .put(CrateRestFilter.ES_API_ENABLED_SETTING, true)
                .build();
    }

    @Test
    public void testBlob() throws Exception {

        BlobIndices blobIndices = cluster().getInstance(BlobIndices.class);

        blobIndices.createBlobTable("test", new NumberOfReplicas(0), 2).get();
        blobIndices.createBlobTable("test_blobs2", new NumberOfReplicas(0), 2).get();

        client().admin().indices().prepareCreate("test_no_blobs")
            .setSettings(
                ImmutableSettings.builder()
                .put("number_of_shards", 2)
                .put("number_of_replicas", 0).build()).execute().actionGet();

        execDocFile("integrationtests/blob.rst", getClass());
    }

}
