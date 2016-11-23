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

package io.crate.integrationtests;

import com.google.common.base.Throwables;
import io.crate.blob.v2.BlobIndicesService;
import io.crate.plugin.BlobPlugin;
import io.crate.rest.CrateRestFilter;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.node.Node;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.test.ESIntegTestCase;
import org.hamcrest.Matchers;
import org.junit.After;

import java.lang.reflect.Field;
import java.util.Collection;
import java.util.Map;

public abstract class BlobIntegrationTestBase extends ESIntegTestCase {

    @Override
    protected Settings nodeSettings(int nodeOrdinal) {
        return Settings.settingsBuilder()
            .put(super.nodeSettings(nodeOrdinal))
            .put(Node.HTTP_ENABLED, true)
            .put(CrateRestFilter.ES_API_ENABLED_SETTING, true)
            .build();
    }

    @SuppressWarnings("unchecked")
    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        return pluginList(BlobPlugin.class);
    }

    @After
    public void assertNoIndicesRemaining() throws Exception {
        internalCluster().wipeIndices("_all");
        Iterable<BlobIndicesService> blobIndicesServices = internalCluster().getInstances(BlobIndicesService.class);
        Field indicesField = BlobIndicesService.class.getDeclaredField("indices");
        indicesField.setAccessible(true);
        assertBusy(() -> {
            for (BlobIndicesService blobIndicesService : blobIndicesServices) {
                Map<String, Object> indices = null;
                try {
                    indices = (Map<String, Object>) indicesField.get(blobIndicesService);
                    assertThat(indices.keySet(), Matchers.empty());
                } catch (IllegalAccessException e) {
                    throw Throwables.propagate(e);
                }
            }
        });
    }
}
