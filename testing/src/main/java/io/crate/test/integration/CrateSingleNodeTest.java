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

package io.crate.test.integration;

import com.google.common.collect.Sets;
import org.elasticsearch.cluster.settings.ClusterDynamicSettings;
import org.elasticsearch.cluster.settings.DynamicSettings;
import org.elasticsearch.common.inject.Key;
import org.elasticsearch.test.ESSingleNodeTestCase;
import org.junit.After;

public class CrateSingleNodeTest extends ESSingleNodeTestCase {

    @Override
    @After
    public void tearDown() throws Exception {
        DynamicSettings dynamicSettings = node().injector().getInstance(Key.get(DynamicSettings.class, ClusterDynamicSettings.class));
        //dynamicSettings.addDynamicSetting("cluster_id");
        client().admin().cluster()
                .prepareUpdateSettings()
                .setTransientSettingsToRemove(Sets.newHashSet("cluster_id"))
                .execute().actionGet();
        super.tearDown();
    }
}
