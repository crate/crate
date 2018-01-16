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

package io.crate.execution.expression.reference.sys.check.cluster;

import io.crate.test.integration.CrateUnitTest;
import org.elasticsearch.Version;
import org.elasticsearch.cluster.metadata.IndexMetaData;
import org.elasticsearch.common.settings.Settings;
import org.junit.Test;

import static io.crate.execution.expression.reference.sys.check.cluster.TablesNeedRecreationSysCheck.isRecreationRequired;
import static org.hamcrest.Matchers.is;

public class TablesNeedRecreationSysCheckTest extends CrateUnitTest {

    @Test
    public void testRecreationRequired() {
        assertThat(isRecreationRequired("noMetaData", null), is(false));

        IndexMetaData recreationRequired = new IndexMetaData.Builder("testRecreationRequired")
            .settings(Settings.builder().put(IndexMetaData.SETTING_VERSION_CREATED, Version.V_2_4_2).build())
            .numberOfShards(5)
            .numberOfReplicas(2)
            .build();
        assertThat(isRecreationRequired("testRecreationRequired", recreationRequired), is(true));

        IndexMetaData noRecreationRequired = new IndexMetaData.Builder("testNoRecreationRequired")
            .settings(Settings.builder().put(IndexMetaData.SETTING_VERSION_CREATED, Version.V_5_0_1).build())
            .numberOfShards(5)
            .numberOfReplicas(2)
            .build();
        assertThat(isRecreationRequired("testNoRecreationRequired", noRecreationRequired), is(false));

        IndexMetaData noRecreationRequiredBlob = new IndexMetaData.Builder(".blob_testRecreationRequired")
            .settings(Settings.builder().put(IndexMetaData.SETTING_VERSION_CREATED, Version.V_2_4_2).build())
            .numberOfShards(5)
            .numberOfReplicas(2)
            .build();
        assertThat(isRecreationRequired(".blob_testRecreationRequired", noRecreationRequiredBlob), is(false));
    }
}
