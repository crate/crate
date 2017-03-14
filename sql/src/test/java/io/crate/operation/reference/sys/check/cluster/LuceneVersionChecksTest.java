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

package io.crate.operation.reference.sys.check.cluster;

import io.crate.test.integration.CrateUnitTest;
import org.elasticsearch.Version;
import org.elasticsearch.cluster.metadata.IndexMetaData;
import org.elasticsearch.common.settings.Settings;
import org.junit.Ignore;
import org.junit.Test;

import static org.hamcrest.Matchers.is;

public class LuceneVersionChecksTest extends CrateUnitTest {

    // FIXME: re-enable test after tests are rewritten to crate versions
    @Ignore
    @Test
    public void testRecreationRequired() {
        Version V_1_2_3 = Version.fromId(1020399);

        assertThat(LuceneVersionChecks.isRecreationRequired(null), is(false));
        IndexMetaData recreationRequired = new IndexMetaData.Builder("testRecreationRequired")
            .settings(Settings.builder().put(IndexMetaData.SETTING_VERSION_CREATED, V_1_2_3).build())
            .numberOfShards(5)
            .numberOfReplicas(2)
            .build();
        assertThat(LuceneVersionChecks.isRecreationRequired(recreationRequired), is(true));

        IndexMetaData noRecreationRequired = new IndexMetaData.Builder("testNoRecreationRequired")
            .settings(Settings.builder().put(IndexMetaData.SETTING_VERSION_CREATED, Version.V_2_0_0).build())
            .numberOfShards(5)
            .numberOfReplicas(2)
            .build();
        assertThat(LuceneVersionChecks.isRecreationRequired(noRecreationRequired), is(false));
    }

    @Test
    public void testUpgradeRequired() {
        assertThat(LuceneVersionChecks.isUpgradeRequired(null), is(false));
        assertThat(LuceneVersionChecks.isUpgradeRequired("4.9.0"), is(true));
        assertThat(LuceneVersionChecks.isUpgradeRequired("5.0.0"), is(true));
    }

    @Test
    public void testUpgradeRequiredInvalidArg() {
        expectedException.expect(IllegalArgumentException.class);
        expectedException.expectMessage("'invalidVersion' is not a valid Lucene version");
        LuceneVersionChecks.isUpgradeRequired("invalidVersion");
    }
}
