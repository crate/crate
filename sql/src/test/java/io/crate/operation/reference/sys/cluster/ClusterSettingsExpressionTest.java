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

package io.crate.operation.reference.sys.cluster;

import io.crate.core.collections.StringObjectMaps;
import io.crate.metadata.settings.CrateSettings;
import io.crate.plugin.SQLPlugin;
import io.crate.test.integration.CrateDummyClusterServiceUnitTest;
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.settings.Settings;
import org.junit.Test;

import java.util.Collection;
import java.util.Map;

import static org.hamcrest.core.Is.is;

public class ClusterSettingsExpressionTest extends CrateDummyClusterServiceUnitTest {

    @Override
    protected Collection<Setting<?>> additionalClusterSettings() {
        return new SQLPlugin(Settings.EMPTY).getSettings();
    }

    @Test
    public void testSettingsAreAppliedImmediately() throws Exception {
        ClusterSettingsExpression clusterSettingsExpression = new ClusterSettingsExpression(
            Settings.builder().put("bulk.request_timeout", "20s").build(), clusterService);

        assertThat(((BytesRef) clusterSettingsExpression
                .getChildImplementation("bulk")
                .getChildImplementation("request_timeout").value()).utf8ToString(),
            is("20s"));
    }


    @Test
    public void testSettingsAreUpdated() throws Exception {
        ClusterSettingsExpression expression = new ClusterSettingsExpression(Settings.EMPTY, clusterService);

        Settings.Builder builder = Settings.builder()
            .put(CrateSettings.STATS_JOBS_LOG_SIZE.settingName(), 1)
            .put(CrateSettings.STATS_ENABLED.settingName(), false)
            .put(CrateSettings.GRACEFUL_STOP_MIN_AVAILABILITY.settingName(), "full");
        clusterService.getClusterSettings().applySettings(builder.build());

        Map<String, Object> values = expression.value();
        assertThat(
            StringObjectMaps.getByPath(values, CrateSettings.STATS_JOBS_LOG_SIZE.settingName()), is(1));

        assertThat(
            StringObjectMaps.getByPath(values, CrateSettings.STATS_ENABLED.settingName()), is(false));

        assertThat(
            StringObjectMaps.getByPath(values, CrateSettings.GRACEFUL_STOP_MIN_AVAILABILITY.settingName()), is("full"));
    }
}
