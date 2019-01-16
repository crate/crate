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

package io.crate.analyze;

import io.crate.test.integration.CrateDummyClusterServiceUnitTest;
import io.crate.testing.SQLExecutor;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.metadata.MetaData;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.test.ClusterServiceUtils;
import org.junit.Before;
import org.junit.Test;

import static org.hamcrest.Matchers.hasItem;
import static org.hamcrest.Matchers.nullValue;

public class DropAnalyzerTest extends CrateDummyClusterServiceUnitTest {

    private SQLExecutor e;

    @Before
    public void setUpExecutor() throws Exception {
        Settings settings = Settings.builder()
            .put("crate.analysis.custom.analyzer.a1", "{\"index\":{\"analysis\":{\"analyzer\":{\"a1\":{\"type\":\"custom\",\"tokenizer\":\"lowercase\"}}}}}")

            .put("crate.analysis.custom.analyzer.a2",
                "{\"index\":{\"analysis\":{\"analyzer\":{\"a2\":{" +
                "\"tokenizer\":\"a2_mypattern\"," +
                "\"type\":\"custom\"}}}}}")
            .put("crate.analysis.custom.tokenizer.a2_mypattern",
                "{\"index\":{\"analysis\":{\"tokenizer\":{\"a2_mypattern\":{\"pattern\":\".*\",\"type\":\"pattern\"}}}}}")

            .put("crate.analysis.custom.analyzer.a3",
                "{\"index\":{\"analysis\":{\"analyzer\":{\"a3\":{" +
                "\"filter\":[\"a3_lowercase_german\",\"kstem\"]," +
                "\"type\":\"custom\"}}}}}")
            .put("crate.analysis.custom.filter.a3_lowercase_german",
                "{\"index\":{\"analysis\":{\"filter\":{\"a3_lowercase_german\":{\"type\":\"lowercase\",\"language\":\"german\"}}}}}")

            .put("crate.analysis.custom.analyzer.a4",
                "{\"index\":{\"analysis\":{\"analyzer\":{\"a4\":{" +
                "\"char_filter\":[\"html_strip\",\"a4_mymapping\"]," +
                "\"type\":\"custom\"}}}}}")
            .put("crate.analysis.custom.char_filter.a4_mymapping",
                "{\"index\":{\"analysis\":{\"char_filter\":{\"a4_mymapping\":{\"type\":\"mapping\",\"mappings\":[\"foo=>bar\"]}}}}}")

            .build();

        ClusterState clusterState = ClusterState.builder(clusterService.state())
            .metaData(MetaData.builder(clusterService.state().metaData())
                .persistentSettings(settings))
            .build();
        ClusterServiceUtils.setState(clusterService, clusterState);

        e = SQLExecutor.builder(clusterService).build();
    }

    private void assertIsMarkedToBeRemove(Settings settings, String settingName) {
        assertThat(settings.keySet(), hasItem(settingName));
        assertThat(settings.get(settingName), nullValue());
    }

    @Test
    public void testDropAnalyzer() {
        DropAnalyzerStatement analyzedStatement = e.analyze("drop analyzer a1");
        assertIsMarkedToBeRemove(analyzedStatement.settingsForRemoval(), "crate.analysis.custom.analyzer.a1");
    }

    @Test
    public void testDropAnalyzerWithCustomTokenizer() {
        DropAnalyzerStatement analyzedStatement = e.analyze("drop analyzer a2");
        assertIsMarkedToBeRemove(analyzedStatement.settingsForRemoval(), "crate.analysis.custom.analyzer.a2");
        assertIsMarkedToBeRemove(analyzedStatement.settingsForRemoval(), "crate.analysis.custom.tokenizer.a2_mypattern");
    }

    @Test
    public void testDropAnalyzerWithCustomTokenFilter() {
        DropAnalyzerStatement analyzedStatement = e.analyze("drop analyzer a3");
        assertIsMarkedToBeRemove(analyzedStatement.settingsForRemoval(), "crate.analysis.custom.analyzer.a3");
        assertIsMarkedToBeRemove(analyzedStatement.settingsForRemoval(), "crate.analysis.custom.filter.a3_lowercase_german");
    }

    @Test
    public void testDropAnalyzerWithCustomCharFilter() {
        DropAnalyzerStatement analyzedStatement = e.analyze("drop analyzer a4");
        assertIsMarkedToBeRemove(analyzedStatement.settingsForRemoval(), "crate.analysis.custom.analyzer.a4");
        assertIsMarkedToBeRemove(analyzedStatement.settingsForRemoval(), "crate.analysis.custom.char_filter.a4_mymapping");
    }
}
