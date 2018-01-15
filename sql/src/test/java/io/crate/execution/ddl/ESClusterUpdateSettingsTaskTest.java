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

package io.crate.execution.ddl;

import com.google.common.collect.ImmutableList;
import io.crate.data.Row;
import io.crate.data.RowN;
import io.crate.sql.tree.Expression;
import io.crate.sql.tree.ParameterExpression;
import io.crate.sql.tree.StringLiteral;
import io.crate.test.integration.CrateUnitTest;
import org.elasticsearch.common.collect.MapBuilder;
import org.elasticsearch.common.settings.Settings;
import org.junit.Test;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.hamcrest.core.Is.is;

public class ESClusterUpdateSettingsTaskTest extends CrateUnitTest {

    @Test
    public void testUpdateSettingsWithStringValue() throws Exception {
        Map<String, List<Expression>> settings = new HashMap<String, List<Expression>>() {{
            put("cluster.graceful_stop.min_availability", ImmutableList.of(new StringLiteral("full")));
        }};
        Settings expected = Settings.builder()
            .put("cluster.graceful_stop.min_availability", "full")
            .build();
        assertThat(ESClusterUpdateSettingsTask.buildSettingsFrom(settings, Row.EMPTY), is(expected));
    }

    @Test
    public void testUpdateMultipleSettingsWithParameters() throws Exception {
        Map<String, List<Expression>> settings = new HashMap<String, List<Expression>>() {{
            put("stats.operations_log_size", ImmutableList.of(new ParameterExpression(1)));
            put("stats.jobs_log_size", ImmutableList.of(new ParameterExpression(2)));
        }};
        Settings expected = Settings.builder()
            .put("stats.operations_log_size", 10)
            .put("stats.jobs_log_size", 25)
            .build();
        assertThat(
            ESClusterUpdateSettingsTask.buildSettingsFrom(settings, new RowN(new Object[]{10, 25})),
            is(expected)
        );
    }

    @Test
    public void testUpdateObjectWithParameter() throws Exception {
        Map<String, List<Expression>> settings = new HashMap<String, List<Expression>>() {{
            put("stats", ImmutableList.of(new ParameterExpression(1)));
        }};
        Map<String, Object> param = MapBuilder.<String, Object>newMapBuilder()
            .put("enabled", true)
            .put("breaker",
                MapBuilder.newMapBuilder()
                .put("log", MapBuilder.newMapBuilder()
                    .put("jobs", MapBuilder.newMapBuilder()
                        .put("overhead", 1.05d).map()
                    ).map()
                ).map()
            ).map();

        Settings expected = Settings.builder()
            .put("stats.enabled", true)
            .put("stats.breaker.log.jobs.overhead", 1.05d)
            .build();
        assertThat(ESClusterUpdateSettingsTask.buildSettingsFrom(settings, new RowN(new Object[]{param})), is(expected));
    }

    @Test
    public void testUnsupportedSetting() throws Exception {
        expectedException.expect(IllegalArgumentException.class);
        expectedException.expectMessage("Setting 'unsupported_setting' is not supported");

        Map<String, List<Expression>> settings = new HashMap<String, List<Expression>>() {{
            put("unsupported_setting", ImmutableList.of(new StringLiteral("foo")));
        }};
        ESClusterUpdateSettingsTask.buildSettingsFrom(settings, Row.EMPTY);
    }
}
