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

package io.crate.analyze;

import io.crate.metadata.MetaDataModule;
import io.crate.operation.operator.OperatorModule;
import io.crate.testing.MockedClusterServiceModule;
import org.elasticsearch.common.inject.Module;
import org.elasticsearch.common.settings.Settings;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import java.util.Arrays;
import java.util.List;

import static org.hamcrest.Matchers.*;

public class SetAnalyzerTest extends BaseAnalyzerTest {

    @Rule
    public ExpectedException expectedException = ExpectedException.none();

    @Override
    protected List<Module> getModules() {
        List<Module> modules = super.getModules();
        modules.addAll(Arrays.<Module>asList(
                        new MockedClusterServiceModule(),
                        new MetaDataModule(),
                        new OperatorModule())
        );
        return modules;
    }

    @Test
    public void testSet() throws Exception {
        SetAnalyzedStatement analysis = analyze("SET GLOBAL PERSISTENT stats.operations_log_size=1");
        assertThat(analysis.isPersistent(), is(true));
        assertThat(analysis.settings().toDelimitedString(','), is("stats.operations_log_size=1,"));

        analysis = analyze("SET GLOBAL TRANSIENT stats.jobs_log_size=2");
        assertThat(analysis.isPersistent(), is(false));
        assertThat(analysis.settings().toDelimitedString(','), is("stats.jobs_log_size=2,"));

        analysis = analyze("SET GLOBAL TRANSIENT stats.enabled=false, stats.operations_log_size=0, stats.jobs_log_size=0");
        assertThat(analysis.isPersistent(), is(false));
        assertThat(analysis.settings(), is(Settings.builder()
                .put("stats.enabled", false)
                .put("stats.operations_log_size", 0)
                .put("stats.jobs_log_size", 0)
                .build()));
    }

    @Test
    public void testSetFullQualified() throws Exception {
        SetAnalyzedStatement analysis = analyze("SET GLOBAL PERSISTENT stats['operations_log_size']=1");
        assertThat(analysis.isPersistent(), is(true));
        assertThat(analysis.settings().toDelimitedString(','), is("stats.operations_log_size=1,"));
    }

    @Test
    public void testSetTimeValue() throws Exception {
        SetAnalyzedStatement analysis = analyze("SET GLOBAL PERSISTENT cluster.graceful_stop.timeout = 60000");
        assertThat(analysis.settings().toDelimitedString(','), is("cluster.graceful_stop.timeout=60000ms,"));

        analysis = analyze("SET GLOBAL PERSISTENT cluster.graceful_stop.timeout = '2.5m'");
        assertThat(analysis.settings().toDelimitedString(','), is("cluster.graceful_stop.timeout=150000ms,"));

        analysis = analyze("SET GLOBAL PERSISTENT cluster.graceful_stop.timeout = ?", new Object[]{ 1000.0 });
        assertThat(analysis.settings().toDelimitedString(','), is("cluster.graceful_stop.timeout=1000ms,"));
    }

    @Test
    public void testSetInvalidTimeValue() throws Exception {
        expectedException.expect(IllegalArgumentException.class);
        expectedException.expectMessage("Invalid value for argument 'cluster.graceful_stop.timeout'");
        analyze("SET GLOBAL PERSISTENT cluster.graceful_stop.timeout = '-1h'");
    }

    @Test
    public void testSetStringValue() throws Exception {
        SetAnalyzedStatement analysis = analyze("SET GLOBAL PERSISTENT cluster.graceful_stop.min_availability = 'full'");
        assertThat(analysis.settings().toDelimitedString(','), is("cluster.graceful_stop.min_availability=full,"));
    }

    @Test
    public void testSetInvalidStringValue() throws Exception {
        expectedException.expect(IllegalArgumentException.class);
        expectedException.expectMessage("'something' is not an allowed value. Allowed values are:");
        analyze("SET GLOBAL PERSISTENT cluster.graceful_stop.min_availability = 'something'");
    }

    @Test
    public void testSetInvalidStringValueObject() throws Exception {
        expectedException.expect(IllegalArgumentException.class);
        expectedException.expectMessage("Object values are not allowed at 'indices.store.throttle.type'");
        analyze("SET GLOBAL PERSISTENT \"indices.store.throttle.type\" = {foo='bar'}");
    }

    @Test
    public void testSetByteSizeValue() throws Exception {
        SetAnalyzedStatement analysis = analyze("SET GLOBAL PERSISTENT indices.recovery.file_chunk_size = '1024kb'");
        assertThat(analysis.settings().toDelimitedString(','), is("indices.recovery.file_chunk_size=1048576b,"));

        analysis = analyze("SET GLOBAL PERSISTENT indices.recovery.file_chunk_size = ?", new Object[]{ "1mb" });
        assertThat(analysis.settings().toDelimitedString(','), is("indices.recovery.file_chunk_size=1048576b,"));
    }

    @Test
    public void testSetInvalidByteSizeValue() throws Exception {
        expectedException.expect(IllegalArgumentException.class);
        expectedException.expectMessage("Invalid value for argument 'indices.recovery.file_chunk_size'");
        analyze("SET GLOBAL PERSISTENT indices.recovery.file_chunk_size = 'something'");
    }

    @Test
    public void testSetMemorySizeValue() throws Exception {
        SetAnalyzedStatement analysis = analyze("SET GLOBAL PERSISTENT \"indices.breaker.query.limit\" = '70%'");
        assertThat(analysis.settings().toDelimitedString(','), startsWith("indices.breaker.query.limit="));

        analysis = analyze("SET GLOBAL PERSISTENT \"indices.breaker.query.limit\" = '100mb'");
        assertThat(analysis.settings().toDelimitedString(','), is("indices.breaker.query.limit=100mb,"));

        analysis = analyze("SET GLOBAL PERSISTENT \"indices.breaker.query.limit\" = 1024");
        assertThat(analysis.settings().toDelimitedString(','), is("indices.breaker.query.limit=1kb,"));

        analysis = analyze("SET GLOBAL PERSISTENT \"indices.breaker.query.limit\" = ?", new Object[]{ "100mb" });
        assertThat(analysis.settings().toDelimitedString(','), is("indices.breaker.query.limit=100mb,"));
    }

    @Test
    public void testSetInvalidMemorySizeValue() throws Exception {
        expectedException.expect(IllegalArgumentException.class);
        expectedException.expectMessage("Invalid value for argument 'indices.breaker.query.limit'");
        analyze("SET GLOBAL PERSISTENT \"indices.breaker.query.limit\" = '80x'");

    }

    @Test
    public void testObjectValue() throws Exception {
        SetAnalyzedStatement analysis = analyze("SET GLOBAL PERSISTENT cluster.graceful_stop = {timeout='1h',force=false}");
        assertThat(analysis.settings().toDelimitedString(','), is("cluster.graceful_stop.force=false,cluster.graceful_stop.timeout=3600000ms,"));
    }

    @Test
    public void testNestedObjectValue() throws Exception {
        SetAnalyzedStatement analysis = analyze("SET GLOBAL PERSISTENT cluster.routing.allocation = {disk ={threshold_enabled = false, watermark = {high= '75%'}}}");
        assertThat(analysis.settings(), is(Settings.builder()
                .put("cluster.routing.allocation.disk.watermark.high", "75%")
                .put("cluster.routing.allocation.disk.threshold_enabled", false).build()));
    }

    @Test
    public void testSetRuntimeSettingSubscript() {
        SetAnalyzedStatement analysis = analyze("SET GLOBAL TRANSIENT cluster['routing']['allocation']['include'] = {_host = 'host1.example.com'}");
        assertThat(analysis.settings().toDelimitedString(','), is("cluster.routing.allocation.include._host=host1.example.com,"));
    }

    @Test
    public void testNestedObjectValueInvalid() throws Exception {
        expectedException.expect(IllegalArgumentException.class);
        expectedException.expectMessage("Only object values are allowed at 'cluster.routing.allocation.disk'");
        analyze("SET GLOBAL PERSISTENT cluster.routing.allocation = {disk = 1}");
    }

    @Test
    public void testSetParameter() throws Exception {
        SetAnalyzedStatement analysis = analyze("SET GLOBAL PERSISTENT stats.operations_log_size=?, stats.jobs_log_size=?", new Object[]{1, 2});
        assertThat(analysis.isPersistent(), is(true));
        assertThat(analysis.settings(), is(Settings.builder()
                .put("stats.operations_log_size", 1)
                .put("stats.jobs_log_size", 2).build()));
    }

    @Test
    public void testSetParameterInvalidType() throws Exception {
        expectedException.expect(IllegalArgumentException.class);
        expectedException.expectMessage("Invalid value for argument 'stats.operations_log_size'");
        analyze("SET GLOBAL PERSISTENT stats.operations_log_size=?", new Object[]{"foobar"});
    }

    @Test
    public void testSetParameterInvalidBooleanType() throws Exception {
        expectedException.expect(IllegalArgumentException.class);
        expectedException.expectMessage("Invalid value for argument 'stats.enabled'");
        analyze("SET GLOBAL PERSISTENT stats.enabled=?", new Object[]{"foobar"});
    }

    @Test
    public void testSetInvalidSetting() throws Exception {
        expectedException.expect(IllegalArgumentException.class);
        expectedException.expectMessage("setting 'forbidden' not supported");
        analyze("SET GLOBAL PERSISTENT forbidden=1");
    }

    @Test
    public void testSetInvalidValue() throws Exception {
        expectedException.expect(IllegalArgumentException.class);
        expectedException.expectMessage("Invalid value for argument 'stats.jobs_log_size'");
        analyze("SET GLOBAL TRANSIENT stats.jobs_log_size=-1");
    }

    @Test
    public void testSetInvalidValueType() throws Exception {
        expectedException.expect(IllegalArgumentException.class);
        expectedException.expectMessage("Invalid value for argument 'stats.jobs_log_size'");
        analyze("SET GLOBAL TRANSIENT stats.jobs_log_size='some value'");
    }

    @Test
    public void testSetInvalidBooleanValue() throws Exception {
        expectedException.expect(IllegalArgumentException.class);
        expectedException.expectMessage("Invalid value for argument 'stats.enabled'");
        analyze("SET GLOBAL TRANSIENT stats.enabled = 'hello'");
    }

    @Test
    public void testSetInvalidNumberValue() throws Exception {
        expectedException.expect(IllegalArgumentException.class);
        expectedException.expectMessage("Invalid value for argument 'stats.jobs_log_size'");
        analyze("SET GLOBAL TRANSIENT stats = { jobs_log_size = 'hello' }");
    }

    @Test
    public void testReset() throws Exception {
        ResetAnalyzedStatement analysis = analyze("RESET GLOBAL stats.enabled");
        assertThat(analysis.settingsToRemove(), contains("stats.enabled"));

        analysis = analyze("RESET GLOBAL stats");
        assertThat(analysis.settingsToRemove(), containsInAnyOrder("stats.enabled", "stats.jobs_log_size", "stats.operations_log_size"));
    }

    @Test
    public void testSetNonRuntimeSetting() {
        expectedException.expect(UnsupportedOperationException.class);
        expectedException.expectMessage("setting 'gateway.recover_after_time' cannot be set/reset at runtime");
        analyze("SET GLOBAL TRANSIENT gateway.recover_after_time = '5m'");
    }

    @Test
    public void testResetNonRuntimeSetting() {
        expectedException.expect(UnsupportedOperationException.class);
        expectedException.expectMessage("setting 'gateway.recover_after_nodes' cannot be set/reset at runtime");
        analyze("RESET GLOBAL gateway.recover_after_nodes");
    }

    @Test
    public void testSetNonRuntimeSettingObject() {
        expectedException.expect(UnsupportedOperationException.class);
        expectedException.expectMessage("setting 'gateway.recover_after_nodes' cannot be set/reset at runtime");
        analyze("SET GLOBAL TRANSIENT gateway = {recover_after_nodes = 3}");
    }

    @Test
    public void testResetNonRuntimeSettingObject() {
        expectedException.expect(UnsupportedOperationException.class);
        expectedException.expectMessage("setting 'gateway.recover_after_nodes' cannot be set/reset at runtime");
        analyze("RESET GLOBAL gateway");
    }

}
