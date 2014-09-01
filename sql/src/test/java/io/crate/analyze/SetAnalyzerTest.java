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
import org.elasticsearch.common.inject.Module;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import java.util.Arrays;
import java.util.List;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;

public class SetAnalyzerTest extends BaseAnalyzerTest {

    @Rule
    public ExpectedException expectedException = ExpectedException.none();

    @Override
    protected List<Module> getModules() {
        List<Module> modules = super.getModules();
        modules.addAll(Arrays.<Module>asList(
                        new TestModule(),
                        new MetaDataModule(),
                        new OperatorModule())
        );
        return modules;
    }

    @Test
    public void testSet() throws Exception {
        SetAnalysis analysis = (SetAnalysis) analyze("SET GLOBAL PERSISTENT stats.operations_log_size=1");
        assertThat(analysis.isPersistent(), is(true));
        assertThat(analysis.settings().toDelimitedString(','), is("stats.operations_log_size=1,"));

        analysis = (SetAnalysis) analyze("SET GLOBAL TRANSIENT stats.jobs_log_size=2");
        assertThat(analysis.isPersistent(), is(false));
        assertThat(analysis.settings().toDelimitedString(','), is("stats.jobs_log_size=2,"));

        analysis = (SetAnalysis) analyze("SET GLOBAL TRANSIENT stats.enabled=false, stats.operations_log_size=0, stats.jobs_log_size=0");
        assertThat(analysis.isPersistent(), is(false));
        assertThat(analysis.settings().toDelimitedString(','), is("stats.enabled=false,stats.operations_log_size=0,stats.jobs_log_size=0,"));
    }

    @Test
    public void testSetFullQualified() throws Exception {
        SetAnalysis analysis = (SetAnalysis) analyze("SET GLOBAL PERSISTENT stats['operations_log_size']=1");
        assertThat(analysis.isPersistent(), is(true));
        assertThat(analysis.settings().toDelimitedString(','), is("stats.operations_log_size=1,"));
    }

    @Test
    public void testSetTimeValue() throws Exception {
        SetAnalysis analysis = (SetAnalysis) analyze("SET GLOBAL PERSISTENT cluster.graceful_stop.timeout = 60000");
        assertThat(analysis.settings().toDelimitedString(','), is("cluster.graceful_stop.timeout=60000,"));

        analysis = (SetAnalysis) analyze("SET GLOBAL PERSISTENT cluster.graceful_stop.timeout = '2.5m'");
        assertThat(analysis.settings().toDelimitedString(','), is("cluster.graceful_stop.timeout=150000,"));

        analysis = (SetAnalysis) analyze("SET GLOBAL PERSISTENT cluster.graceful_stop.timeout = ?", new Object[]{ 1000.0 });
        assertThat(analysis.settings().toDelimitedString(','), is("cluster.graceful_stop.timeout=1000,"));
    }

    @Test
    public void testSetInvalidTimeValue() throws Exception {
        expectedException.expect(IllegalArgumentException.class);
        expectedException.expectMessage("Invalid value for argument 'cluster.graceful_stop.timeout'");
        analyze("SET GLOBAL PERSISTENT cluster.graceful_stop.timeout = '-1h'");
    }

    @Test
    public void testSetStringValue() throws Exception {
        SetAnalysis analysis = (SetAnalysis) analyze("SET GLOBAL PERSISTENT cluster.graceful_stop.min_availability = 'full'");
        assertThat(analysis.settings().toDelimitedString(','), is("cluster.graceful_stop.min_availability=full,"));
    }

    @Test
    public void testSetInvalidStringValue() throws Exception {
        expectedException.expect(IllegalArgumentException.class);
        expectedException.expectMessage("'something' is not an allowed value. Allowed values are: primaries, none, full");
        analyze("SET GLOBAL PERSISTENT cluster.graceful_stop.min_availability = 'something'");
    }

    @Test
    public void testSetByteSizeValue() throws Exception {
        SetAnalysis analysis = (SetAnalysis) analyze("SET GLOBAL PERSISTENT indices.recovery.file_chunk_size = '1024kb'");
        assertThat(analysis.settings().toDelimitedString(','), is("indices.recovery.file_chunk_size=1048576,"));

        analysis = (SetAnalysis) analyze("SET GLOBAL PERSISTENT indices.recovery.file_chunk_size = ?", new Object[]{ "1mb" });
        assertThat(analysis.settings().toDelimitedString(','), is("indices.recovery.file_chunk_size=1048576,"));
    }

    @Test
    public void testSetInvalidByteSizeValue() throws Exception {
        expectedException.expect(IllegalArgumentException.class);
        expectedException.expectMessage("Invalid value for argument 'indices.recovery.file_chunk_size'");
        analyze("SET GLOBAL PERSISTENT indices.recovery.file_chunk_size = 'something'");
    }

    @Test
    public void testObjectValue() throws Exception {
        SetAnalysis analysis = (SetAnalysis) analyze("SET GLOBAL PERSISTENT cluster.graceful_stop = {timeout='1h',force=false}");
        assertThat(analysis.settings().toDelimitedString(','), is("cluster.graceful_stop={force=false, timeout=1h},"));
    }

    @Test
    public void testSetParameter() throws Exception {
        SetAnalysis analysis = (SetAnalysis) analyze("SET GLOBAL PERSISTENT stats.operations_log_size=?, stats.jobs_log_size=?", new Object[]{1, 2});
        assertThat(analysis.isPersistent(), is(true));
        assertThat(analysis.settings().toDelimitedString(','), is("stats.operations_log_size=1,stats.jobs_log_size=2,"));
    }

    @Test(expected = IllegalArgumentException.class)
    public void testSetParameterInvalidType() throws Exception {
        analyze("SET GLOBAL PERSISTENT stats.operations_log_size=?", new Object[]{"foobar"});
    }

    @Test(expected = IllegalArgumentException.class)
    public void testSetParameterInvalidBooleanType() throws Exception {
        analyze("SET GLOBAL PERSISTENT stats.collect_stats=?", new Object[]{"foobar"});
    }

    @Test(expected = IllegalArgumentException.class)
    public void testSetInvalidSetting() throws Exception {
        analyze("SET GLOBAL PERSISTENT forbidden=1");
    }

    @Test(expected = IllegalArgumentException.class)
    public void testSetInvalidValue() throws Exception {
        analyze("SET GLOBAL TRANSIENT stats.jobs_log_size=-1");
    }

    @Test(expected = IllegalArgumentException.class)
    public void testSetInvalidValueType() throws Exception {
        analyze("SET GLOBAL TRANSIENT stats.jobs_log_size='some value'");
    }
}
