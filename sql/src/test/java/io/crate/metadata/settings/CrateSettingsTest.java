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

package io.crate.metadata.settings;

import com.google.common.collect.Sets;
import io.crate.data.Row;
import io.crate.sql.tree.Expression;
import io.crate.sql.tree.Literal;
import io.crate.test.integration.CrateUnitTest;
import org.elasticsearch.common.settings.Settings;
import org.hamcrest.core.Is;
import org.junit.Test;

import java.util.HashMap;

import static org.hamcrest.core.Is.is;
import static org.hamcrest.core.IsNull.notNullValue;
import static org.hamcrest.core.IsNull.nullValue;

public class CrateSettingsTest extends CrateUnitTest {

    @Test
    public void testStringSettingsValidation() throws Exception {
        StringSetting stringSetting =
            new StringSetting("foo_bar_setting", Sets.newHashSet("foo", "bar", "foobar"), false, "foo", null);

        String validation = stringSetting.validate("foo");
        assertThat(validation, is(nullValue()));
        validation = stringSetting.validate("unknown");
        assertThat(validation, is("'unknown' is not an allowed value. Allowed values are: bar, foo, foobar"));
    }

    @Test
    public void testStringSettingsEmptyValidation() throws Exception {
        StringSetting stringSetting = new StringSetting("foo_bar_setting", Sets.newHashSet(""), false, "foo", null);

        String validation = stringSetting.validate("foo");
        assertThat(validation, is("'foo' is not an allowed value. Allowed values are: "));
        validation = stringSetting.validate("");
        assertThat(validation, is(nullValue()));
    }

    @Test
    public void testCrateStringSettingsDefaultValues() throws Exception {
        assertThat(CrateSettings.GRACEFUL_STOP_MIN_AVAILABILITY.validate(
            CrateSettings.GRACEFUL_STOP_MIN_AVAILABILITY.defaultValue()
        ), is(nullValue()));
        assertThat(CrateSettings.ROUTING_ALLOCATION_ENABLE.validate(
            CrateSettings.ROUTING_ALLOCATION_ENABLE.defaultValue()
        ), is(nullValue()));
    }

    @Test
    public void testIntSettingsValidation() throws Exception {
        IntSetting intSetting = new IntSetting("integerSetting", 10, false, 0, 100, null, null);
        SettingsAppliers.IntSettingsApplier intSettingsApplier = new SettingsAppliers.IntSettingsApplier(intSetting);
        Integer toValidate = 10;
        Object validatedObject = intSettingsApplier.validate(toValidate);
        assertThat(toValidate, is(validatedObject));
    }

    @Test
    public void testIntSettingsValidationFailure() throws Exception {
        IntSetting intSetting = new IntSetting("integerSetting", 10, false, 5, 10, null, null);
        SettingsAppliers.IntSettingsApplier intSettingsApplier = new SettingsAppliers.IntSettingsApplier(intSetting);

        expectedException.expect(IllegalArgumentException.class);
        expectedException.expectMessage("Invalid value for argument 'integerSetting'");
        intSettingsApplier.validate(100);
    }

    @Test
    public void testBooleanSettingsValidation() throws Exception {
        BoolSetting booleanSetting = new BoolSetting("booleanSetting", false, false);
        SettingsAppliers.BooleanSettingsApplier booleanSettingsApplier
            = new SettingsAppliers.BooleanSettingsApplier(booleanSetting);
        Object validatedObject = booleanSettingsApplier.validate(Boolean.TRUE);
        assertThat(validatedObject, Is.<Object>is(true));
    }

    @Test
    public void testBooleanSettingsValidationFailure() throws Exception {
        BoolSetting booleanSetting = new BoolSetting("booleanSetting", false, false);
        SettingsAppliers.BooleanSettingsApplier booleanSettingsApplier
            = new SettingsAppliers.BooleanSettingsApplier(booleanSetting);

        expectedException.expect(IllegalArgumentException.class);
        expectedException.expectMessage("Can't convert \"Wrong value\" to boolean");
        booleanSettingsApplier.validate("Wrong value");
    }

    @Test
    public void testApplyInvalidValueOnByteCrateSetting() throws Exception {
        expectedException.expect(IllegalArgumentException.class);
        expectedException.expectMessage("Invalid value for argument 'indices.recovery.max_bytes_per_sec'");
        applySetting(CrateSettings.INDICES_RECOVERY_MAX_BYTES_PER_SEC.settingName(), Literal.fromObject("something"));
    }

    @Test
    public void testApplyObjectValueOnIntCrateSetting() throws Exception {
        expectedException.expect(IllegalArgumentException.class);
        expectedException.expectMessage("Invalid value for argument 'stats.operations_log_size'");
        applySetting("stats.operations_log_size", Literal.fromObject(new Object[]{"foo"}));
    }

    @Test
    public void testApplyInvalidValueOnObjectBooleanCrateSetting() throws Exception {
        expectedException.expect(IllegalArgumentException.class);
        expectedException.expectMessage("Invalid value for argument 'stats.enabled'");
        applySetting("stats", Literal.fromObject(new HashMap<String, String>() {{
            put("enabled", "bar");
        }}));
    }

    @Test
    public void testSetInvalidSetting() throws Exception {
        expectedException.expect(IllegalArgumentException.class);
        expectedException.expectMessage("setting 'forbidden' not supported");
        applySetting("forbidden", Literal.fromObject(1));
    }

    @Test
    public void testApplyInvalidValueNumberCrateSetting() throws Exception {
        expectedException.expect(IllegalArgumentException.class);
        expectedException.expectMessage("Invalid value for argument 'stats.jobs_log_size'");
        applySetting("stats.jobs_log_size", Literal.fromObject(-1));
    }

    @Test
    public void testApplyInvalidValueOnBooleanCrateSetting() throws Exception {
        expectedException.expect(IllegalArgumentException.class);
        expectedException.expectMessage("Invalid value for argument 'stats.enabled'");
        applySetting("stats.enabled", Literal.fromObject("bar"));
    }

    @Test
    public void testApplyInvalidValueOnObjectIntCrateSetting() throws Exception {
        expectedException.expect(IllegalArgumentException.class);
        expectedException.expectMessage("Invalid value for argument 'stats.jobs_log_size'");
        applySetting("stats.jobs_log_size", Literal.fromObject(new HashMap<String, String>() {{
            put("jobs_log_size", "foo");
        }}));
    }

    @Test
    public void testApplyInvalidObjectCrateSetting() throws Exception {
        expectedException.expect(IllegalArgumentException.class);
        expectedException.expectMessage("Object values are not allowed at 'indices.store.throttle.type'");
        applySetting("indices.store.throttle.type", Literal.fromObject(new HashMap<String, String>() {{
            put("foo", "bar");
        }}));
    }

    @Test
    public void testApplyInvalidOnMemoryCrateSetting() throws Exception {
        expectedException.expect(IllegalArgumentException.class);
        expectedException.expectMessage("Invalid value for argument 'indices.breaker.query.limit'");
        applySetting("indices.breaker.query.limit", Literal.fromObject("80x"));
    }

    @Test
    public void testApplyObjectValueOnStringCrateSetting() throws Exception {
        expectedException.expect(IllegalArgumentException.class);
        expectedException.expectMessage("Only object values are allowed at 'cluster.routing.allocation.disk'");
        applySetting("cluster.routing.allocation", Literal.fromObject(new HashMap<String, Integer>() {{
            put("disk", 1);
        }}));
    }

    @Test
    public void testApplyNotAllowedValueOnStringCrateSetting() {
        expectedException.expect(IllegalArgumentException.class);
        expectedException.expectMessage("\'INF\' is not an allowed value. Allowed values are: TRACE, DEBUG, INFO, WARN, ERROR");
        applySetting("logger.action", Literal.fromObject("INF"));
    }

    @Test
    public void testApplyStringCrateSetting() throws Exception {
        StringSetting stringSetting = CrateSettings.GRACEFUL_STOP_MIN_AVAILABILITY;
        SettingsAppliers.StringSettingsApplier stringSettingsApplier =
            new SettingsAppliers.StringSettingsApplier(stringSetting);

        Settings.Builder settings = Settings.builder();
        stringSettingsApplier.apply(settings, Row.EMPTY, Literal.fromObject("full"));
        assertThat(settings.get("cluster.graceful_stop.min_availability"), is("full"));
    }

    @Test
    public void testApplyByteCrateSetting() throws Exception {
        ByteSizeSetting byteSizeSetting = CrateSettings.INDICES_RECOVERY_MAX_BYTES_PER_SEC;
        SettingsAppliers.ByteSizeSettingsApplier intSettingsApplier =
            new SettingsAppliers.ByteSizeSettingsApplier(byteSizeSetting);

        Settings.Builder settings = Settings.builder();
        intSettingsApplier.apply(settings, Row.EMPTY, Literal.fromObject("1024kb"));
        assertThat(settings.get(byteSizeSetting.settingName()), is("1048576b"));

        settings = Settings.builder();
        intSettingsApplier.apply(settings, Row.EMPTY, Literal.fromObject("1mb"));
        assertThat(settings.get(byteSizeSetting.settingName()), is("1048576b"));
    }

    @Test
    public void testApplyTimeCrateSetting() throws Exception {
        TimeSetting timeSetting = CrateSettings.GRACEFUL_STOP_TIMEOUT;
        SettingsAppliers.TimeSettingsApplier timeSettingsApplier =
            new SettingsAppliers.TimeSettingsApplier(timeSetting);

        Settings.Builder settings = Settings.builder();
        timeSettingsApplier.apply(settings, Row.EMPTY, Literal.fromObject("2m"));
        assertThat(settings.get("cluster.graceful_stop.timeout"), is("120000ms"));

        settings = Settings.builder();
        timeSettingsApplier.apply(settings, Row.EMPTY, Literal.fromObject(1000));
        assertThat(settings.get("cluster.graceful_stop.timeout"), is("1000ms"));
    }

    @Test
    public void testApplyInvalidTimeCrateSetting() throws Exception {
        TimeSetting timeSetting = CrateSettings.GRACEFUL_STOP_TIMEOUT;
        SettingsAppliers.TimeSettingsApplier timeSettingsApplier =
            new SettingsAppliers.TimeSettingsApplier(timeSetting);

        expectedException.expect(IllegalArgumentException.class);
        expectedException.expectMessage("Invalid value for argument 'cluster.graceful_stop.timeout'");
        timeSettingsApplier.apply(Settings.builder(), Row.EMPTY, Literal.fromObject("-1h"));
    }

    @Test
    public void testApplyMemoryCrateSetting() throws Exception {
        SettingsAppliers.MemoryValueSettingsApplier intSettingsApplier =
            new SettingsAppliers.MemoryValueSettingsApplier(CrateSettings.INDICES_BREAKER_QUERY_LIMIT);

        Settings.Builder settings = Settings.builder();
        intSettingsApplier.apply(settings, Row.EMPTY, Literal.fromObject("70%"));
        assertThat(settings.get("indices.breaker.query.limit"), is(notNullValue()));

        settings = Settings.builder();
        intSettingsApplier.apply(settings, Row.EMPTY, Literal.fromObject("100mb"));
        assertThat(settings.get("indices.breaker.query.limit"), is("100mb"));

        settings = Settings.builder();
        intSettingsApplier.apply(settings, Row.EMPTY, Literal.fromObject(1024));
        assertThat(settings.get("indices.breaker.query.limit"), is("1kb"));
    }

    private void applySetting(String setting, Expression expression) {
        Settings.Builder settings = Settings.builder();
        SettingsApplier settingsApplier = CrateSettings.getSettingsApplier(setting);
        settingsApplier.apply(settings, Row.EMPTY, expression);
        assertThat(settings.build().get(setting), is(notNullValue()));
    }

    @Test
    public void testPercentageByteSettingsApplier() {
        StringSetting setting = CrateSettings.ROUTING_ALLOCATION_DISK_WATERMARK_HIGH;
        SettingsAppliers.PercentageOrAbsoluteByteSettingApplier applier =
            new SettingsAppliers.PercentageOrAbsoluteByteSettingApplier(setting);
        assertThat(applier.validate("12mb"), is("12mb"));
        assertThat(applier.validate("12%"), is("12%"));
    }

    public void testPercentageOrAbsoluteByteSettingApplierInvalidPercentageSettingValue() {
        StringSetting setting = CrateSettings.ROUTING_ALLOCATION_DISK_WATERMARK_LOW;
        SettingsAppliers.PercentageOrAbsoluteByteSettingApplier applier =
            new SettingsAppliers.PercentageOrAbsoluteByteSettingApplier(setting);

        expectedException.expect(SettingsAppliers.InvalidSettingValueContentException.class);
        expectedException.expectMessage("percentage should be in [0-100], got [1444]");
        applier.validate("1444%");
    }

    @Test
    public void testPercentageOrAbsoluteByteSettingApplierInvalidByteSettingValue() {
        StringSetting setting = CrateSettings.ROUTING_ALLOCATION_DISK_WATERMARK_LOW;
        SettingsAppliers.PercentageOrAbsoluteByteSettingApplier applier =
            new SettingsAppliers.PercentageOrAbsoluteByteSettingApplier(setting);

        expectedException.expect(SettingsAppliers.InvalidSettingValueContentException.class);
        expectedException.expectMessage("failed to parse [12mbb] as a byte size value");
        applier.validate("12mbb");
    }
}
