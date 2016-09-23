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

package io.crate.metadata.settings;

import com.google.common.collect.Sets;
import io.crate.test.integration.CrateUnitTest;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

public class CrateSettingsTest extends CrateUnitTest {

    @Rule
    public ExpectedException expectedException = ExpectedException.none();

    @Test
    public void testStringSettingsValidation() throws Exception {
        StringSetting stringSetting =
            new StringSetting("foo_bar_setting", Sets.newHashSet("foo", "bar", "foobar"), false, "foo", null);

        String validation = stringSetting.validate("foo");
        assertEquals(validation, null);
        validation = stringSetting.validate("unknown");
        assertEquals(validation, "'unknown' is not an allowed value. Allowed values are: bar, foo, foobar");
    }

    @Test
    public void testStringSettingsEmptyValidation() throws Exception {
        StringSetting stringSetting = new StringSetting("foo_bar_setting", Sets.newHashSet(""), false, "foo", null);

        String validation = stringSetting.validate("foo");
        assertEquals(validation, "'foo' is not an allowed value. Allowed values are: ");
        validation = stringSetting.validate("");
        assertEquals(validation, null);
    }

    @Test
    public void testCrateStringSettingsDefaultValues() throws Exception {
        assertEquals(CrateSettings.GRACEFUL_STOP_MIN_AVAILABILITY.validate(
            CrateSettings.GRACEFUL_STOP_MIN_AVAILABILITY.defaultValue()
        ), null);
        assertEquals(CrateSettings.ROUTING_ALLOCATION_ENABLE.validate(
            CrateSettings.ROUTING_ALLOCATION_ENABLE.defaultValue()
        ), null);
    }

    @Test
    public void testIntSettingsValidation() throws Exception {
        IntSetting intSetting = new IntSetting("integerSetting", 10, false, 0, 100);
        SettingsAppliers.IntSettingsApplier intSettingsApplier = new SettingsAppliers.IntSettingsApplier(intSetting);
        Integer toValidate = 10;
        Object validatedObject = intSettingsApplier.validate(toValidate);
        assertEquals(toValidate, validatedObject);
    }

    @Test
    public void testIntSettingsValidationFailure() throws Exception {
        IntSetting intSetting = new IntSetting("integerSetting", 10, false, 5, 10);
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
        Boolean toValidate = Boolean.TRUE;
        Object validatedObject = booleanSettingsApplier.validate(toValidate);
        assertEquals(toValidate, validatedObject);
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
}
