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
import org.junit.Test;

public class CrateSettingsTest extends CrateUnitTest {

    @Test
    public void testStringSettingsValidation() throws Exception {
        StringSetting stringSetting = new StringSetting(
                Sets.newHashSet("foo", "bar", "foobar")
        ) {
            @Override
            public String name() { return "foo_bar_setting"; }

            @Override
            public String defaultValue() { return "foo"; }

        };

        String validation = stringSetting.validate("foo");
        assertEquals(validation, null);
        validation = stringSetting.validate("unknown");
        assertEquals(validation, "'unknown' is not an allowed value. Allowed values are: bar, foo, foobar");
    }

    @Test
    public void testStringSettingsEmptyValidation() throws Exception {
        StringSetting stringSetting = new StringSetting(
                Sets.newHashSet("")
        ) {
            @Override
            public String name() { return "foo_bar_setting"; }

            @Override
            public String defaultValue() { return "foo"; }

        };

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
}
