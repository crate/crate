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
package org.elasticsearch.common.settings;

import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.common.logging.Loggers;
import org.elasticsearch.common.settings.Setting.Property;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.test.MockLogAppender;
import org.junit.Test;

import java.util.Arrays;
import java.util.function.Consumer;

public class SettingsFilterTests extends ESTestCase {

    @Test
    public void testMaskedSettingIsNotLogged() throws Exception {
        Settings oldSettings = Settings.builder().put("key", "old").build();
        Settings newSettings = Settings.builder().put("key", "new").build();

        Setting<String> filteredSetting = Setting.simpleString("key", Property.Masked);
        assertExpectedLogMessages((testLogger) -> Setting.logSettingUpdate(filteredSetting, newSettings, oldSettings, testLogger),
            new MockLogAppender.SeenEventExpectation("secure logging", "org.elasticsearch.test", Level.INFO, "updating [key]"),
            new MockLogAppender.UnseenEventExpectation("unwanted old setting name", "org.elasticsearch.test", Level.INFO, "*old*"),
            new MockLogAppender.UnseenEventExpectation("unwanted new setting name", "org.elasticsearch.test", Level.INFO, "*new*")
        );
    }

    @Test
    public void testRegularSettingUpdateIsFullyLogged() throws Exception {
        Settings oldSettings = Settings.builder().put("key", "old").build();
        Settings newSettings = Settings.builder().put("key", "new").build();

        Setting<String> regularSetting = Setting.simpleString("key");
        assertExpectedLogMessages((testLogger) -> Setting.logSettingUpdate(regularSetting, newSettings, oldSettings, testLogger),
            new MockLogAppender.SeenEventExpectation("regular logging", "org.elasticsearch.test", Level.INFO,
            "updating [key] from [old] to [new]"));
    }

    private void assertExpectedLogMessages(Consumer<Logger> consumer,
                                           MockLogAppender.LoggingExpectation ... expectations) throws IllegalAccessException {
        Logger testLogger = LogManager.getLogger("org.elasticsearch.test");
        MockLogAppender appender = new MockLogAppender();
        Loggers.addAppender(testLogger, appender);
        try {
            appender.start();
            Arrays.stream(expectations).forEach(appender::addExpectation);
            consumer.accept(testLogger);
            appender.assertAllExpectationsMatched();
        } finally {
            Loggers.removeAppender(testLogger, appender);
        }
    }
}
