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

package io.crate.ssl;

import io.crate.protocols.ssl.SslResourcePollIntervalParser;
import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.common.logging.Loggers;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.test.MockLogAppender;
import org.junit.Test;

import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertThat;

public class SslResourcePollIntervalParserTest {

    private static final Logger LOGGER = Loggers.getLogger(SslResourcePollIntervalParserTest.class);

    @Test
    public void testPollIntervalParserExpectedIsParsed() {
        var p = new SslResourcePollIntervalParser(LOGGER);
        assertThat(p.apply("2s"), is(TimeValue.timeValueSeconds(2)));
        assertThat(p.apply("10s"), is(TimeValue.timeValueSeconds(10)));
        assertThat(p.apply("30s"), is(TimeValue.timeValueSeconds(30)));
    }

    @Test
    public void testPollIntervalParserLogsOnSmallerThanMinimum() throws Exception {
        String expectedLogMsg = "Value 100ms is smaller than the minimum effective value of 2s, will use 2s instead.";
        assertExpectedLogMessagesAndParsedValue("100ms",
                                                TimeValue.timeValueSeconds(2),
                                                expectedLogMsg);
    }

    @Test
    public void testPollIntervalParserLogsOnSmallerThanMedium() throws Exception {
        String expectedLogMsg = "Value 5s is smaller than the next effective value of 10s, will use 10s instead.";
        assertExpectedLogMessagesAndParsedValue("5s",
                                                TimeValue.timeValueSeconds(10),
                                                expectedLogMsg);
    }

    @Test
    public void testPollIntervalParserLogsOnSmallerThanLow() throws Exception {
        String expectedLogMsg = "Value 13s is smaller than the next effective value of 30s, will use 30s instead.";
        assertExpectedLogMessagesAndParsedValue("13s",
                                                TimeValue.timeValueSeconds(30),
                                                expectedLogMsg);
    }

    @Test
    public void testPollIntervalParserLogsOnGreaterThanLow() throws Exception {
        String expectedLogMsg = "Value 50s is greater than the maximum effective value of 30s, will use 30s instead.";
        assertExpectedLogMessagesAndParsedValue("50s",
                                                TimeValue.timeValueSeconds(30),
                                                expectedLogMsg);
    }


    private <T> void assertExpectedLogMessagesAndParsedValue(String value,
                                                             T expectedParsedValue,
                                                             String expectedLogMessage) throws IllegalAccessException {
        String loggerName = "org.elasticsearch.test";
        Logger testLogger = LogManager.getLogger(loggerName);
        MockLogAppender appender = new MockLogAppender();
        Loggers.addAppender(testLogger, appender);

        var expectation = new MockLogAppender.SeenEventExpectation("warn logging",
                                                 loggerName,
                                                 Level.WARN,
                                                 expectedLogMessage);
        try {
            appender.start();
            appender.addExpectation(expectation);
            var p = new SslResourcePollIntervalParser(testLogger);
            assertThat(p.apply(value), is(expectedParsedValue));
            appender.assertAllExpectationsMatched();
        } finally {
            Loggers.removeAppender(testLogger, appender);
        }
    }

}
