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

package io.crate.test.integration;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.common.logging.Loggers;
import org.junit.runner.Description;
import org.junit.runner.Result;
import org.junit.runner.notification.RunListener;

import java.util.HashMap;
import java.util.Map;

/**
 * This listener is used to enable detailed test logging configuration like @TestLogging does
 * but from a system variable `tests.loggers.levels`. It's registered on top of LoggingListener of ES
 * and the settings specified are overriding the @TestLogging settings.
 */
public class SystemPropsTestLoggingListener extends RunListener {

    private static final String TESTS_LOGGERS_LEVELS_SYSTEM_PROPERTY_NAME = "tests.loggers.levels";
    private Map<String, String> previousLoggingMap;
    private Map<String, String> previousClassLoggingMap;
    private Map<String, String> previousPackageLoggingMap;

    @Override
    public void testRunStarted(Description description) throws Exception {
        previousPackageLoggingMap = processTestLogging();
        previousClassLoggingMap = processTestLogging();
    }

    @Override
    public void testRunFinished(Result result) throws Exception {
        previousClassLoggingMap = reset(previousClassLoggingMap);
        previousPackageLoggingMap = reset(previousPackageLoggingMap);
    }

    @Override
    public void testStarted(Description description) throws Exception {
        previousLoggingMap = processTestLogging();
    }

    @Override
    public void testFinished(Description description) throws Exception {
        previousLoggingMap = reset(previousLoggingMap);
    }

    private static Logger resolveLogger(String loggerName) {
        if (loggerName.equalsIgnoreCase("_root")) {
            return LogManager.getRootLogger();
        }
        return LogManager.getLogger(loggerName);
    }


    private Map<String, String> processTestLogging() {
        Map<String, String> map = getLoggersAndLevelsFromSystemProperty();
        if (map == null) {
            return null;
        }
        Map<String, String> previousValues = new HashMap<>();
        for (Map.Entry<String, String> entry : map.entrySet()) {
            Logger logger = resolveLogger(entry.getKey());
            previousValues.put(entry.getKey(), logger.getLevel().toString());
            Loggers.setLevel(logger, entry.getValue());
        }
        return previousValues;
    }

    private static Map<String, String> getLoggersAndLevelsFromSystemProperty() {
        String loggersLevelsVar = System.getProperty(TESTS_LOGGERS_LEVELS_SYSTEM_PROPERTY_NAME);
        if (loggersLevelsVar != null) {
            Map<String, String> map = new HashMap<>();
            String[] loggersAndLevels = loggersLevelsVar.split(",");
            for (String loggerAndLevel : loggersAndLevels) {
                String[] loggerAndLevelArray = loggerAndLevel.split(":");
                if (loggerAndLevelArray.length == 2) {
                    map.put(loggerAndLevelArray[0], loggerAndLevelArray[1]);
                } else {
                    throw new UnsupportedOperationException("Wrong format specified, " +
                                                            "please use: 'logger:level' format");
                }
            }
            return map;
        }
        return null;
    }

    private Map<String, String> reset(Map<String, String> map) {
        if (map != null) {
            for (Map.Entry<String, String> previousLogger : map.entrySet()) {
                Logger logger = resolveLogger(previousLogger.getKey());
                Loggers.setLevel(logger, previousLogger.getValue());
            }
        }
        return null;
    }
}
