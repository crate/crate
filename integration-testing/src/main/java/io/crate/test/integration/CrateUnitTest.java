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

package io.crate.test.integration;

import com.carrotsearch.randomizedtesting.annotations.Listeners;
import com.carrotsearch.randomizedtesting.annotations.ThreadLeakLingering;
import com.carrotsearch.randomizedtesting.annotations.ThreadLeakScope;
import io.crate.test.GroovyTestSanitizer;
import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.core.Appender;
import org.apache.logging.log4j.core.LogEvent;
import org.apache.logging.log4j.core.appender.AbstractAppender;
import org.apache.logging.log4j.core.layout.PatternLayout;
import org.elasticsearch.common.logging.Loggers;
import org.elasticsearch.test.ESTestCase;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.rules.ExpectedException;
import org.mockito.MockitoAnnotations;

import java.util.ArrayList;
import java.util.Collection;

import static org.hamcrest.Matchers.empty;

@ThreadLeakScope(ThreadLeakScope.Scope.SUITE)
@ThreadLeakLingering(linger = 5000) // 5 sec lingering
@Listeners({SystemPropsTestLoggingListener.class})
public abstract class CrateUnitTest extends ESTestCase {

    private static final Collection<String> nettyLoggedLeaks = new ArrayList<>();

    static {
        GroovyTestSanitizer.isGroovySanitized();

        // Enable Netty leak detection and monitor logger for logged leak errors
        System.setProperty("io.netty.leakDetection.level", "paranoid");
        String leakLoggerName = "io.netty.util.ResourceLeakDetector";
        Logger leakLogger = LogManager.getLogger(leakLoggerName);
        Appender leakAppender = new AbstractAppender(leakLoggerName, null,
            PatternLayout.newBuilder().withPattern("%m").build()) {
            @Override
            public void append(LogEvent event) {
                String message = event.getMessage().getFormattedMessage();
                if (Level.ERROR.equals(event.getLevel()) && message.contains("LEAK:")) {
                    synchronized (nettyLoggedLeaks) {
                        nettyLoggedLeaks.add(message);
                    }
                }
            }
        };
        leakAppender.start();
        Loggers.addAppender(leakLogger, leakAppender);

        Runtime.getRuntime().addShutdownHook(new Thread(leakAppender::stop));
    }

    @Rule
    public ExpectedException expectedException = ExpectedException.none();

    @Before
    public void prepareMocks() throws Exception {
        MockitoAnnotations.initMocks(this);
    }

    @After
    public void checkForLeaks() throws Exception {
        synchronized (nettyLoggedLeaks) {
            try {
                assertThat(nettyLoggedLeaks, empty());
            } finally {
                nettyLoggedLeaks.clear();
            }
        }
    }

    protected static boolean isRunningOnWindows() {
        return System.getProperty("os.name").startsWith("Windows");
    }

    protected static boolean isRunningOnMacOSX() {
        return System.getProperty("os.name").equals("Mac OS X");
    }
}
