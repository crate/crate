/*
 * Licensed to Crate.io GmbH ("Crate") under one or more contributor
 * license agreements.  See the NOTICE file distributed with this work for
 * additional information regarding copyright ownership.  Crate licenses
 * this file to you under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.  You may
 * obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
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

package io.crate.gradle;

import org.gradle.api.logging.Logger;
import org.gradle.api.logging.Logging;
import org.gradle.api.tasks.testing.TestDescriptor;
import org.gradle.api.tasks.testing.TestListener;
import org.gradle.api.tasks.testing.TestOutputEvent;
import org.gradle.api.tasks.testing.TestOutputListener;
import org.gradle.api.tasks.testing.TestResult;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class TestLogger implements TestListener, TestOutputListener {

    private static final Logger LOGGER = Logging.getLogger(TestLogger.class);

    private final Map<TestDescriptor, List<String>> testOutputs = new HashMap<>();

    @Override
    public void beforeSuite(TestDescriptor test) {
        LOGGER.lifecycle("Running: " + test);
    }

    @Override
    public void afterSuite(TestDescriptor testDescriptor, TestResult testResult) {
    }

    @Override
    public void beforeTest(TestDescriptor testDescriptor) {
    }

    @Override
    public void afterTest(TestDescriptor test, TestResult result) {
        if (result.getResultType() == TestResult.ResultType.FAILURE) {
            LOGGER.error("## FAILURE: " + test);
            for (String output : testOutputs.getOrDefault(test, List.of())) {
                System.out.println(output);
            }
            for (Throwable exception : result.getExceptions()) {
                exception.printStackTrace();
            }
        }
        testOutputs.remove(test);
    }

    @Override
    public void onOutput(TestDescriptor test, TestOutputEvent outputEvent) {
        testOutputs.compute(test, (key, val) -> {
            if (val == null) {
                val = new ArrayList<>();
            }
            val.add(outputEvent.getMessage());
            return val;
        });
    }
}
