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

package io.crate.testing;

import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.instanceOf;

import org.elasticsearch.common.settings.Settings;
import org.hamcrest.Matcher;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.function.Executable;

public class Asserts {

    public static SettingsAssert assertThat(Settings actual) {
        return new SettingsAssert(actual);
    }

    private Asserts() {}

    public static void assertThrowsMatches(Executable executable, Matcher<? super Throwable> matcher) {
        try {
            executable.execute();
            Assertions.fail("Expected exception to be thrown, but nothing was thrown.");
        } catch (Throwable t) {
            org.hamcrest.MatcherAssert.assertThat(t, matcher);
        }
    }

    public static void assertThrowsMatches(Executable executable, Class<? extends Throwable> type, String msgSubString) {
        assertThrowsMatches(executable, type, msgSubString,"Expected exception to be thrown, but nothing was thrown.");
    }

    public static void assertThrowsMatches(Executable executable,
                                           Class<? extends Throwable> type,
                                           String msgSubString,
                                           String assertionFailMsg) {
        try {
            executable.execute();
            Assertions.fail(assertionFailMsg);
        } catch (Throwable t) {
            org.hamcrest.MatcherAssert.assertThat(t, instanceOf(type));
            org.hamcrest.MatcherAssert.assertThat(t.getMessage(), containsString(msgSubString));
        }
    }
}
