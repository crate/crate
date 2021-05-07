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

import org.hamcrest.Matcher;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.function.Executable;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.instanceOf;

public class Asserts {

    private Asserts() {}

    public static void assertThrows(Executable executable, Matcher<? super Throwable> matcher) {
        try {
            executable.execute();
            Assertions.fail("Expected exception to be thrown, but nothing was thrown.");
        } catch (Throwable t) {
            assertThat(t, matcher);
        }
    }

    public static void assertThrows(Executable executable, Class<? extends Throwable> type, String subString) {
        try {
            executable.execute();
            Assertions.fail("Expected exception to be thrown, but nothing was thrown.");
        } catch (Throwable t) {
            assertThat(t, instanceOf(type));
            assertThat(t.getMessage(), containsString(subString));
        }
    }
}
