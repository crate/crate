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

package io.crate.test;

import org.hamcrest.Description;
import org.hamcrest.TypeSafeMatcher;

import javax.annotation.Nullable;

public class CauseMatcher {

    public static org.hamcrest.Matcher cause(Class<? extends Throwable> type) {
        return cause(type, null);
    }

    public static org.hamcrest.Matcher cause(Class<? extends Throwable> type, String expectedMessage) {
        return new Matcher(type, expectedMessage);
    }

    public static org.hamcrest.Matcher causeOfCause(Class<? extends Throwable> type) {
        return causeOfCause(type, null);
    }

    public static org.hamcrest.Matcher causeOfCause(Class<? extends Throwable> type, String expectedMessage) {
        return CauseOfCauseMatcher.hasCause(new Matcher(type, expectedMessage));
    }

    private static class Matcher extends TypeSafeMatcher<Throwable> {

        private final Class<? extends Throwable> type;
        private final String expectedMessage;

        public Matcher(Class<? extends Throwable> type, @Nullable String expectedMessage) {
            this.type = type;
            this.expectedMessage = expectedMessage;
        }

        @Override
        protected boolean matchesSafely(Throwable item) {
            return item.getClass().isAssignableFrom(type)
                   && (null == expectedMessage || item.getMessage().contains(expectedMessage));
        }

        @Override
        public void describeTo(Description description) {
            description.appendText("expects type ").appendValue(type);
            if (expectedMessage != null) {
                description.appendText(" and a message ").appendValue(expectedMessage);
            }
        }
    }

    public static class CauseOfCauseMatcher<T extends Throwable> extends TypeSafeMatcher<T> {

        private final org.hamcrest.Matcher<T> fMatcher;

        public CauseOfCauseMatcher(org.hamcrest.Matcher<T> matcher) {
            fMatcher = matcher;
        }

        public void describeTo(Description description) {
            description.appendText("exception with cause of cause ");
            description.appendDescriptionOf(fMatcher);
        }

        @Override
        protected boolean matchesSafely(T item) {
            assert item.getCause() != null : "item.getCause() must not be null";
            return fMatcher.matches(item.getCause().getCause());
        }

        @Override
        protected void describeMismatchSafely(T item, Description description) {
            description.appendText("cause of cause ");
            fMatcher.describeMismatch(item.getCause().getCause(), description);
        }

        public static <T extends Throwable> org.hamcrest.Matcher<T> hasCause(final org.hamcrest.Matcher<T> matcher) {
            return new CauseOfCauseMatcher<T>(matcher);
        }
    }
}
