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

package io.crate.exceptions;

import org.elasticsearch.test.ESTestCase;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;

import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.not;

public class MultiExceptionTest extends ESTestCase {

    @Test
    public void testGetMessageReturnsCombinedMessages() throws Exception {
        MultiException multiException = new MultiException(
            List.of(
                new Exception("first one"),
                new Exception("second one"))
        );
        assertThat(
            multiException.getMessage(),
            is("first one\nsecond one"));
    }

    @Test
    public void testMaxCharactersInMultiException() throws Exception {
        ArrayList<Exception> exceptions = new ArrayList<>();
        for (int i = 0; i < 10_000; i++) {
            exceptions.add(new Exception("exc"));
        }
        MultiException multiException = new MultiException(exceptions);
        assertThat(multiException.getMessage().length(), is(10038));
        assertThat(multiException.getMessage(), containsString("too much output. output truncated."));
    }

    @Test
    public void testMultiExceptionsAreFlattened() throws Exception {
        MultiException e1 = new MultiException(List.of(new Exception("exception 1"), new Exception("exception 2")));
        Exception e2 = new Exception("exception 3");

        MultiException multiException = MultiException.of(e1, e2);
        multiException.getExceptions().forEach(ex -> assertThat(ex, not(instanceOf(MultiException.class))));
    }
}
