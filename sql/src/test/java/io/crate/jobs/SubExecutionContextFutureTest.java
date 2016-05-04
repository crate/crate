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

package io.crate.jobs;

import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import java.util.List;

import static org.hamcrest.Matchers.isA;

public class SubExecutionContextFutureTest {

    @Rule
    public ExpectedException expectedException = ExpectedException.none();

    @Test
    public void testCloseWithCancellationExceptionAsPartOfCombinedFuture() throws Exception {
        expectedException.expectCause(isA(InterruptedException.class));

        SubExecutionContextFuture f1 = new SubExecutionContextFuture();
        SubExecutionContextFuture f2 = new SubExecutionContextFuture();

        // close with InterruptedException results in a cancel call on a internal future.
        // cancel on one future causes the other futures to be canceled too
        f1.close(new InterruptedException());

        ListenableFuture<List<SubExecutionContextFuture.State>> asList = Futures.allAsList(f1, f2);
        asList.get();
    }
}
