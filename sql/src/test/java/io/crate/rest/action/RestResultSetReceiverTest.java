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

package io.crate.rest.action;

import io.crate.breaker.RamAccountingContext;
import io.crate.breaker.RowAccounting;
import org.elasticsearch.common.breaker.NoopCircuitBreaker;
import org.elasticsearch.rest.AbstractRestChannel;
import org.elasticsearch.rest.RestChannel;
import org.elasticsearch.rest.RestResponse;
import org.elasticsearch.test.rest.FakeRestRequest;
import org.junit.Test;

import java.util.Collections;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;

public class RestResultSetReceiverTest {

    @Test
    public void testCompletionFutureIsCompletedAfterChannelErrors() {
        RestChannel restChannel = new ErroneousChannel();
        RestResultSetReceiver restResultSetReceiver = new RestResultSetReceiver(
            restChannel,
            Collections.emptyList(),
            0,
            new RowAccounting(Collections.emptyList(),
                new RamAccountingContext("context", new NoopCircuitBreaker("test"))),
            false
        );

        restResultSetReceiver.allFinished(true);
        assertThat(restResultSetReceiver.completionFuture().isCompletedExceptionally(), is(true));
    }

    private static class ErroneousChannel extends AbstractRestChannel {

        ErroneousChannel() {
            super(new FakeRestRequest(), false);
        }

        @Override
        public void sendResponse(RestResponse response) {
            throw new RuntimeException("Failing to test error handling.");
        }
    }
}
