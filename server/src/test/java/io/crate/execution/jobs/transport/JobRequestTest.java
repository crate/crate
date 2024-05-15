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

package io.crate.execution.jobs.transport;

import static org.assertj.core.api.Assertions.assertThat;
import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertThat;

import java.util.Collections;
import java.util.UUID;

import org.elasticsearch.common.io.stream.BytesStreamOutput;
import org.junit.Test;

import io.crate.metadata.SearchPath;
import io.crate.metadata.settings.SessionSettings;

public class JobRequestTest {

    @Test
    public void testJobRequestStreaming() throws Exception {
        JobRequest r1 = JobRequest.of("dummyNodeId",
                                                 UUID.randomUUID(),
                                                 new SessionSettings("dummyUser",
                                                                     SearchPath.createSearchPathFrom("dummySchema")),
                                                 "n1",
                                                 Collections.emptyList(),
                                                 true).innerRequest();

        BytesStreamOutput out = new BytesStreamOutput();
        r1.writeTo(out);

        JobRequest r2 = new JobRequest(out.bytes().streamInput());

        assertThat(r1.coordinatorNodeId(), is(r2.coordinatorNodeId()));
        assertThat(r1.jobId(), is(r2.jobId()));
        assertThat(r1.sessionSettings(), is(r2.sessionSettings()));
        assertThat(r1.nodeOperations().isEmpty()).isTrue();
        assertThat(r1.enableProfiling(), is(r2.enableProfiling()));
    }
}
