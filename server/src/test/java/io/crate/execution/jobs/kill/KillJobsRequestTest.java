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

package io.crate.execution.jobs.kill;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.List;
import java.util.UUID;

import org.elasticsearch.common.io.stream.BytesStreamOutput;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.test.ESTestCase;
import org.junit.Test;

public class KillJobsRequestTest extends ESTestCase {

    @Test
    public void testStreaming() throws Exception {
        List<UUID> toKill = List.of(UUID.randomUUID(), UUID.randomUUID());
        KillJobsNodeRequest.KillJobsRequest r = new KillJobsNodeRequest(
            List.of(), toKill, "dummy-user", "just because")
            .innerRequest();

        BytesStreamOutput out = new BytesStreamOutput();
        r.writeTo(out);

        StreamInput in = out.bytes().streamInput();
        KillJobsNodeRequest.KillJobsRequest r2 = new KillJobsNodeRequest.KillJobsRequest(in);

        assertThat(r.toKill()).isEqualTo(r2.toKill());
        assertThat(r.reason()).isEqualTo(r2.reason());
        assertThat(r.userName()).isEqualTo(r2.userName());
    }
}
