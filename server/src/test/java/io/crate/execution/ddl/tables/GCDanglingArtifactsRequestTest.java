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

package io.crate.execution.ddl.tables;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.List;

import org.elasticsearch.Version;
import org.elasticsearch.common.io.stream.BytesStreamOutput;
import org.junit.Test;

public class GCDanglingArtifactsRequestTest {

    @Test
    public void test_bwc_streaming() throws Exception {
        String indexUUID = "uuid1";
        var req = new GCDanglingArtifactsRequest(List.of(indexUUID));
        List<Version> noUUIDVersions = List.of(Version.V_6_0_2);
        for (var version : noUUIDVersions) {
            try (var out = new BytesStreamOutput()) {
                out.setVersion(version);
                req.writeTo(out);

                try (var in = out.bytes().streamInput()) {
                    in.setVersion(version);
                    var reqIn = new GCDanglingArtifactsRequest(in);
                    assertThat(reqIn.indexUUIDs()).isEmpty();
                }
            }
        }

        List<Version> uuidVersions = List.of(Version.V_6_1_1, Version.CURRENT);
        for (var version : uuidVersions) {
            try (var out = new BytesStreamOutput()) {
                out.setVersion(version);
                req.writeTo(out);

                try (var in = out.bytes().streamInput()) {
                    in.setVersion(version);
                    var reqIn = new GCDanglingArtifactsRequest(in);
                    assertThat(reqIn.indexUUIDs()).containsExactly(indexUUID);
                }
            }
        }
    }
}

