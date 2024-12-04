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

import io.crate.metadata.RelationName;

public class CloseTableRequestTest {

    @Test
    public void test_bwc_streaming() throws Exception {
        RelationName relation = new RelationName("foo", "bar");
        List<CloseTableRequest> requests = List.of(
            new CloseTableRequest(relation, List.of("1")),
            new CloseTableRequest(relation, List.of())
        );

        for (var request : requests) {
            {
                BytesStreamOutput out = new BytesStreamOutput();
                request.writeTo(out);

                try (var in = out.bytes().streamInput()) {
                    CloseTableRequest deserializedRequest = new CloseTableRequest(in);
                    assertThat(deserializedRequest.table()).isEqualTo(relation);
                    assertThat(deserializedRequest.partitionValues()).isEqualTo(request.partitionValues());
                }
            }
            {
                BytesStreamOutput out = new BytesStreamOutput();
                out.setVersion(Version.V_5_9_0);
                request.writeTo(out);

                try (var in = out.bytes().streamInput()) {
                    in.setVersion(Version.V_5_9_0);
                    CloseTableRequest deserializedRequest = new CloseTableRequest(in);
                    assertThat(deserializedRequest.table()).isEqualTo(relation);
                    assertThat(deserializedRequest.partitionValues()).isEqualTo(request.partitionValues());
                }
            }
        }
    }
}

