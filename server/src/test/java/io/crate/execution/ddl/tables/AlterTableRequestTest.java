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

import java.util.Arrays;
import java.util.List;

import org.elasticsearch.Version;
import org.elasticsearch.common.io.stream.BytesStreamOutput;
import org.elasticsearch.common.settings.Settings;
import org.junit.Test;

import io.crate.metadata.RelationName;

public class AlterTableRequestTest {

    @Test
    public void test_streaming_bwc() throws Exception {
        RelationName tbl = new RelationName("doc", "tbl");
        List<AlterTableRequest> requests = List.of(
            new AlterTableRequest(tbl, List.of(), true, false, Settings.EMPTY),
            new AlterTableRequest(tbl, Arrays.asList("1", null, "3"), true, false, Settings.EMPTY)
        );
        for (var request : requests) {
            try (var out = new BytesStreamOutput()) {
                request.writeTo(out);
                try (var in = out.bytes().streamInput()) {
                    AlterTableRequest inRequest = new AlterTableRequest(in);
                    assertThat(inRequest).isEqualTo(request);
                }
            }
            try (var out = new BytesStreamOutput()) {
                out.setVersion(Version.V_5_9_4);
                request.writeTo(out);
                try (var in = out.bytes().streamInput()) {
                    in.setVersion(Version.V_5_9_4);
                    AlterTableRequest inRequest = new AlterTableRequest(in);
                    assertThat(inRequest).isEqualTo(request);
                }
            }
        }
    }
}

