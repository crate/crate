/*
 * Licensed to Crate.io GmbH ("Crate") under one or more contributor
 * license agreements.  See the NOTICE file distributed with this work for
 * additional information regarding copyright ownership.  Crate licenses
 * this file to you under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.  You may
 * obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
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

package io.crate.metadata;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.List;

import org.elasticsearch.Version;
import org.elasticsearch.common.io.stream.BytesStreamOutput;
import org.junit.Test;

import io.crate.sql.tree.CascadeMode;

public class DropSchemaRequestTest {

    @Test
    public void test_streaming() throws Exception {
        var request = new DropSchemaRequest(List.of("foo", "bar"), true, CascadeMode.CASCADE);
        try (var out = new BytesStreamOutput()) {
            request.writeTo(out);
            try (var in = out.bytes().streamInput()) {
                assertThat(new DropSchemaRequest(in)).isEqualTo(request);
            }
        }

        // streaming bwc
        try (var out = new BytesStreamOutput()) {
            out.setVersion(Version.V_6_2_0);
            request.writeTo(out);
            try (var in = out.bytes().streamInput()) {
                in.setVersion(Version.V_6_2_0);
                var bwcRequest = new DropSchemaRequest(in);
                assertThat(bwcRequest.names()).containsExactly("foo", "bar");
                assertThat(bwcRequest.ifExists()).isTrue();
                // 6.2. has no support for cascade
                assertThat(bwcRequest.mode()).isEqualTo(CascadeMode.RESTRICT);
            }
        }
    }
}
