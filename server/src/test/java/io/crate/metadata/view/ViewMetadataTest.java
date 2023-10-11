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

package io.crate.metadata.view;

import static org.assertj.core.api.Assertions.assertThat;

import org.elasticsearch.Version;
import org.elasticsearch.common.io.stream.BytesStreamOutput;
import org.elasticsearch.common.io.stream.StreamInput;
import org.junit.Test;

import io.crate.metadata.SearchPath;

public class ViewMetadataTest {

    @Test
    public void test_bwc_streaming() throws Exception {
        var viewMetadata = new ViewMetadata(
            "select 1",
            "me",
            SearchPath.createSearchPathFrom("foo", "bar")
        );
        try (BytesStreamOutput out = new BytesStreamOutput()) {
            out.setVersion(Version.V_5_4_0);
            viewMetadata.writeTo(out);
            StreamInput in = out.bytes().streamInput();
            in.setVersion(Version.V_5_4_0);
            ViewMetadata fromStream = ViewMetadata.of(in);
            assertThat(fromStream.searchPath()).isEqualTo(SearchPath.pathWithPGCatalogAndDoc());
            assertThat(fromStream.stmt()).isEqualTo(viewMetadata.stmt());
            assertThat(fromStream.owner()).isEqualTo(viewMetadata.owner());
        }
        try (BytesStreamOutput out = new BytesStreamOutput()) {
            out.setVersion(Version.V_5_3_4);
            viewMetadata.writeTo(out);
            StreamInput in = out.bytes().streamInput();
            in.setVersion(Version.V_5_3_4);
            ViewMetadata fromStream = ViewMetadata.of(in);
            assertThat(fromStream.searchPath()).isEqualTo(SearchPath.pathWithPGCatalogAndDoc());
            assertThat(fromStream.stmt()).isEqualTo(viewMetadata.stmt());
            assertThat(fromStream.owner()).isEqualTo(viewMetadata.owner());
        }
        try (BytesStreamOutput out = new BytesStreamOutput()) {
            out.setVersion(Version.V_5_4_1);
            viewMetadata.writeTo(out);
            StreamInput in = out.bytes().streamInput();
            in.setVersion(Version.V_5_4_1);
            ViewMetadata fromStream = ViewMetadata.of(in);
            assertThat(fromStream).isEqualTo(viewMetadata);
        }
        try (BytesStreamOutput out = new BytesStreamOutput()) {
            out.setVersion(Version.V_5_3_5);
            viewMetadata.writeTo(out);
            StreamInput in = out.bytes().streamInput();
            in.setVersion(Version.V_5_3_5);
            ViewMetadata fromStream = ViewMetadata.of(in);
            assertThat(fromStream).isEqualTo(viewMetadata);
        }
    }
}

