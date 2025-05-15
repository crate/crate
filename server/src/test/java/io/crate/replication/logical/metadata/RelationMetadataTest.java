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

package io.crate.replication.logical.metadata;

import static io.crate.testing.Asserts.assertThat;

import org.elasticsearch.Version;
import org.elasticsearch.common.io.stream.BytesStreamOutput;
import org.elasticsearch.common.io.stream.StreamInput;
import org.junit.Test;

import io.crate.metadata.RelationName;
import io.crate.test.integration.CrateDummyClusterServiceUnitTest;
import io.crate.testing.SQLExecutor;

public class RelationMetadataTest extends CrateDummyClusterServiceUnitTest {

    @Test
    public void test_streaming_current_version() throws Exception {
        RelationName relationName = new RelationName("doc", "t1");
        SQLExecutor.of(clusterService)
            .addTable("CREATE TABLE doc.t1 (id int)");

        boolean allPrimaryShardsActive = false;
        // Verify that a new version writes and reads false flag
        var relationMetadata =
            RelationMetadata.fromMetadata(relationName, clusterService.state().metadata(), _ -> allPrimaryShardsActive);

        BytesStreamOutput out = new BytesStreamOutput();
        relationMetadata.writeTo(out);

        StreamInput in = out.bytes().streamInput();
        var fromStream = new RelationMetadata(in);

        assertThat(relationMetadata).isEqualTo(fromStream);
    }

    @Test
    public void test_bwc_streaming() throws Exception {
        RelationName relationName = new RelationName("doc", "t1");
        SQLExecutor.of(clusterService)
            .addTable("CREATE TABLE doc.t1 (id int)");

        boolean allPrimaryShardsActive = false;
        var relationMetadata =
            RelationMetadata.fromMetadata(relationName, clusterService.state().metadata(), _ -> allPrimaryShardsActive);

        BytesStreamOutput out = new BytesStreamOutput();
        out.setVersion(Version.V_5_10_5);
        relationMetadata.writeTo(out);

        StreamInput in = out.bytes().streamInput();
        in.setVersion(Version.V_5_10_5);
        var fromStream = new RelationMetadata(in);

        assertThat(relationMetadata.name()).isEqualTo(fromStream.name());
        for (int i = 0; i < relationMetadata.indices().size(); i++) {
            assertThat(relationMetadata.indices().get(i).indexMetadata())
                .isEqualTo(fromStream.indices().get(i).indexMetadata());

            assertThat(relationMetadata.indices().get(i).allPrimaryShardsActive()).isFalse();
            assertThat(fromStream.indices().get(i).allPrimaryShardsActive()).isTrue();
        }
    }


}
