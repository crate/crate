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

package org.elasticsearch.action.admin.indices.shrink;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.List;

import org.elasticsearch.Version;
import org.elasticsearch.action.admin.indices.create.CreateIndexRequest;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.common.io.stream.BytesStreamOutput;
import org.elasticsearch.common.settings.Settings;
import org.junit.Test;

import io.crate.execution.ddl.tables.AlterTableClient;
import io.crate.metadata.PartitionName;
import io.crate.metadata.RelationName;

public class ResizeRequestTest {

    @Test
    public void test_streaming() throws Exception {
        RelationName table = new RelationName("foo", "bar");
        List<ResizeRequest> requests = List.of(
            new ResizeRequest(table, List.of(), 4),
            new ResizeRequest(table, List.of("2", "3"), 10)
        );
        for (var originalReq : requests) {
            try (var out = new BytesStreamOutput()) {
                originalReq.writeTo(out);
                try (var in = out.bytes().streamInput()) {
                    var request = new ResizeRequest(in);
                    assertThat(request.table()).isEqualTo(table);
                    assertThat(request.partitionValues()).isEqualTo(originalReq.partitionValues());
                    assertThat(request.newNumShards()).isEqualTo(originalReq.newNumShards());
                }
            }

            RelationName tbl = originalReq.table();
            List<String> values = originalReq.partitionValues();
            String indexName = values.isEmpty()
                ? tbl.indexNameOrAlias()
                : new PartitionName(tbl, values).asIndexName();
            try (var out = new BytesStreamOutput()) {
                out.setVersion(Version.V_5_9_4);

                Settings settings = Settings.builder()
                    .put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, originalReq.newNumShards())
                    .build();
                CreateIndexRequest indexRequest = new CreateIndexRequest(
                    AlterTableClient.RESIZE_PREFIX + indexName,
                    settings
                );

                // Can't use originalReq.writeTo() here because it cannot write the resizeType
                // without knowing the current number of shards
                // So this is re-implementing writeTo to test the reading
                //
                // triggering resize on a 5.10 is not supported, but triggering from 5.9 has to work
                // because 5.9 doesn't contain logic to disallow it with 5.10 nodes

                // this covers super.writeTo from
                originalReq.getParentTask().writeTo(out);
                out.writeTimeValue(originalReq.masterNodeTimeout());
                out.writeTimeValue(originalReq.timeout());


                indexRequest.writeTo(out);
                out.writeString(indexName); // sourceIndex
                out.writeEnum(ResizeType.SHRINK);
                out.writeOptionalBoolean(true); // copySettings, was always true

                try (var in = out.bytes().streamInput()) {
                    in.setVersion(Version.V_5_9_4);

                    var request = new ResizeRequest(in);
                    assertThat(request.table()).isEqualTo(table);
                    assertThat(request.partitionValues()).isEqualTo(originalReq.partitionValues());
                    assertThat(request.newNumShards()).isEqualTo(originalReq.newNumShards());
                }
            }
        }
    }
}
