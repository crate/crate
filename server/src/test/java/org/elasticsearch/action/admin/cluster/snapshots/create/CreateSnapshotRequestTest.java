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

package org.elasticsearch.action.admin.cluster.snapshots.create;

import static org.assertj.core.api.Assertions.assertThat;
import static org.elasticsearch.common.settings.Settings.readSettingsFromStream;
import static org.elasticsearch.common.settings.Settings.writeSettingsToStream;

import java.util.List;
import java.util.concurrent.TimeUnit;

import org.elasticsearch.Version;
import org.elasticsearch.action.support.IndicesOptions;
import org.elasticsearch.common.io.stream.BytesStreamOutput;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.tasks.TaskId;
import org.junit.Test;

import io.crate.common.unit.TimeValue;
import io.crate.metadata.PartitionName;
import io.crate.metadata.RelationName;

public class CreateSnapshotRequestTest {

    @Test
    public void test_streaming_bwc_can_be_read_on_5() throws Exception {
        RelationName table = new RelationName("foo", "bar");
        PartitionName partition = new PartitionName(new RelationName("my_schema", "my_table"), "my_partition");

        CreateSnapshotRequest request = new CreateSnapshotRequest("my_repo", "my_snapshot");
        request.relationNames(List.of(table));
        request.partitionNames(List.of(partition));

        try (var out = new BytesStreamOutput()) {
            out.setVersion(Version.V_5_10_0);
            request.writeTo(out);
            try (var in = out.bytes().streamInput()) {
                // parent request
                TaskId.readFromStream(in);
                in.readTimeValue();

                // CreateSnapshotRequest
                var snapshot = in.readString();
                var repository = in.readString();
                var indices = in.readStringArray();
                var indicesOptions = IndicesOptions.readIndicesOptions(in);
                var templates = in.readStringArray();
                readSettingsFromStream(in);
                in.readBoolean();
                in.readBoolean();
                in.readBoolean();

                assertThat(snapshot).isEqualTo("my_snapshot");
                assertThat(repository).isEqualTo("my_repo");
                assertThat(indices).containsExactlyInAnyOrder(table.indexNameOrAlias(), partition.asIndexName());
                assertThat(templates).containsExactly(PartitionName.templateName(table.schema(), table.name()));
                assertThat(indicesOptions).isEqualTo(IndicesOptions.STRICT_EXPAND_OPEN);
            }
        }
    }

    @Test
    public void test_streaming_bwc_can_be_read_from_5() throws Exception {
        RelationName table = new RelationName("foo", "bar");
        PartitionName partition = new PartitionName(new RelationName("my_schema", "my_table"), "my_partition");
        String[] indices = new String[] { table.indexNameOrAlias(), partition.asIndexName() };
        String[] templates = new String[] { PartitionName.templateName(table.schema(), table.name()) };

        try (var out = new BytesStreamOutput()) {
            // parent request
            TaskId.EMPTY_TASK_ID.writeTo(out);
            out.writeTimeValue(new TimeValue(30, TimeUnit.SECONDS));

            // CreateSnapshotRequest
            out.writeString("my_snapshot");
            out.writeString("my_repo");
            out.writeStringArray(indices);
            IndicesOptions.STRICT_EXPAND_OPEN.writeIndicesOptions(out);
            out.writeStringArray(templates);
            writeSettingsToStream(out, Settings.builder().build());
            out.writeBoolean(true);
            out.writeBoolean(true);
            out.writeBoolean(true);

            try (var in = out.bytes().streamInput()) {
                in.setVersion(Version.V_5_10_0);
                var request = new CreateSnapshotRequest(in);
                assertThat(request.relationNames()).containsExactly(table);
                assertThat(request.partitionNames()).containsExactly(partition);
            }
        }
    }
}
