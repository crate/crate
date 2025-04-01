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

package org.elasticsearch.action.admin.cluster.state;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.List;
import java.util.concurrent.TimeUnit;

import org.elasticsearch.Version;
import org.elasticsearch.action.support.IndicesOptions;
import org.elasticsearch.common.io.stream.BytesStreamOutput;
import org.elasticsearch.tasks.TaskId;
import org.junit.Test;

import io.crate.common.unit.TimeValue;
import io.crate.metadata.PartitionName;
import io.crate.metadata.RelationName;

public class ClusterStateRequestTest {

    @Test
    public void test_streaming_bwc_can_be_read_on_5() throws Exception {
        RelationName table = new RelationName("foo", "bar");

        ClusterStateRequest request = new ClusterStateRequest();
        request.relationNames(List.of(table));

        try (var out = new BytesStreamOutput()) {
            out.setVersion(Version.V_5_10_0);
            request.writeTo(out);
            try (var in = out.bytes().streamInput()) {
                TaskId.readFromStream(in);
                in.readTimeValue();
                in.readBoolean();
                in.readBoolean();
                in.readBoolean();
                in.readBoolean();
                in.readBoolean();
                in.readBoolean();
                var indices = in.readStringArray();
                var indicesOptions = IndicesOptions.readIndicesOptions(in);
                in.readTimeValue();
                in.readOptionalLong();
                var templates = in.readStringArray();

                assertThat(indices).containsExactly(table.indexNameOrAlias());
                assertThat(templates).containsExactly(PartitionName.templateName(table.schema(), table.name()));
                assertThat(indicesOptions).isEqualTo(IndicesOptions.LENIENT_EXPAND_OPEN);
            }
        }
    }

    @Test
    public void test_streaming_bwc_can_be_read_from_5() throws Exception {
        RelationName table = new RelationName("foo", "bar");
        String[] indices = new String[] { table.indexNameOrAlias() };
        String[] templates = new String[] { PartitionName.templateName(table.schema(), table.name()) };

        try (var out = new BytesStreamOutput()) {
            TaskId.EMPTY_TASK_ID.writeTo(out);
            out.writeTimeValue(new TimeValue(30, TimeUnit.SECONDS));
            out.writeBoolean(true);

            out.writeBoolean(true);
            out.writeBoolean(true);
            out.writeBoolean(true);
            out.writeBoolean(true);
            out.writeBoolean(true);
            out.writeStringArray(indices);
            IndicesOptions.LENIENT_EXPAND_OPEN.writeIndicesOptions(out);
            out.writeTimeValue(new TimeValue(30, TimeUnit.SECONDS));
            out.writeOptionalLong(null);
            out.writeStringArray(templates);
            try (var in = out.bytes().streamInput()) {
                in.setVersion(Version.V_5_10_0);
                var request = new ClusterStateRequest(in);
                assertThat(request.relationNames()).containsExactly(table);
            }
        }
    }
}
