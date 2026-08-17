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
import static org.elasticsearch.cluster.metadata.Metadata.OID_UNASSIGNED;

import java.util.List;

import org.elasticsearch.Version;
import org.elasticsearch.common.io.stream.BytesStreamOutput;
import org.junit.Test;

import io.crate.common.unit.TimeValue;
import io.crate.metadata.RelationName;
import io.crate.test.integration.CrateDummyClusterServiceUnitTest;

public class ResizeRequestTest extends CrateDummyClusterServiceUnitTest {

    @Test
    public void test_streaming_table_oids() throws Exception {
        // streaming to nodes with supported versions
        RelationName table = new RelationName("foo", "bar");
        int tableOid = 1234;
        ResizeRequest request = new ResizeRequest(table, tableOid, List.of(), 4);

        var out = new BytesStreamOutput();
        out.setVersion(Version.V_6_5_0);
        request.writeTo(out);

        var in = out.bytes().streamInput();
        in.setVersion(Version.V_6_5_0);
        ResizeRequest streamed = new ResizeRequest(in);

        assertThat(streamed.tableOid()).isEqualTo(tableOid);

        // streaming to nodes with unsupported versions
        request = new ResizeRequest(table, tableOid, List.of(), 4);

        out = new BytesStreamOutput();
        out.setVersion(Version.V_6_4_0);
        request.writeTo(out);

        in = out.bytes().streamInput();
        in.setVersion(Version.V_6_4_0);
        streamed = new ResizeRequest(in);

        assertThat(streamed.tableOid()).isEqualTo(OID_UNASSIGNED);
    }

    @Test
    public void test_streaming() throws Exception {
        RelationName table = new RelationName("foo", "bar");
        List<ResizeRequest> requests = List.of(
            new ResizeRequest(table, OID_UNASSIGNED, List.of(), 4),
            new ResizeRequest(table, OID_UNASSIGNED, List.of("2", "3"), 10)
        );
        for (var originalReq : requests) {
            try (var out = new BytesStreamOutput()) {
                originalReq.writeTo(out);
                try (var in = out.bytes().streamInput()) {
                    var request = new ResizeRequest(in);
                    assertThat(request.table()).isEqualTo(table);
                    assertThat(request.partitionValues()).isEqualTo(originalReq.partitionValues());
                    assertThat(request.newNumShards()).isEqualTo(originalReq.newNumShards());
                    assertThat(request.masterNodeTimeout()).isEqualTo(TimeValue.timeValueSeconds(60));
                    assertThat(request.ackTimeout()).isEqualTo(TimeValue.timeValueSeconds(60));
                }
            }
        }
    }
}
