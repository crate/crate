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

package org.elasticsearch.action.admin.cluster.snapshots.restore;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.List;
import java.util.Set;

import org.elasticsearch.Version;
import org.elasticsearch.action.support.IndicesOptions;
import org.elasticsearch.common.io.stream.BytesStreamOutput;
import org.elasticsearch.common.settings.Settings;
import org.junit.Test;

import io.crate.metadata.RelationName;

public class RestoreSnapshotRequestTest {

    @Test
    public void test_serialization_bwc() throws Exception {
        var req = new RestoreSnapshotRequest(
            "repoName",
            "snapshotName",
            List.of(
                new RestoreSnapshotRequest.TableOrPartition(new RelationName("s", "t1"), null)
            ),
            IndicesOptions.LENIENT_EXPAND_OPEN_CLOSED,
            Settings.EMPTY,
            true,
            true,
            Set.of("x"),
            true,
            List.of("a", "b")
        );

        try (var out = new BytesStreamOutput()) {
            req.writeTo(out);
            try (var in = out.bytes().streamInput()) {
                assertThat(new RestoreSnapshotRequest(in)).isEqualTo(req);
            }
        }

        try (var out = new BytesStreamOutput()) {
            out.setVersion(Version.V_5_10_1);
            req.writeTo(out);
            try (var in = out.bytes().streamInput()) {
                in.setVersion(Version.V_5_10_1);
                assertThat(new RestoreSnapshotRequest(in)).isEqualTo(req);
            }
        }
    }
}

