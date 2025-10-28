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

package org.elasticsearch.cluster.metadata;

import static org.assertj.core.api.Assertions.assertThat;

import org.elasticsearch.cluster.Diff;
import org.elasticsearch.cluster.metadata.IndexMetadata.State;
import org.elasticsearch.common.UUIDs;
import org.elasticsearch.common.io.stream.BytesStreamOutput;
import org.elasticsearch.common.settings.Settings;
import org.junit.Test;

import io.crate.metadata.RelationName;

public class RelationMetadataTest {

    @Test
    public void test_diff_streaming() throws Exception {
        RelationName t1 = new RelationName("blob", "t1");
        RelationMetadata.BlobTable t1Open = new RelationMetadata.BlobTable(
            t1,
            UUIDs.randomBase64UUID(),
            Settings.EMPTY,
            State.OPEN
        );
        RelationMetadata.BlobTable t1Close = new RelationMetadata.BlobTable(
            t1,
            UUIDs.randomBase64UUID(),
            Settings.EMPTY,
            State.CLOSE
        );

        Diff<RelationMetadata> diff = t1Close.diff(t1Open);
        try (var out = new BytesStreamOutput()) {
            diff.writeTo(out);
            try (var in = out.bytes().streamInput()) {
                Diff<RelationMetadata> diffIn = RelationMetadata.readDiffFrom(in);
                RelationMetadata relIn = diffIn.apply(null);

                RelationMetadata.BlobTable blobIn = (RelationMetadata.BlobTable) relIn;
                assertThat(blobIn.state()).isEqualTo(State.CLOSE);
            }
        }
    }
}

