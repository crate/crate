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

import java.util.ArrayList;

import org.elasticsearch.Version;
import org.elasticsearch.cluster.Diff;
import org.elasticsearch.cluster.metadata.IndexMetadata.State;
import org.elasticsearch.common.UUIDs;
import org.elasticsearch.common.collect.ImmutableOpenMap;
import org.elasticsearch.common.io.stream.BytesStreamOutput;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.test.ESTestCase;
import org.junit.Test;

import io.crate.metadata.RelationName;

public class SchemaMetadataTest extends ESTestCase {

    RelationName t1 = new RelationName("blob", "t1");
    RelationMetadata.BlobTable relT1 = new RelationMetadata.BlobTable(
        t1,
        UUIDs.randomBase64UUID(),
        Settings.EMPTY,
        State.OPEN
    );
    RelationName t2 = new RelationName("blob", "t2");
    RelationMetadata.BlobTable relT2 = new RelationMetadata.BlobTable(
        t2,
        UUIDs.randomBase64UUID(),
        Settings.EMPTY,
        State.OPEN
    );
    RelationName t3 = new RelationName("blob", "t3");
    RelationMetadata.BlobTable relT3 = new RelationMetadata.BlobTable(
        t3,
        UUIDs.randomBase64UUID(),
        Settings.EMPTY,
        State.OPEN
    );

    private void assertRelations(int expected, SchemaMetadata schemaMetadata) {
        ArrayList<RelationMetadata> actualRelations = new ArrayList<>();
        schemaMetadata.relations().valuesIt().forEachRemaining(actualRelations::add);
        if (expected == 3) {
            assertThat(actualRelations).containsExactlyInAnyOrder(relT1, relT2, relT3);
        } else {
            assertThat(actualRelations).containsExactlyInAnyOrder(relT1, relT2);
        }
    }

    @Test
    public void test_diff_streaming() throws Exception {
        ImmutableOpenMap.Builder<String, RelationMetadata> relationsV1 = ImmutableOpenMap.builder();
        relationsV1.put("t1", relT1);
        relationsV1.put("t2", relT2);
        SchemaMetadata schemaV1 = new SchemaMetadata(relationsV1.build());

        ImmutableOpenMap.Builder<String, RelationMetadata> relationsV2 = ImmutableOpenMap.builder();
        relationsV2.put("t1", relT1);
        relationsV2.put("t2", relT2);
        relationsV2.put("t3", relT3);
        SchemaMetadata schemaV2 = new SchemaMetadata(relationsV2.build());

        SchemaMetadata before;
        SchemaMetadata after;
        int expectedRelations;
        if (randomBoolean()) {
            before = schemaV1;
            after = schemaV2;
            expectedRelations = 3;
        } else {
            before = schemaV2;
            after = schemaV1;
            expectedRelations = 2;
        }

        Diff<SchemaMetadata> diff = after.diff(Version.CURRENT, before);
        try (var out = new BytesStreamOutput()) {
            diff.writeTo(out);
            try (var in = out.bytes().streamInput()) {
                Diff<SchemaMetadata> diffFrom = SchemaMetadata.readDiffFrom(in);
                SchemaMetadata schemaIn = diffFrom.apply(after);
                assertRelations(expectedRelations, schemaIn);
            }
        }

        try (var out = new BytesStreamOutput()) {
            out.setVersion(Version.V_6_1_0);
            diff.writeTo(out);

            try (var in = out.bytes().streamInput()) {
                in.setVersion(Version.V_6_1_0);
                Diff<SchemaMetadata> diffFrom = SchemaMetadata.readDiffFrom(in);
                SchemaMetadata schemaIn = diffFrom.apply(after);
                assertRelations(expectedRelations, schemaIn);

                // ensure that deserialized diff can also be written back to 6.2.0 or 6.1.0
                try (var out2 = new BytesStreamOutput()) {
                    diffFrom.writeTo(out2);
                    try (var in2 = out2.bytes().streamInput()) {
                        Diff<SchemaMetadata> diffFrom2 = SchemaMetadata.readDiffFrom(in2);
                        SchemaMetadata schemaIn2 = diffFrom2.apply(schemaV1);
                        assertRelations(expectedRelations, schemaIn2);
                    }
                }

                try (var out2 = new BytesStreamOutput()) {
                    out2.setVersion(Version.V_6_1_0);
                    diffFrom.writeTo(out2);
                    try (var in2 = out2.bytes().streamInput()) {
                        in2.setVersion(Version.V_6_1_0);
                        Diff<SchemaMetadata> diffFrom2 = SchemaMetadata.readDiffFrom(in2);
                        SchemaMetadata schemaIn2 = diffFrom2.apply(after);
                        assertRelations(expectedRelations, schemaIn2);
                    }
                }
            }
        }
    }
}

