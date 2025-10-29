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

import java.io.IOException;

import org.elasticsearch.Version;
import org.elasticsearch.cluster.CompleteDiff;
import org.elasticsearch.cluster.Diff;
import org.elasticsearch.cluster.Diffable;
import org.elasticsearch.cluster.Diffs;
import org.elasticsearch.common.collect.ImmutableOpenMap;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.jetbrains.annotations.Nullable;

import io.crate.metadata.RelationName;

public class SchemaMetadata implements Diffable<SchemaMetadata> {

    private final ImmutableOpenMap<String, RelationMetadata> relations;

    public SchemaMetadata(ImmutableOpenMap<String, RelationMetadata> relations) {
        this.relations = relations;
    }

    public static SchemaMetadata of(StreamInput in) throws IOException {
        ImmutableOpenMap<String, RelationMetadata> relations = in.readImmutableMap(
            StreamInput::readString,
            RelationMetadata::of
        );
        return new SchemaMetadata(relations);
    }

    public static Diff<SchemaMetadata> readDiffFrom(StreamInput in) throws IOException {
        return new SchemaMetadataDiff(in);
    }

    @Override
    public Diff<SchemaMetadata> diff(SchemaMetadata previousState) {
        return new SchemaMetadataDiff(previousState, this);
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeMap(relations, StreamOutput::writeString, RelationMetadata::toStream);
    }

    public ImmutableOpenMap<String, RelationMetadata> relations() {
        return relations;
    }

    @Nullable
    public RelationMetadata get(RelationName relation) {
        return relations.get(relation.name());
    }

    @Override
    public int hashCode() {
        return relations.hashCode();
    }

    @Override
    public boolean equals(Object obj) {
        return obj instanceof SchemaMetadata other
            && relations.equals(other.relations);
    }

    static class SchemaMetadataDiff implements Diff<SchemaMetadata> {

        private static final Diffs.DiffableValueReader<String, RelationMetadata> REL_DIFF_VALUE_READER =
            new Diffs.DiffableValueReader<>(RelationMetadata::of, RelationMetadata::readDiffFrom);

        private final Diff<ImmutableOpenMap<String, RelationMetadata>> relations;
        private final Diff<SchemaMetadata> legacyDiff;

        SchemaMetadataDiff(SchemaMetadata before, SchemaMetadata after) {
            this.relations = Diffs.diff(
                before.relations,
                after.relations,
                Diffs.stringKeySerializer(),
                RelationMetadata.VALUE_SERIALIZER
            );
            this.legacyDiff = Diffs.completeDiff(before, after);
        }

        SchemaMetadataDiff(StreamInput in) throws IOException {
            if (in.getVersion().onOrAfter(Version.V_6_2_0)) {
                relations = Diffs.readMapDiff(in, Diffs.stringKeySerializer(), REL_DIFF_VALUE_READER);
                legacyDiff = this;
            } else {
                if (in.readBoolean()) {
                    SchemaMetadata schemaMetadata = SchemaMetadata.of(in);
                    legacyDiff = new CompleteDiff<SchemaMetadata>(schemaMetadata);
                    relations = Diffs.diff(
                        ImmutableOpenMap.of(),
                        schemaMetadata.relations(),
                        Diffs.stringKeySerializer(),
                        RelationMetadata.VALUE_SERIALIZER
                    );
                } else {
                    legacyDiff = Diffs.empty();
                    relations = Diffs.empty();
                }
            }
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            if (out.getVersion().onOrAfter(Version.V_6_2_0)) {
                relations.writeTo(out);
            } else {
                legacyDiff.writeTo(out);
            }
        }

        @Override
        public SchemaMetadata apply(SchemaMetadata part) {
            if (legacyDiff == this) {
                ImmutableOpenMap<String, RelationMetadata> newRelations = relations.apply(part.relations);
                return new SchemaMetadata(newRelations);
            } else {
                return legacyDiff.apply(part);
            }
        }
    }
}
