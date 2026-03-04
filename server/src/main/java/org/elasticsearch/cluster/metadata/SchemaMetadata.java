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
import java.util.List;
import java.util.Map;

import org.elasticsearch.Version;
import org.elasticsearch.cluster.Diff;
import org.elasticsearch.cluster.Diffable;
import org.elasticsearch.cluster.Diffs;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.jspecify.annotations.Nullable;

import io.crate.expression.udf.UserDefinedFunctionMetadata;
import io.crate.metadata.RelationName;

public class SchemaMetadata implements Diffable<SchemaMetadata> {

    private final Map<String, RelationMetadata> relations;
    private final List<UserDefinedFunctionMetadata> udfs;
    private final boolean explicit;

    /// @param explicit true indicates this was created via CREATE SCHEMA
    public SchemaMetadata(Map<String, RelationMetadata> relations,
                          List<UserDefinedFunctionMetadata> udfs,
                          boolean explicit) {
        this.relations = relations;
        this.udfs = udfs;
        this.explicit = explicit;
    }

    public static SchemaMetadata of(StreamInput in) throws IOException {
        Map<String, RelationMetadata> relations = in.readMap(
            StreamInput::readString,
            RelationMetadata::of
        );
        boolean explicit = in.getVersion().onOrAfter(Version.V_6_2_0)
            ? in.readBoolean()
            : false;
        List<UserDefinedFunctionMetadata> udfs = in.getVersion().onOrAfter(Version.V_6_3_0)
            ? in.readList(UserDefinedFunctionMetadata::new)
            : List.of();
        return new SchemaMetadata(relations, udfs, explicit);
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeMap(relations, StreamOutput::writeString, RelationMetadata::toStream);
        Version version = out.getVersion();
        if (version.onOrAfter(Version.V_6_2_0)) {
            out.writeBoolean(explicit);
        }
        if (version.onOrAfter(Version.V_6_3_0)) {
            out.writeList(udfs);
        }
    }

    public static Diff<SchemaMetadata> readDiffFrom(StreamInput in) throws IOException {
        if (in.getVersion().onOrAfter(Version.V_6_2_0)) {
            return new SchemaMetadataDiff(in);
        } else {
            return Diffs.readDiffFrom(SchemaMetadata::of, in);
        }
    }

    @Override
    public Diff<SchemaMetadata> diff(Version version, SchemaMetadata previousState) {
        if (version.onOrAfter(Version.V_6_2_0)) {
            return new SchemaMetadataDiff(version, previousState, this);
        } else {
            return Diffs.completeDiff(previousState, this);
        }
    }

    public Map<String, RelationMetadata> relations() {
        return relations;
    }

    public List<UserDefinedFunctionMetadata> udfs() {
        return udfs;
    }

    @Nullable
    public RelationMetadata get(RelationName relation) {
        return relations.get(relation.name());
    }

    @Nullable
    public RelationMetadata get(String name) {
        return relations.get(name);
    }

    /// True if schema was created explicitly via CREATE SCHEMA
    /// Explicit schemas are not removed if the last table within a relation is dropped.
    public boolean explicit() {
        return explicit;
    }


    @Override
    public int hashCode() {
        final int prime = 31;
        int result = 1;
        result = prime * result + relations.hashCode();
        result = prime * result + udfs.hashCode();
        result = prime * result + (explicit ? 1231 : 1237);
        return result;
    }

    @Override
    public boolean equals(Object obj) {
        return obj instanceof SchemaMetadata other
            && relations.equals(other.relations)
            && udfs.equals(other.udfs)
            && explicit == other.explicit;
    }

    static class SchemaMetadataDiff implements Diff<SchemaMetadata> {

        private static final Diffs.DiffableValueReader<String, RelationMetadata> REL_DIFF_VALUE_READER =
            new Diffs.DiffableValueReader<>(RelationMetadata::of, RelationMetadata::readDiffFrom);

        private final Diff<Map<String, RelationMetadata>> relations;
        private final List<UserDefinedFunctionMetadata> udfs;
        private final boolean explicit;

        SchemaMetadataDiff(Version version, SchemaMetadata before, SchemaMetadata after) {
            this.relations = Diffs.diff(
                version,
                before.relations,
                after.relations,
                Diffs.stringKeySerializer(),
                RelationMetadata.VALUE_SERIALIZER
            );
            this.udfs = after.udfs();
            this.explicit = after.explicit;
        }

        SchemaMetadataDiff(StreamInput in) throws IOException {
            relations = Diffs.readMapDiff(in, Diffs.stringKeySerializer(), REL_DIFF_VALUE_READER);
            explicit = in.readBoolean();
            udfs = in.getVersion().onOrAfter(Version.V_6_3_0)
                ? in.readList(UserDefinedFunctionMetadata::new)
                : List.of();
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            relations.writeTo(out);
            out.writeBoolean(explicit);
            if (out.getVersion().onOrAfter(Version.V_6_3_0)) {
                out.writeList(udfs);
            }
        }

        @Override
        public SchemaMetadata apply(SchemaMetadata part) {
            Map<String, RelationMetadata> newRelations = relations.apply(part.relations);
            return new SchemaMetadata(newRelations, udfs, explicit);
        }
    }
}
