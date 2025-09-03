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
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import org.elasticsearch.cluster.metadata.IndexMetadata.State;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.common.settings.Settings;
import org.jetbrains.annotations.Nullable;

import io.crate.metadata.ColumnIdent;
import io.crate.metadata.Reference;
import io.crate.metadata.RelationName;
import io.crate.sql.tree.ColumnPolicy;

public sealed interface RelationMetadata extends Writeable permits
    RelationMetadata.BlobTable,
    RelationMetadata.Table {

    short ord();

    RelationName name();

    static RelationMetadata of(StreamInput in) throws IOException {
        short ord = in.readShort();
        return switch (ord) {
            case BlobTable.ORD -> RelationMetadata.BlobTable.of(in);
            case Table.ORD -> RelationMetadata.Table.of(in);
            default -> throw new IllegalArgumentException("Invalid RelationMetadata ord: " + ord);
        };
    }

    List<String> indexUUIDs();

    static void toStream(StreamOutput out, RelationMetadata v) throws IOException {
        out.writeShort(v.ord());
        v.writeTo(out);
    }

    RelationMetadata withIndexUUIDs(List<String> indexUUIDs);

    record BlobTable(RelationName name,
                     String indexUUID,
                     Settings settings,
                     IndexMetadata.State state) implements RelationMetadata {

        private static final short ORD = 0;

        static BlobTable of(StreamInput in) throws IOException {
            RelationName name = new RelationName(in);
            String indexUUID = in.readString();
            Settings settings = Settings.readSettingsFromStream(in);
            IndexMetadata.State state = in.readEnum(IndexMetadata.State.class);
            return new BlobTable(name, indexUUID, settings, state);
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            name.writeTo(out);
            out.writeString(indexUUID);
            Settings.writeSettingsToStream(out, settings);
            out.writeEnum(state);
        }

        @Override
        public short ord() {
            return ORD;
        }

        @Override
        public List<String> indexUUIDs() {
            return List.of(indexUUID);
        }

        @Override
        public RelationMetadata withIndexUUIDs(List<String> indexUUIDs) {
            assert indexUUIDs.size() == 1 : "Must have exactly one index linked with blob table";
            return new BlobTable(
                name,
                indexUUIDs.getFirst(),
                settings,
                state
            );
        }
    }

    record Table(RelationName name,
                 List<Reference> columns,
                 Settings settings,
                 @Nullable ColumnIdent routingColumn,
                 ColumnPolicy columnPolicy,
                 @Nullable String pkConstraintName,
                 Map<String, String> checkConstraints,
                 List<ColumnIdent> primaryKeys,
                 List<ColumnIdent> partitionedBy,
                 IndexMetadata.State state,
                 List<String> indexUUIDs,
                 long tableVersion) implements RelationMetadata {

        public Table {
            assert (partitionedBy.isEmpty() && indexUUIDs.size() == 1) || !partitionedBy.isEmpty()
                : "Non-Partitioned table " + name + " must have exactly one indexUUID: " + indexUUIDs;
        }


        private static final short ORD = 1;

        @Override
        public short ord() {
            return ORD;
        }

        public static Table of(StreamInput in) throws IOException {
            RelationName name = new RelationName(in);
            List<Reference> columns = in.readList(Reference::fromStream);
            Settings settings = Settings.readSettingsFromStream(in);
            ColumnIdent routingColumn = in.readOptionalWriteable(ColumnIdent::of);
            ColumnPolicy columnPolicy = ColumnPolicy.VALUES.get(in.readVInt());
            String pkConstraintName = in.readOptionalString();
            Map<String, String> checkConstraints = in.readMap(
                LinkedHashMap::new, StreamInput::readString, StreamInput::readString);
            List<ColumnIdent> primaryKeys = in.readList(ColumnIdent::of);
            List<ColumnIdent> partitionedBy = in.readList(ColumnIdent::of);
            State state = in.readEnum(State.class);
            List<String> indexUUIDs = in.readStringList();
            long tableVersion = in.readLong();
            return new Table(
                name,
                columns,
                settings,
                routingColumn,
                columnPolicy,
                pkConstraintName,
                checkConstraints,
                primaryKeys,
                partitionedBy,
                state,
                indexUUIDs,
                tableVersion
            );
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            name.writeTo(out);
            out.writeCollection(columns, Reference::toStream);
            Settings.writeSettingsToStream(out, settings);
            out.writeOptionalWriteable(routingColumn);
            out.writeVInt(columnPolicy.ordinal());
            out.writeOptionalString(pkConstraintName);
            out.writeMap(checkConstraints, StreamOutput::writeString, StreamOutput::writeString);
            out.writeList(primaryKeys);
            out.writeList(partitionedBy);
            out.writeEnum(state);
            out.writeStringCollection(indexUUIDs);
            out.writeLong(tableVersion);
        }

        @Override
        public RelationMetadata withIndexUUIDs(List<String> indexUUIDs) {
            return new Table(
                name,
                columns,
                settings,
                routingColumn,
                columnPolicy,
                pkConstraintName,
                checkConstraints,
                primaryKeys,
                partitionedBy,
                state,
                indexUUIDs,
                tableVersion
            );
        }
    }
}
