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

    public static RelationMetadata of(StreamInput in) throws IOException {
        short ord = in.readShort();
        return switch (ord) {
            case BlobTable.ORD -> RelationMetadata.BlobTable.of(in);
            case Table.ORD -> RelationMetadata.Table.of(in);
            default -> throw new IllegalArgumentException("Invalid RelationMetadata ord: " + ord);
        };
    }

    public static void toStream(StreamOutput out, RelationMetadata v) throws IOException {
        out.writeShort(v.ord());
        v.writeTo(out);
    }

    public static record BlobTable(RelationName name, String indexUUID) implements RelationMetadata {

        private static final short ORD = 0;

        static BlobTable of(StreamInput in) throws IOException {
            RelationName name = new RelationName(in);
            String indexUUID = in.readString();
            return new BlobTable(name, indexUUID);
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            name.writeTo(out);
            out.writeString(indexUUID);
        }

        @Override
        public short ord() {
            return ORD;
        }
    }

    public static final record Table(
            RelationName name,
            List<Reference> columns,
            Settings settings,
            @Nullable ColumnIdent routingColumn,
            ColumnPolicy columnPolicy,
            @Nullable String pkConstraintName,
            Map<String, String> checkConstraints,
            List<ColumnIdent> primaryKeys,
            List<ColumnIdent> partitionedBy,
            IndexMetadata.State state,
            List<String> indexUUIDs) implements RelationMetadata {

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
                indexUUIDs
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
        }
    }
}
