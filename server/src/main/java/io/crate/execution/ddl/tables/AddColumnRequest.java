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

package io.crate.execution.ddl.tables;

import io.crate.analyze.AnalyzedColumnDefinition;
import io.crate.metadata.IndexType;
import io.crate.metadata.RelationName;
import io.crate.sql.tree.CheckColumnConstraint;
import io.crate.sql.tree.ColumnPolicy;
import io.crate.types.ArrayType;
import io.crate.types.DataType;
import io.crate.types.DataTypes;
import org.elasticsearch.action.support.master.AcknowledgedRequest;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

public class AddColumnRequest extends AcknowledgedRequest<AddColumnRequest> {

    private final RelationName relationName;
    private final boolean isPartitioned;

    // Used only with ADD COLUMN.
    // Not included into StreamableColumnInfo to avoid adding empty list in most cases.
    // Checks are merged into mapping without knowing column name as it's contained in expression itself
    // and thus we can keep them separately which is not the case for other constraints.
    private final List<StreamableCheckConstraint> checkConstraints = new ArrayList<>();

    private final List<StreamableColumnInfo> columns = new ArrayList<>();

    public AddColumnRequest(StreamInput in) throws IOException {
        super(in);
        relationName = new RelationName(in);
        isPartitioned = in.readBoolean();

        int count = in.readVInt();
        for (int i = 0; i < count; i++) {
            checkConstraints.add(StreamableCheckConstraint.readFrom(in));
        }

        count = in.readVInt();
        for (int i = 0; i < count; i++) {
            columns.add(StreamableColumnInfo.readFrom(in, ""));
        }
    }

    /**
     * For ADD COLUMN.
     */
    public AddColumnRequest(RelationName relationName,
                            boolean isPartitioned,
                            AnalyzedColumnDefinition colToAdd,
                            Map<String, String> checkConstraints) {
        this.relationName = relationName;
        this.isPartitioned = isPartitioned;
        this.columns.add(new StreamableColumnInfo(colToAdd));
        this.checkConstraints.addAll(checkConstraints.entrySet()
            .stream()
            .map(nameExprEntry -> new StreamableCheckConstraint(nameExprEntry.getKey(), nameExprEntry.getValue()))
            .collect(Collectors.toList()));
    }

    public RelationName relationName() {
        return this.relationName;
    }

    public boolean isPartitioned() {
        return isPartitioned;
    }

    public List<StreamableCheckConstraint> checkConstraints() {
        return this.checkConstraints;
    }

    public List<StreamableColumnInfo> columns() {
        return this.columns;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        relationName.writeTo(out);
        out.writeBoolean(isPartitioned);

        out.writeVInt(checkConstraints.size());
        for (int i = 0; i < checkConstraints.size(); i++) {
            checkConstraints.get(i).writeTo(out);
        }

        out.writeVInt(columns.size());
        for (int i = 0; i < columns.size(); i++) {
            columns.get(i).writeTo(out);
        }
    }

    /**
     * @param name is a non-fqn name of a column.
     * @param fqn is not streamed but constructed in readFrom() based on name and hierarchy.
     * We create StreamableColumnInfo from the root, name == fqn on the first call and then update prefix on traversing children.
     * FQN is available when created from AnalyzedColumnDefinition.
     */
    public record StreamableColumnInfo(@Nonnull String name,
                                       @Nonnull String fqn,
                                       @Nonnull DataType type,
                                       int position,
                                       boolean isPrimaryKey,
                                       boolean isNullable,
                                       boolean hasDocValues,
                                       IndexType indexType,
                                       boolean isArrayType,
                                       @Nullable String analyzer,
                                       @Nullable String genExpression,
                                       @Nullable ColumnPolicy columnPolicy,
                                       @Nullable String geoTree,
                                       @Nonnull Map<String, Object> geoProperties,
                                       @Nonnull List<String> copyToTargets,
                                       @Nonnull List<StreamableColumnInfo> children) implements Writeable {

        public StreamableColumnInfo(AnalyzedColumnDefinition<Object> colToAdd) {
            this(
                colToAdd.name(),
                colToAdd.ident().fqn(),
                colToAdd.dataType(),
                colToAdd.position,
                colToAdd.hasPrimaryKeyConstraint(),
                colToAdd.hasNotNullConstraint(),
                !AnalyzedColumnDefinition.docValuesSpecifiedAndDisabled(colToAdd),
                colToAdd.indexConstraint(),
                ArrayType.NAME.equals(colToAdd.collectionType()),
                colToAdd.analyzer(),
                colToAdd.formattedGeneratedExpression(),
                colToAdd.objectType(),
                colToAdd.geoTree(),
                colToAdd.geoProperties() == null ? new HashMap<>() : colToAdd.geoProperties().properties(),
                colToAdd.copyToTargets() == null ? new ArrayList<>() : colToAdd.copyToTargets(),
                colToAdd.children().stream().map(child -> new StreamableColumnInfo(child)).collect(Collectors.toList())
            );
        }

        /**
         * Columns can have nested objects. We read/write column structure according to writeTo traversal order.
         * StreamInput looks like: (single column info) --> (subtrees_amount) --> (repeat structure for subtrees).
         * It's guaranteed that each readFrom() call has a single column values - on the first call we are adding some column
         * and then we repeat only if subtree(s) exist.
         */
        public static StreamableColumnInfo readFrom(StreamInput in, String fqnPrefix) throws IOException {
            String policy;
            String indexType;
            var name = in.readString();
            var updatedPrefix = fqnPrefix.isEmpty() ? name : fqnPrefix + "." + name;
            return new StreamableColumnInfo(
                name,
                updatedPrefix,
                DataTypes.fromStream(in),
                in.readInt(),
                in.readBoolean(),
                in.readBoolean(),
                in.readBoolean(),
                (indexType = in.readOptionalString()) == null ? null : IndexType.valueOf(indexType),
                in.readBoolean(),
                in.readOptionalString(),
                in.readOptionalString(),
                (policy = in.readOptionalString()) == null ? null : ColumnPolicy.valueOf(policy),
                in.readOptionalString(),
                in.readMap(StreamInput::readString, StreamInput::readGenericValue),
                in.readList(StreamInput::readString),
                readChildren(in, updatedPrefix)
            );
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            out.writeString(name);
            DataTypes.toStream(type, out);
            out.writeInt(position);
            out.writeBoolean(isPrimaryKey);
            out.writeBoolean(isNullable);
            out.writeBoolean(hasDocValues);
            out.writeOptionalString(indexType != null ? indexType.name() : null);
            out.writeBoolean(isArrayType);
            out.writeOptionalString(analyzer);
            out.writeOptionalString(genExpression);
            out.writeOptionalString(columnPolicy != null ? columnPolicy.name() : null);
            out.writeOptionalString(geoTree);
            out.writeMap(geoProperties, (o, k) -> out.writeString(k), (o, v) -> out.writeGenericValue(v));
            out.writeStringCollection(copyToTargets);
            writeChildren(out);
        }

        private static List<StreamableColumnInfo> readChildren(StreamInput in, String fqnPrefix) throws IOException {
            ArrayList<StreamableColumnInfo> children = new ArrayList();
            int count = in.readVInt();
            for (int i = 0; i < count; i++) {
                children.add(readFrom(in, fqnPrefix));
            }
            return children;
        }

        private void writeChildren(StreamOutput out) throws IOException {
            out.writeVInt(children.size());
            for (int i = 0; i < children.size(); i++) {
                children.get(i).writeTo(out);
            }
        }
    }

    /**
     * Streamable version of the {@link CheckColumnConstraint}.
     * We don't need to stream columnName as it's already contained in StreamableColumnInfo.
     * We don't stream parsed expression, only raw string.
     */
    public record StreamableCheckConstraint(String name, String expression) implements Writeable {

        public static StreamableCheckConstraint readFrom(StreamInput in) throws IOException {
            var name = in.readString();
            var expr = in.readString();
            return new StreamableCheckConstraint(name, expr);
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            out.writeString(name);
            out.writeString(expression);
        }
    }
}
