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

import io.crate.metadata.Reference;
import io.crate.metadata.RelationName;
import io.crate.sql.tree.CheckColumnConstraint;
import org.elasticsearch.action.support.master.AcknowledgedRequest;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.io.IOException;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * Contains all supported for ADD COLUMN configurations such as constraints,
 * doc values enabled flag, column type properties (bit length, text analyzer e.t.c).
 */
public class AddColumnRequest extends AcknowledgedRequest<AddColumnRequest> {

    private final RelationName relationName;
    private final boolean isPartitioned;
    private final Reference columnRef; // Contains isNullable and hasDocValues which reflects NOT-NULL and STORAGE WITH.
    private final Map<String, Object> columnProperties = new LinkedHashMap<>();
    private final boolean isPrimaryKey;

    @Nullable
    private final String generatedExpression;
    private final List<StreamableCheckConstraint> checkConstraints = new ArrayList<>();

    public AddColumnRequest(StreamInput in) throws IOException {
        super(in);
        relationName = new RelationName(in);
        isPartitioned = in.readBoolean();
        columnRef = Reference.fromStream(in);
        columnProperties.putAll(in.readMap(StreamInput::readString, StreamInput::readGenericValue));
        isPrimaryKey = in.readBoolean();
        generatedExpression = in.readOptionalString();
        int count = in.readVInt();
        for (int i = 0; i < count; i++) {
            checkConstraints.add(StreamableCheckConstraint.readFrom(in));
        }
    }

    public AddColumnRequest(RelationName relationName,
                            boolean isPartitioned,
                            Reference column,
                            @Nonnull Map<String, Object> columnTypeProperties,
                            boolean isPrimaryKey,
                            @Nullable String generatedExpression,
                            @Nonnull Map<String, String> checkConstraints) {
        this.relationName = relationName;
        this.isPartitioned = isPartitioned;
        this.columnRef = column;
        this.columnProperties.putAll(columnTypeProperties);
        this.isPrimaryKey = isPrimaryKey;
        this.generatedExpression = generatedExpression;
        this.checkConstraints.addAll(checkConstraints.entrySet()
            .stream()
            .map(nameExprEntry -> new StreamableCheckConstraint(nameExprEntry.getKey(), nameExprEntry.getValue()))
            .collect(Collectors.toList()));
    }

    @Nonnull
    public RelationName relationName() {
        return this.relationName;
    }

    @Nonnull
    public Reference columnRef() {
        return this.columnRef;
    }

    public boolean isPrimaryKey() {
        return this.isPrimaryKey;
    }

    public boolean isPartitioned() {
        return isPartitioned;
    }

    @Nullable
    public String generatedExpression() {
        return this.generatedExpression;
    }

    @Nonnull
    List<StreamableCheckConstraint> checkConstraints() {
        return this.checkConstraints;
    }

    public Map<String, Object> columnProperties() {
        return this.columnProperties;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        relationName.writeTo(out);
        out.writeBoolean(isPartitioned);
        Reference.toStream(columnRef, out);
        out.writeMap(columnProperties, (o, v) -> o.writeString(v), (o, v) -> out.writeGenericValue(v));
        out.writeBoolean(isPrimaryKey);
        out.writeOptionalString(generatedExpression);
        out.writeVInt(checkConstraints.size());
        for (int i = 0; i < checkConstraints.size(); i++) {
            checkConstraints.get(i).writeTo(out);
        }
    }

    /**
     * Streamable version of the {@link CheckColumnConstraint}.
     * We don't need to stream columnName as it's already contained in the columnRef.
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
