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

import com.carrotsearch.hppc.IntArrayList;
import io.crate.metadata.Reference;
import io.crate.metadata.RelationName;
import io.crate.sql.tree.CheckColumnConstraint;
import org.elasticsearch.action.support.master.AcknowledgedRequest;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;

import javax.annotation.Nonnull;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

public class AddColumnRequest extends AcknowledgedRequest<AddColumnRequest> {

    private final RelationName relationName;
    private final List<Reference> refs = new ArrayList<>();
    private final IntArrayList pKeyIndices = new IntArrayList();
    private final List<StreamableCheckConstraint> checkConstraints = new ArrayList<>();

    public AddColumnRequest(StreamInput in) throws IOException {
        super(in);
        relationName = new RelationName(in);

        int count = in.readVInt();
        for (int i = 0; i < count; i++) {
            checkConstraints.add(StreamableCheckConstraint.readFrom(in));
        }

        count = in.readVInt();
        for (int i = 0; i < count; i++) {
            refs.add(Reference.fromStream(in));
        }

        count = in.readVInt();
        for (int i = 0; i < count; i++) {
            pKeyIndices.add(in.readVInt());
        }
    }

    /**
     * @param checkConstraints must be accumulated map of all columns' constraints in case of adding multiple columns.
     *  This would be the case when we eventually support adding multiple column via ADD COLUMN statement.
     */
    public AddColumnRequest(@Nonnull RelationName relationName,
                            @Nonnull List<Reference> colsToAdd,
                            @Nonnull Map<String, String> checkConstraints,
                            @Nonnull IntArrayList pKeyIndices) {
        this.relationName = relationName;
        this.refs.addAll(colsToAdd);

        this.checkConstraints.addAll(checkConstraints.entrySet()
            .stream()
            .map(nameExprEntry -> new StreamableCheckConstraint(nameExprEntry.getKey(), nameExprEntry.getValue()))
            .collect(Collectors.toList()));
        this.pKeyIndices.addAll(pKeyIndices);
    }


    @Nonnull
    public RelationName relationName() {
        return this.relationName;
    }

    @Nonnull
    public List<StreamableCheckConstraint> checkConstraints() {
        return this.checkConstraints;
    }

    @Nonnull
    public List<Reference> references() {
        return this.refs;
    }

    @Nonnull
    public IntArrayList pKeyIndices() {
        return this.pKeyIndices;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        relationName.writeTo(out);

        out.writeVInt(checkConstraints.size());
        for (int i = 0; i < checkConstraints.size(); i++) {
            checkConstraints.get(i).writeTo(out);
        }

        out.writeVInt(refs.size());
        for (int i = 0; i < refs.size(); i++) {
            Reference.toStream(refs.get(i), out);
        }

        out.writeVInt(pKeyIndices.size());
        for (int i = 0; i < pKeyIndices.size(); i++) {
            out.writeVInt(pKeyIndices.get(i));
        }
    }

    /**
     * Streamable version of the {@link CheckColumnConstraint}.
     * We don't need to stream columnName as it's already contained in expression itself
     * and we add (check name -> expression) pair directly to the mapping.
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
