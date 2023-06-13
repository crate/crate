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

import java.io.IOException;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import org.jetbrains.annotations.NotNull;

import org.elasticsearch.action.support.master.AcknowledgedRequest;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;

import com.carrotsearch.hppc.IntArrayList;

import io.crate.metadata.Reference;
import io.crate.metadata.RelationName;

public class AddColumnRequest extends AcknowledgedRequest<AddColumnRequest> {

    private final RelationName relationName;
    private final List<Reference> colsToAdd;
    private final IntArrayList pKeyIndices;
    private final Map<String, String> checkConstraints;

    /**
     * @param checkConstraints must be accumulated map of all columns' constraints in case of adding multiple columns.
     */
    public AddColumnRequest(@NotNull RelationName relationName,
                            @NotNull List<Reference> colsToAdd,
                            @NotNull Map<String, String> checkConstraints,
                            @NotNull IntArrayList pKeyIndices) {
        this.relationName = relationName;
        this.colsToAdd = colsToAdd;
        this.checkConstraints = checkConstraints;
        this.pKeyIndices = pKeyIndices;
        assert colsToAdd.isEmpty() == false : "Columns to add must not be empty";
    }

    public AddColumnRequest(StreamInput in) throws IOException {
        super(in);
        this.relationName = new RelationName(in);
        ReferencesAndConstraints referencesAndConstraints = ReferencesAndConstraints.read(in);
        this.colsToAdd = referencesAndConstraints.colsToAdd();
        this.checkConstraints = referencesAndConstraints.checkConstraints();
        this.pKeyIndices = referencesAndConstraints.pKeyIndices();
    }

    public record ReferencesAndConstraints(ArrayList<Reference> colsToAdd,
                                    Map<String, String> checkConstraints,
                                    IntArrayList pKeyIndices) {

        public static ReferencesAndConstraints read(StreamInput in) throws IOException {
            int numConstraints = in.readVInt();
            Map<String, String> checkConstraints = new LinkedHashMap<>();
            for (int i = 0; i < numConstraints; i++) {
                String name = in.readString();
                String expression = in.readString();
                checkConstraints.put(name, expression);
            }

            int numColumns = in.readVInt();
            ArrayList<Reference> colsToAdd = new ArrayList<>(numColumns);
            for (int i = 0; i < numColumns; i++) {
                colsToAdd.add(Reference.fromStream(in));
            }

            int numPKIndices = in.readVInt();
            IntArrayList pKeyIndices = new IntArrayList(numPKIndices);
            for (int i = 0; i < numPKIndices; i++) {
                pKeyIndices.add(in.readVInt());
            }
            return new ReferencesAndConstraints(colsToAdd, checkConstraints, pKeyIndices);
        }
    }

    public static void writeReferencesAndConstraints(StreamOutput out,
                                                     Map<String, String> checkConstraints,
                                                     List<Reference> colsToAdd,
                                                     IntArrayList pKeyIndices) throws IOException {
        out.writeVInt(checkConstraints.size());
        for (var entry : checkConstraints.entrySet()) {
            out.writeString(entry.getKey());
            out.writeString(entry.getValue());
        }

        out.writeVInt(colsToAdd.size());
        for (int i = 0; i < colsToAdd.size(); i++) {
            Reference.toStream(colsToAdd.get(i), out);
        }

        out.writeVInt(pKeyIndices.size());
        for (int i = 0; i < pKeyIndices.size(); i++) {
            out.writeVInt(pKeyIndices.get(i));
        }
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        relationName.writeTo(out);
        writeReferencesAndConstraints(out, checkConstraints, colsToAdd, pKeyIndices);
    }

    @NotNull
    public RelationName relationName() {
        return this.relationName;
    }

    @NotNull
    public Map<String, String> checkConstraints() {
        return this.checkConstraints;
    }

    @NotNull
    public List<Reference> references() {
        return this.colsToAdd;
    }

    @NotNull
    public IntArrayList pKeyIndices() {
        return this.pKeyIndices;
    }
}
