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
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

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
    public AddColumnRequest(RelationName relationName,
                            List<Reference> colsToAdd,
                            Map<String, String> checkConstraints,
                            IntArrayList pKeyIndices) {
        this.relationName = relationName;
        this.colsToAdd = colsToAdd;
        this.checkConstraints = checkConstraints;
        this.pKeyIndices = pKeyIndices;
        assert colsToAdd.isEmpty() == false : "Columns to add must not be empty";
    }

    public AddColumnRequest(StreamInput in) throws IOException {
        super(in);
        this.relationName = new RelationName(in);
        this.checkConstraints = in.readMap(
            LinkedHashMap::new, StreamInput::readString, StreamInput::readString);
        this.colsToAdd = in.readList(Reference::fromStream);
        int numPKIndices = in.readVInt();
        this.pKeyIndices = new IntArrayList(numPKIndices);
        for (int i = 0; i < numPKIndices; i++) {
            pKeyIndices.add(in.readVInt());
        }
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        relationName.writeTo(out);
        out.writeMap(checkConstraints, StreamOutput::writeString, StreamOutput::writeString);
        out.writeCollection(colsToAdd, Reference::toStream);
        out.writeVInt(pKeyIndices.size());
        for (int i = 0; i < pKeyIndices.size(); i++) {
            out.writeVInt(pKeyIndices.get(i));
        }
    }

    public RelationName relationName() {
        return this.relationName;
    }

    public Map<String, String> checkConstraints() {
        return this.checkConstraints;
    }

    public List<Reference> references() {
        return this.colsToAdd;
    }

    public IntArrayList pKeyIndices() {
        return this.pKeyIndices;
    }
}
