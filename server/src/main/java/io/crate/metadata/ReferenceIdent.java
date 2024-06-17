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

package io.crate.metadata;

import java.io.IOException;
import java.util.List;
import java.util.Locale;
import java.util.Objects;

import org.apache.lucene.util.Accountable;
import org.apache.lucene.util.RamUsageEstimator;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.jetbrains.annotations.Nullable;

public final class ReferenceIdent implements Accountable {

    private static final long SHALLOW_SIZE = RamUsageEstimator.shallowSizeOfInstance(ReferenceIdent.class);

    private final RelationName relationName;
    private final ColumnIdent columnIdent;

    public ReferenceIdent(StreamInput in) throws IOException {
        columnIdent = new ColumnIdent(in);
        relationName = new RelationName(in);
    }

    public ReferenceIdent(RelationName relationName, ColumnIdent columnIdent) {
        this.relationName = relationName;
        this.columnIdent = columnIdent;
    }

    public ReferenceIdent(RelationName relationName, String column) {
        this(relationName, new ColumnIdent(column));
    }

    public ReferenceIdent(RelationName relationName, String column, @Nullable List<String> path) {
        this(relationName, new ColumnIdent(column, path));
    }

    public RelationName tableIdent() {
        return relationName;
    }

    public ColumnIdent columnIdent() {
        return columnIdent;
    }

    @Override
    public boolean equals(Object o) {
        return o instanceof ReferenceIdent that
            && Objects.equals(relationName, that.relationName)
            && Objects.equals(columnIdent, that.columnIdent);
    }

    @Override
    public int hashCode() {
        int result = relationName.hashCode();
        return 31 * result + columnIdent.hashCode();
    }

    @Override
    public String toString() {
        return String.format(Locale.ENGLISH, "<RefIdent: %s->%s>", relationName, columnIdent);
    }

    public void writeTo(StreamOutput out) throws IOException {
        columnIdent.writeTo(out);
        relationName.writeTo(out);
    }

    @Override
    public long ramBytesUsed() {
        return SHALLOW_SIZE
            + relationName.ramBytesUsed()
            + columnIdent.ramBytesUsed();
    }
}
