/*
 * Licensed to Crate.io GmbH ("Crate") under one or more contributor
 * license agreements.  See the NOTICE file distributed with this work for
 * additional information regarding copyright ownership.  Crate licenses
 * this file to you under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.  You may
 * obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
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

package io.crate.replication.logical.metadata;

import io.crate.metadata.RelationName;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

public class Publication implements Writeable {

    private final String owner;
    private final boolean forAllTables;
    private final List<RelationName> tables;

    public Publication(String owner, boolean forAllTables, List<RelationName> tables) {
        this.owner = owner;
        this.forAllTables = forAllTables;
        this.tables = tables;
    }

    Publication(StreamInput in) throws IOException {
        owner = in.readString();
        forAllTables = in.readBoolean();
        int size = in.readVInt();
        tables = new ArrayList<>(size);
        for (int i = 0; i < size; i++) {
            tables.add(RelationName.fromIndexName(in.readString()));
        }
    }

    public String owner() {
        return owner;
    }

    public boolean isForAllTables() {
        return forAllTables;
    }

    public List<RelationName> tables() {
        return tables;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeString(owner);
        out.writeBoolean(forAllTables);
        out.writeVInt(tables.size());
        for (var table : tables) {
            out.writeString(table.indexNameOrAlias());
        }
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        Publication that = (Publication) o;
        return forAllTables == that.forAllTables && owner.equals(that.owner) && tables.equals(that.tables);
    }

    @Override
    public int hashCode() {
        return Objects.hash(owner, tables, forAllTables);
    }
}
