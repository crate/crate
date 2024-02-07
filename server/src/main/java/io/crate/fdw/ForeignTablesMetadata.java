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

package io.crate.fdw;

import java.io.IOException;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.elasticsearch.Version;
import org.elasticsearch.cluster.AbstractNamedDiffable;
import org.elasticsearch.cluster.metadata.Metadata;
import org.elasticsearch.cluster.metadata.Metadata.XContentContext;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.jetbrains.annotations.Nullable;

import io.crate.exceptions.RelationUnknown;
import io.crate.metadata.ColumnIdent;
import io.crate.metadata.Reference;
import io.crate.metadata.RelationName;

public class ForeignTablesMetadata extends AbstractNamedDiffable<Metadata.Custom>
        implements Metadata.Custom, Iterable<ForeignTable> {

    public static final String TYPE = "foreign_tables";
    public static final ForeignTablesMetadata EMPTY = new ForeignTablesMetadata(Map.of());

    private final Map<RelationName, ForeignTable> tables;


    private ForeignTablesMetadata(Map<RelationName, ForeignTable> tables) {
        this.tables = tables;
    }

    public ForeignTablesMetadata(StreamInput in) throws IOException {
        this.tables = in.readMap(RelationName::new, ForeignTable::new);
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeMap(tables, (o, v) -> v.writeTo(o), (o, v) -> v.writeTo(o));
    }

    @Override
    public String getWriteableName() {
        return TYPE;
    }

    @Override
    public Version getMinimalSupportedVersion() {
        return Version.V_5_7_0;
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject(TYPE);
        for (var entry : tables.entrySet()) {
            RelationName tableName = entry.getKey();
            ForeignTable table = entry.getValue();
            builder.startObject(tableName.fqn());
            table.toXContent(builder, params);
            builder.endObject();
        }
        return builder.endObject();
    }

    @Override
    public EnumSet<XContentContext> context() {
        return EnumSet.of(Metadata.XContentContext.GATEWAY, Metadata.XContentContext.SNAPSHOT);
    }

    public boolean contains(RelationName tableName) {
        return tables.containsKey(tableName);
    }

    public ForeignTablesMetadata add(RelationName tableName,
                                     Map<ColumnIdent, Reference> columns,
                                     String server,
                                     Map<String, Object> options) {
        HashMap<RelationName, ForeignTable> newTables = new HashMap<>(tables);
        newTables.put(tableName, new ForeignTable(tableName, columns, server, options));
        return new ForeignTablesMetadata(newTables);
    }

    @Nullable
    public ForeignTable get(RelationName name) {
        return tables.get(name);
    }

    public boolean anyDependOnServer(String serverName) {
        return tables.values().stream().anyMatch(x -> x.server().equals(serverName));
    }

    public ForeignTablesMetadata removeAllForServers(List<String> names) {
        HashMap<RelationName, ForeignTable> newTables = new HashMap<>();
        for (var entry : tables.entrySet()) {
            var relationName = entry.getKey();
            var foreignTable = entry.getValue();
            if (!names.contains(foreignTable.server())) {
                newTables.put(relationName, foreignTable);
            }
        }
        return newTables.size() == tables.size() ? this : new ForeignTablesMetadata(newTables);
    }

    public ForeignTablesMetadata remove(List<RelationName> relations, boolean ifExists) {
        HashMap<RelationName, ForeignTable> newTables = new HashMap<>(tables);
        for (var relation : relations) {
            ForeignTable removed = newTables.remove(relation);
            if (removed == null && !ifExists) {
                throw new RelationUnknown(relation);
            }
        }
        return newTables.size() == tables.size() ? this : new ForeignTablesMetadata(newTables);
    }

    @Override
    public Iterator<ForeignTable> iterator() {
        return tables.values().iterator();
    }

}
