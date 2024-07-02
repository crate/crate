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

import static org.elasticsearch.common.xcontent.XContentParser.Token.END_OBJECT;
import static org.elasticsearch.common.xcontent.XContentParser.Token.FIELD_NAME;
import static org.elasticsearch.common.xcontent.XContentParser.Token.START_OBJECT;

import java.io.IOException;
import java.util.Collection;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import org.elasticsearch.Version;
import org.elasticsearch.cluster.AbstractNamedDiffable;
import org.elasticsearch.cluster.metadata.Metadata;
import org.elasticsearch.cluster.metadata.Metadata.XContentContext;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentParser;
import org.jetbrains.annotations.Nullable;

import io.crate.exceptions.RelationUnknown;
import io.crate.metadata.NodeContext;
import io.crate.metadata.Reference;
import io.crate.metadata.RelationName;

public final class ForeignTablesMetadata extends AbstractNamedDiffable<Metadata.Custom>
        implements Metadata.Custom, Iterable<ForeignTable> {

    public static final String TYPE = "foreign_tables";
    public static final ForeignTablesMetadata EMPTY = new ForeignTablesMetadata(Map.of());

    private final Map<RelationName, ForeignTable> tables;

    ForeignTablesMetadata(Map<RelationName, ForeignTable> tables) {
        this.tables = tables;
    }

    public ForeignTablesMetadata(StreamInput in) throws IOException {
        this.tables = in.readMap(RelationName::new, ForeignTable::new);
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeMap(tables, (o, v) -> v.writeTo(o), (o, v) -> v.writeTo(o));
    }

    public static ForeignTablesMetadata fromXContent(NodeContext nodeCtx, XContentParser parser) throws IOException {
        HashMap<RelationName, ForeignTable> tables = new HashMap<>();
        if (parser.currentToken() == START_OBJECT) {
            parser.nextToken();
        }
        if (parser.currentToken() == FIELD_NAME) {
            assert parser.currentName().endsWith(TYPE) : "toXContent starts with startObject(TYPE)";
            parser.nextToken();
        }
        while (parser.nextToken() != END_OBJECT) {
            if (parser.currentToken() == FIELD_NAME) {
                RelationName name = RelationName.fromIndexName(parser.currentName());
                parser.nextToken();
                ForeignTable table = ForeignTable.fromXContent(nodeCtx, name, parser);
                tables.put(name, table);
            }
        }
        parser.nextToken();
        return new ForeignTablesMetadata(tables);
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject(TYPE);
        for (var entry : tables.entrySet()) {
            RelationName tableName = entry.getKey();
            ForeignTable table = entry.getValue();
            builder.startObject(tableName.indexNameOrAlias());
            table.toXContent(builder, params);
            builder.endObject();
        }
        return builder.endObject();
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
    public EnumSet<XContentContext> context() {
        return EnumSet.of(Metadata.XContentContext.GATEWAY, Metadata.XContentContext.SNAPSHOT);
    }

    public boolean contains(RelationName tableName) {
        return tables.containsKey(tableName);
    }

    public ForeignTablesMetadata add(RelationName tableName,
                                     Collection<Reference> columns,
                                     String server,
                                     Settings options) {
        HashMap<RelationName, ForeignTable> newTables = new HashMap<>(tables);
        ForeignTable value = new ForeignTable(
            tableName,
            columns.stream().collect(Collectors.toMap(Reference::column, x -> x)),
            server,
            options
        );
        newTables.put(tableName, value);
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

    @Override
    public int hashCode() {
        return tables.hashCode();
    }

    @Override
    public boolean equals(Object obj) {
        return obj instanceof ForeignTablesMetadata other
            && tables.equals(other.tables);
    }

    public Iterable<ForeignTable.Option> tableOptions() {
        return () -> tables.values().stream()
            .flatMap(table -> table.getOptions())
            .iterator();
    }
}
