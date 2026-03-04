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

import static java.util.Objects.requireNonNull;
import static org.elasticsearch.common.xcontent.XContentParser.Token.END_OBJECT;
import static org.elasticsearch.common.xcontent.XContentParser.Token.FIELD_NAME;
import static org.elasticsearch.common.xcontent.XContentParser.Token.START_OBJECT;

import java.io.IOException;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.elasticsearch.Version;
import org.elasticsearch.cluster.AbstractNamedDiffable;
import org.elasticsearch.cluster.metadata.Metadata;
import org.elasticsearch.cluster.metadata.Metadata.XContentContext;
import org.elasticsearch.cluster.metadata.RelationMetadata;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.XContentParser;
import org.jspecify.annotations.NonNull;
import org.jspecify.annotations.Nullable;

import io.crate.analyze.ParamTypeHints;
import io.crate.analyze.expressions.ExpressionAnalyzer;
import io.crate.analyze.relations.FieldProvider;
import io.crate.metadata.ColumnIdent;
import io.crate.metadata.CoordinatorTxnCtx;
import io.crate.metadata.IndexReference;
import io.crate.metadata.NodeContext;
import io.crate.metadata.Reference;
import io.crate.metadata.RelationName;
import io.crate.metadata.doc.DocTableInfoFactory;

@Deprecated(since = "6.3.0")
public final class ForeignTablesMetadata extends AbstractNamedDiffable<Metadata.Custom>
        implements Metadata.Custom, Iterable<RelationMetadata.ForeignTable> {

    public static final String TYPE = "foreign_tables";
    public static final ForeignTablesMetadata EMPTY = new ForeignTablesMetadata(Map.of());

    private final Map<RelationName, RelationMetadata.ForeignTable> tables;

    public ForeignTablesMetadata(Map<RelationName, RelationMetadata.ForeignTable> tables) {
        this.tables = tables;
    }

    public ForeignTablesMetadata(StreamInput in) throws IOException {
        this.tables = in.readMap(RelationName::new, RelationMetadata.ForeignTable::new);
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeMap(tables, (o, v) -> v.writeTo(o), (o, v) -> v.writeTo(o));
    }

    public static ForeignTablesMetadata fromXContent(NodeContext nodeCtx, XContentParser parser) throws IOException {
        HashMap<RelationName, RelationMetadata.ForeignTable> tables = new HashMap<>();
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
                RelationMetadata.ForeignTable table = fromXContent(nodeCtx, name, parser);
                tables.put(name, table);
            }
        }
        parser.nextToken();
        return new ForeignTablesMetadata(tables);
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

    public RelationMetadata.@Nullable ForeignTable get(RelationName name) {
        return tables.get(name);
    }

    @Override
    @NonNull
    public Iterator<RelationMetadata.ForeignTable> iterator() {
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

    private static RelationMetadata.ForeignTable fromXContent(NodeContext nodeCtx, RelationName name, XContentParser parser) throws IOException {
        Map<ColumnIdent, Reference> references = null;
        String server = null;
        Settings options = null;

        ExpressionAnalyzer expressionAnalyzer = new ExpressionAnalyzer(
            CoordinatorTxnCtx.systemTransactionContext(),
            nodeCtx,
            ParamTypeHints.EMPTY,
            FieldProvider.UNSUPPORTED,
            null
        );

        while (parser.nextToken() != END_OBJECT) {
            if (parser.currentToken() == FIELD_NAME) {
                String fieldName = parser.currentName();
                parser.nextToken();
                switch (fieldName) {
                    case "server":
                        server = parser.text();
                        break;

                    case "options":
                        options = Settings.fromXContent(parser);
                        break;

                    case "references":
                        references = new HashMap<>();
                        Map<ColumnIdent, IndexReference.Builder> indexColumns = new HashMap<>();
                        Set<Reference> droppedColumns = new HashSet<>();

                        Map<String, Object> properties = parser.map();
                        DocTableInfoFactory.parseColumns(
                            expressionAnalyzer,
                            name,
                            null,
                            Map.of(),
                            Set.of(),
                            List.of(),
                            List.of(),
                            properties,
                            indexColumns,
                            references,
                            droppedColumns
                        );
                        break;

                    default:
                        // skip over unknown fields for forward compatibility
                        parser.skipChildren();
                }
            }
        }
        return new RelationMetadata.ForeignTable(
            requireNonNull(name),
            requireNonNull(references),
            requireNonNull(server),
            requireNonNull(options)
        );
    }
}
