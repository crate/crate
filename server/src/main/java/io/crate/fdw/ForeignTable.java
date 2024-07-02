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

import java.io.IOException;
import java.util.Collection;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Stream;

import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.ToXContent;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentParser;
import org.jetbrains.annotations.Nullable;

import io.crate.analyze.ParamTypeHints;
import io.crate.analyze.WhereClause;
import io.crate.analyze.expressions.ExpressionAnalyzer;
import io.crate.analyze.relations.FieldProvider;
import io.crate.execution.ddl.tables.MappingUtil;
import io.crate.execution.ddl.tables.MappingUtil.AllocPosition;
import io.crate.metadata.ColumnIdent;
import io.crate.metadata.CoordinatorTxnCtx;
import io.crate.metadata.IndexReference;
import io.crate.metadata.NodeContext;
import io.crate.metadata.Reference;
import io.crate.metadata.RelationName;
import io.crate.metadata.Routing;
import io.crate.metadata.RoutingProvider;
import io.crate.metadata.RoutingProvider.ShardSelection;
import io.crate.metadata.RowGranularity;
import io.crate.metadata.doc.DocTableInfoFactory;
import io.crate.metadata.settings.CoordinatorSessionSettings;
import io.crate.metadata.table.Operation;
import io.crate.metadata.table.TableInfo;
import io.crate.types.DataTypes;

public record ForeignTable(RelationName name,
                           Map<ColumnIdent, Reference> references,
                           String server,
                           Settings options) implements Writeable, ToXContent, TableInfo {

    ForeignTable(StreamInput in) throws IOException {
        this(
            new RelationName(in),
            in.readMap(LinkedHashMap::new, ColumnIdent::of, Reference::fromStream),
            in.readString(),
            Settings.readSettingsFromStream(in)
        );
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        name.writeTo(out);
        out.writeMap(references, (o, v) -> v.writeTo(o), Reference::toStream);
        out.writeString(server);
        Settings.writeSettingsToStream(out, options);
    }

    public static ForeignTable fromXContent(NodeContext nodeCtx, RelationName name, XContentParser parser) throws IOException {
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
                        Map<ColumnIdent, String> analyzers = new HashMap<>();

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
                            analyzers,
                            references
                        );
                        break;

                    default:
                        // skip over unknown fields for forward compatibility
                        parser.skipChildren();
                }
            }
        }
        return new ForeignTable(
            requireNonNull(name),
            requireNonNull(references),
            requireNonNull(server),
            requireNonNull(options)
        );
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.field("server", server);
        builder.startObject("options");
        options.toXContent(builder, params);
        builder.endObject();

        Map<String, Map<String, Object>> properties = MappingUtil.toProperties(
            AllocPosition.forTable(this),
            Reference.buildTree(references.values())
        );
        builder.field("references", properties);
        return builder;
    }

    @Override
    public Collection<Reference> columns() {
        return references.values();
    }

    @Override
    public Iterator<Reference> iterator() {
        return references.values().iterator();
    }

    @Override
    public RowGranularity rowGranularity() {
        return RowGranularity.DOC;
    }

    @Override
    public RelationName ident() {
        return name;
    }

    @Override
    public List<ColumnIdent> primaryKey() {
        return List.of();
    }

    @Override
    public Settings parameters() {
        return Settings.EMPTY;
    }

    @Override
    public Set<Operation> supportedOperations() {
        return EnumSet.of(Operation.READ, Operation.SHOW_CREATE);
    }

    @Override
    public RelationType relationType() {
        return RelationType.FOREIGN;
    }

    @Nullable
    @Override
    public Reference getReference(ColumnIdent columnIdent) {
        return references.get(columnIdent);
    }

    @Override
    public Routing getRouting(ClusterState state,
                              RoutingProvider routingProvider,
                              WhereClause whereClause,
                              ShardSelection shardSelection,
                              CoordinatorSessionSettings sessionSettings) {
        return Routing.forTableOnSingleNode(name, state.nodes().getLocalNodeId());
    }

    public record Option(RelationName relationName, String name, String value) {
    }

    public Stream<Option> getOptions() {
        return options.getAsStructuredMap().entrySet().stream()
            .map(entry -> new Option(
                name,
                entry.getKey(),
                DataTypes.STRING.implicitCast(entry.getValue())
            ));
    }
}
