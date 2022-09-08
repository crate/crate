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

package io.crate.sql;

import static io.crate.sql.ExpressionFormatter.formatExpression;
import static io.crate.sql.ExpressionFormatter.formatStandaloneExpression;
import static io.crate.sql.tree.Insert.DuplicateKeyContext.Type.NONE;

import java.util.Arrays;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Optional;
import java.util.TreeMap;
import java.util.stream.Collector;
import java.util.stream.Collectors;

import javax.annotation.Nullable;

import io.crate.common.collections.Lists2;
import io.crate.sql.tree.AliasedRelation;
import io.crate.sql.tree.AllColumns;
import io.crate.sql.tree.AlterPublication;
import io.crate.sql.tree.AlterSubscription;
import io.crate.sql.tree.Assignment;
import io.crate.sql.tree.AstVisitor;
import io.crate.sql.tree.CheckColumnConstraint;
import io.crate.sql.tree.CheckConstraint;
import io.crate.sql.tree.ClusteredBy;
import io.crate.sql.tree.CollectionColumnType;
import io.crate.sql.tree.ColumnConstraint;
import io.crate.sql.tree.ColumnDefinition;
import io.crate.sql.tree.ColumnStorageDefinition;
import io.crate.sql.tree.ColumnType;
import io.crate.sql.tree.CopyFrom;
import io.crate.sql.tree.CreateFunction;
import io.crate.sql.tree.CreatePublication;
import io.crate.sql.tree.CreateSnapshot;
import io.crate.sql.tree.CreateSubscription;
import io.crate.sql.tree.CreateTable;
import io.crate.sql.tree.CreateUser;
import io.crate.sql.tree.DeclareCursor;
import io.crate.sql.tree.DecommissionNodeStatement;
import io.crate.sql.tree.DenyPrivilege;
import io.crate.sql.tree.DropAnalyzer;
import io.crate.sql.tree.DropBlobTable;
import io.crate.sql.tree.DropFunction;
import io.crate.sql.tree.DropPublication;
import io.crate.sql.tree.DropRepository;
import io.crate.sql.tree.DropSnapshot;
import io.crate.sql.tree.DropSubscription;
import io.crate.sql.tree.DropTable;
import io.crate.sql.tree.DropUser;
import io.crate.sql.tree.DropView;
import io.crate.sql.tree.EscapedCharStringLiteral;
import io.crate.sql.tree.Explain;
import io.crate.sql.tree.Expression;
import io.crate.sql.tree.FetchFromCursor;
import io.crate.sql.tree.FunctionArgument;
import io.crate.sql.tree.GCDanglingArtifacts;
import io.crate.sql.tree.GenericProperties;
import io.crate.sql.tree.GrantPrivilege;
import io.crate.sql.tree.IndexColumnConstraint;
import io.crate.sql.tree.IndexDefinition;
import io.crate.sql.tree.Insert;
import io.crate.sql.tree.IntegerLiteral;
import io.crate.sql.tree.IntervalLiteral;
import io.crate.sql.tree.Join;
import io.crate.sql.tree.JoinCriteria;
import io.crate.sql.tree.JoinOn;
import io.crate.sql.tree.JoinUsing;
import io.crate.sql.tree.LongLiteral;
import io.crate.sql.tree.NaturalJoin;
import io.crate.sql.tree.Node;
import io.crate.sql.tree.NotNullColumnConstraint;
import io.crate.sql.tree.ObjectColumnType;
import io.crate.sql.tree.PartitionedBy;
import io.crate.sql.tree.PrimaryKeyColumnConstraint;
import io.crate.sql.tree.PrimaryKeyConstraint;
import io.crate.sql.tree.PrivilegeStatement;
import io.crate.sql.tree.QualifiedName;
import io.crate.sql.tree.Query;
import io.crate.sql.tree.QuerySpecification;
import io.crate.sql.tree.RefreshStatement;
import io.crate.sql.tree.Relation;
import io.crate.sql.tree.RevokePrivilege;
import io.crate.sql.tree.Select;
import io.crate.sql.tree.SelectItem;
import io.crate.sql.tree.SetSessionAuthorizationStatement;
import io.crate.sql.tree.SingleColumn;
import io.crate.sql.tree.SortItem;
import io.crate.sql.tree.StringLiteral;
import io.crate.sql.tree.SwapTable;
import io.crate.sql.tree.Table;
import io.crate.sql.tree.TableFunction;
import io.crate.sql.tree.TableSubquery;
import io.crate.sql.tree.Union;
import io.crate.sql.tree.Update;
import io.crate.sql.tree.Values;
import io.crate.sql.tree.ValuesList;
import io.crate.sql.tree.Window;
import io.crate.sql.tree.WindowFrame;
import io.crate.sql.tree.With;
import io.crate.sql.tree.WithQuery;

public final class SqlFormatter {

    private static final String INDENT = "   ";
    private static final Collector<CharSequence, ?, String> COMMA_JOINER = Collectors.joining(", ");

    private SqlFormatter() {
    }

    public static String formatSql(Node root) {
        return formatSql(root, null);
    }

    public static String formatSql(Node root, @Nullable List<Expression> parameters) {
        StringBuilder builder = new StringBuilder();
        Formatter formatter = new Formatter(builder, parameters);
        root.accept(formatter, 0);
        return builder.toString();
    }

    private static class Formatter extends AstVisitor<Void, Integer> {
        private final StringBuilder builder;

        @Nullable
        private final List<Expression> parameters;

        Formatter(StringBuilder builder, @Nullable List<Expression> parameters) {
            this.builder = builder;
            this.parameters = parameters;
        }

        @Override
        protected Void visitNode(Node node, Integer indent) {
            throw new UnsupportedOperationException("not yet implemented: " + node);
        }

        @Override
        public Void visitSwapTable(SwapTable swapTable, Integer indent) {
            append(indent, "ALTER CLUSTER SWAP TABLE ");
            append(indent, swapTable.source().toString());
            append(indent, " TO ");
            append(indent, swapTable.target().toString());
            if (!swapTable.properties().isEmpty()) {
                append(indent, " ");
                swapTable.properties().accept(this, indent);
            }
            return null;
        }

        @Override
        public Void visitGCDanglingArtifacts(GCDanglingArtifacts gcDanglingArtifacts, Integer indent) {
            append(indent, "ALTER CLUSTER GC DANGLING ARTIFACTS");
            return null;
        }

        @Override
        public Void visitAlterClusterDecommissionNode(DecommissionNodeStatement<?> decommissionNode,
                                                      Integer indent) {
            append(indent, "ALTER CLUSTER DECOMMISSION ");
            ((DecommissionNodeStatement<Expression>) decommissionNode)
                .nodeIdOrName().accept(this, indent);
            return null;
        }

        @Override
        public Void visitCopyFrom(CopyFrom<?> node, Integer indent) {
            var copyFrom = (CopyFrom<Expression>) node;

            append(indent, "COPY ");
            copyFrom.table().accept(this, indent);
            var columns = node.columns().iterator();
            if (columns.hasNext()) {
                builder.append('(');
                while (columns.hasNext()) {
                    builder.append(columns.next());
                    if (columns.hasNext()) {
                        builder.append(", ");
                    }
                }
                builder.append(')');
            }
            append(indent, " FROM ");
            copyFrom.path().accept(this, indent);
            if (!copyFrom.properties().isEmpty()) {
                append(indent, " ");
                copyFrom.properties().accept(this, indent);
            }
            if (copyFrom.isReturnSummary()) {
                append(indent," RETURN SUMMARY");
            }
            return null;
        }

        @Override
        public Void visitRefreshStatement(RefreshStatement node, Integer indent) {
            append(indent, "REFRESH TABLE ");
            appendFlatNodeList(node.tables(), indent);
            return null;
        }

        @Override
        protected Void visitExplain(Explain node, Integer indent) {
            append(indent, "EXPLAIN");
            if (node.isAnalyze()) {
                builder.append(" ANALYZE");
            }
            return null;
        }

        @Override
        public Void visitInsert(Insert<?> node, Integer indent) {
            append(indent, "INSERT");
            builder.append(' ');
            append(indent, "INTO");
            builder.append(' ');
            node.table().accept(this, indent);
            builder.append(' ');
            var columns = node.columns().iterator();
            if (columns.hasNext()) {
                builder.append('(');
                while (columns.hasNext()) {
                    builder.append(columns.next());
                    if (columns.hasNext()) {
                        builder.append(", ");
                    }
                }
                builder.append(')');
            }
            builder.append(' ');
            node.insertSource().accept(this, indent);
            var duplicateKeyContext = node.duplicateKeyContext();
            if (duplicateKeyContext.getType() != NONE) {
                builder.append(" ON CONFLICT");
                var constraintColumns = duplicateKeyContext.getConstraintColumns().iterator();
                if (constraintColumns.hasNext()) {
                    builder.append(" (");
                    while (constraintColumns.hasNext()) {
                        builder.append(constraintColumns.next());
                        if (constraintColumns.hasNext()) {
                            builder.append(", ");
                        }
                    }
                    builder.append(')');
                }
                switch (duplicateKeyContext.getType()) {
                    case ON_CONFLICT_DO_NOTHING:
                        builder.append(" DO NOTHING");
                        break;
                    case ON_CONFLICT_DO_UPDATE_SET:
                        builder.append(" DO UPDATE");
                        var assignments = duplicateKeyContext.getAssignments().iterator();
                        if (assignments.hasNext()) {
                            builder.append(" SET ");
                            while (assignments.hasNext()) {
                                assignments.next().accept(this, indent);
                                if (assignments.hasNext()) {
                                    builder.append(", ");
                                }
                            }
                        }
                        break;
                    case NONE:
                    default:
                }
            }

            var returning = node.returningClause().iterator();
            if (returning.hasNext()) {
                append(indent, "RETURNING");
                while (returning.hasNext()) {
                    builder.append(' ');
                    returning.next().accept(this, indent);
                    if (returning.hasNext()) {
                        builder.append(',');
                    }
                }
            }
            return null;
        }

        @Override
        public Void visitUpdate(Update node, Integer indent) {
            append(indent, "UPDATE");
            builder.append(' ');
            node.relation().accept(this, indent);
            builder.append(' ');
            if (!node.assignments().isEmpty()) {
                append(indent, "SET");
                builder.append(' ');
                Iterator<Assignment<Expression>> assignments = node.assignments().iterator();
                while (assignments.hasNext()) {
                    assignments.next().accept(this, indent);
                    if (assignments.hasNext()) {
                        builder.append(',');
                    }
                }
                builder.append(' ');
            }
            node.whereClause().ifPresent(x -> {
                append(indent, "WHERE");
                builder.append(' ');
                x.accept(this, indent);
                builder.append(' ');
            });
            if (!node.returningClause().isEmpty()) {
                append(indent, "RETURNING");
                Iterator<SelectItem> returningItems = node.returningClause().iterator();
                while (returningItems.hasNext()) {
                    builder.append(' ');
                    returningItems.next().accept(this, indent);
                    if (returningItems.hasNext()) {
                        builder.append(',');
                    }
                }
            }
            return null;
        }

        @Override
        public Void visitAssignment(Assignment<?> node, Integer indent) {
            var assignment = (Assignment<Expression>) node;
            assignment.columnName().accept(this, indent);
            append(indent, "=");
            assignment.expression().accept(this, indent);
            return null;
        }

        @Override
        protected Void visitExpression(Expression node, Integer indent) {
            builder.append(formatStandaloneExpression(node, parameters));
            return null;
        }

        @Override
        protected Void visitQuery(Query node, Integer indent) {
            if (node.getWith().isPresent()) {
                node.getWith().get().accept(this, indent);
            }

            node.getQueryBody().accept(this, indent);

            if (!node.getOrderBy().isEmpty()) {
                append(indent,
                    "ORDER BY " + node.getOrderBy().stream()
                        .map(e -> formatSortItem(e, parameters))
                        .collect(COMMA_JOINER)
                ).append('\n');
            }

            if (node.getLimit().isPresent()) {
                append(indent, "LIMIT " + node.getLimit().get())
                    .append('\n');
            }

            if (node.getOffset().isPresent()) {
                append(indent, "OFFSET " + node.getOffset().get())
                    .append('\n');
            }

            return null;
        }

        @Override
        protected Void visitQuerySpecification(QuerySpecification node, Integer indent) {
            node.getSelect().accept(this, indent);

            if (!node.getFrom().isEmpty()) {
                append(indent, "FROM");
                if (node.getFrom().size() > 1) {
                    builder.append('\n');
                    append(indent, "  ");
                    Iterator<Relation> relations = node.getFrom().iterator();
                    while (relations.hasNext()) {
                        relations.next().accept(this, indent);
                        if (relations.hasNext()) {
                            builder.append('\n');
                            append(indent, ", ");
                        }
                    }
                } else {
                    builder.append(' ');
                    Lists2.getOnlyElement(node.getFrom()).accept(this, indent);
                }
            }

            builder.append('\n');

            if (node.getWhere().isPresent()) {
                append(indent, "WHERE " + formatStandaloneExpression(node.getWhere().get(), parameters))
                    .append('\n');
            }

            if (!node.getGroupBy().isEmpty()) {
                append(indent,
                    "GROUP BY " + node.getGroupBy().stream()
                        .map(e -> formatStandaloneExpression(e, parameters))
                        .collect(COMMA_JOINER))
                    .append('\n');
            }

            if (node.getHaving().isPresent()) {
                append(indent, "HAVING " + formatStandaloneExpression(node.getHaving().get(), parameters))
                    .append('\n');
            }

            if (!node.getWindows().isEmpty()) {
                append(indent, "WINDOW ");
                var windows = node.getWindows().entrySet().iterator();
                while (windows.hasNext()) {
                    Map.Entry<String, Window> window = windows.next();
                    append(indent, window.getKey()).append(" AS ");
                    window.getValue().accept(this, indent);
                    if (windows.hasNext()) {
                        append(indent, ", ");
                    }
                }
                builder.append('\n');
            }

            if (!node.getOrderBy().isEmpty()) {
                append(indent,
                    "ORDER BY " + node.getOrderBy().stream()
                        .map(e -> formatSortItem(e, parameters))
                        .collect(COMMA_JOINER)
                ).append('\n');
            }

            if (node.getLimit().isPresent()) {
                append(indent, "LIMIT " + node.getLimit().get())
                    .append('\n');
            }

            if (node.getOffset().isPresent()) {
                append(indent, "OFFSET " + node.getOffset().get())
                    .append('\n');
            }
            return null;
        }


        @Override
        public Void visitValues(Values values, Integer indent) {
            append(indent, "VALUES ");
            List<ValuesList> rows = values.rows();
            for (int i = 0; i < rows.size(); i++) {
                ValuesList row = rows.get(i);

                append(indent, "(");
                List<Expression> expressions = row.values();
                for (int j = 0; j < expressions.size(); j++) {
                    Expression value = expressions.get(j);
                    append(indent, formatExpression(value));
                    if (j + 1 < expressions.size()) {
                        append(indent, ", ");
                    }
                }
                append(indent, ")");

                if (i + 1 < rows.size()) {
                    append(indent, ", ");
                }
            }
            return null;
        }

        @Override
        protected Void visitSelect(Select node, Integer indent) {
            append(indent, "SELECT");
            if (node.isDistinct()) {
                builder.append(" DISTINCT");
            }

            if (node.getSelectItems().size() > 1) {
                boolean first = true;
                for (SelectItem item : node.getSelectItems()) {
                    builder.append("\n")
                        .append(indentString(indent))
                        .append(first ? "  " : ", ");

                    item.accept(this, indent);
                    first = false;
                }
            } else {
                builder.append(' ');
                Lists2.getOnlyElement(node.getSelectItems()).accept(this, indent);
            }

            builder.append('\n');
            return null;
        }

        @Override
        protected Void visitSingleColumn(SingleColumn node, Integer indent) {
            builder.append(formatStandaloneExpression(node.getExpression(), parameters));
            if (node.getAlias() != null) {
                builder
                    .append(' ')
                    .append(quoteIdentifierIfNeeded(node.getAlias()));
            }

            return null;
        }

        @Override
        protected Void visitAllColumns(AllColumns node, Integer indent) {
            builder.append(node.toString());
            return null;
        }

        @Override
        public Void visitTableFunction(TableFunction node, Integer context) {
            builder.append(node.name());
            builder.append("(");
            Iterator<Expression> iterator = node.functionCall().getArguments().iterator();
            while (iterator.hasNext()) {
                Expression expression = iterator.next();
                expression.accept(this, context);
                if (iterator.hasNext()) {
                    builder.append(", ");
                }
            }
            builder.append(")");
            return null;
        }

        @Override
        protected Void visitTable(Table<?> node, Integer indent) {
            if (node.excludePartitions()) {
                builder.append("ONLY ");
            }
            builder.append(formatQualifiedName(node.getName()));
            if (!node.partitionProperties().isEmpty()) {
                builder.append(" PARTITION (");
                for (Assignment assignment : node.partitionProperties()) {
                    builder.append(assignment.columnName().toString());
                    builder.append("=");
                    builder.append(assignment.expression().toString());
                }
                builder.append(")");
            }
            return null;
        }

        @Override
        public Void visitCreateTable(CreateTable node, Integer indent) {
            builder.append("CREATE TABLE ");
            if (node.ifNotExists()) {
                builder.append("IF NOT EXISTS ");
            }

            node.name().accept(this, indent);

            builder.append(" ");
            appendNestedNodeList(node.tableElements(), indent);

            Optional<ClusteredBy> clusteredBy = node.clusteredBy();
            if (clusteredBy.isPresent()) {
                builder.append("\n");
                clusteredBy.get().accept(this, indent);
            }
            Optional<PartitionedBy> partitionedBy = node.partitionedBy();
            if (partitionedBy.isPresent()) {
                builder.append("\n");
                partitionedBy.get().accept(this, indent);
            }
            if (!node.properties().isEmpty()) {
                builder.append("\n");
                node.properties().accept(this, indent);
            }
            return null;
        }

        @Override
        public Void visitCreateFunction(CreateFunction node, Integer indent) {
            builder.append("CREATE");
            if (node.replace()) {
                builder.append(" OR REPLACE");
            }
            builder
                .append(" FUNCTION ")
                .append(node.name());
            appendFlatNodeList(node.arguments(), indent);

            builder
                .append(" RETURNS ")
                .append(node.returnType()).append(" ")
                .append(" LANGUAGE ").append(node.language().toString().replace("'", "")).append(" ")
                .append(" AS ").append(node.definition().toString());
            return null;
        }

        @Override
        public Void visitCreateUser(CreateUser node, Integer indent) {
            builder.append("CREATE USER ").append(quoteIdentifierIfNeeded(node.name()));
            if (!node.properties().isEmpty()) {
                builder.append("\n");
                node.properties().accept(this, indent);
            }
            return null;
        }

        @Override
        public Void visitGrantPrivilege(GrantPrivilege node, Integer indent) {
            builder.append("GRANT ");
            appendPrivilegeStatement(node);
            return null;
        }

        @Override
        public Void visitDenyPrivilege(DenyPrivilege node, Integer context) {
            builder.append("DENY ");
            appendPrivilegeStatement(node);
            return null;
        }

        @Override
        public Void visitRevokePrivilege(RevokePrivilege node, Integer indent) {
            builder.append("REVOKE ");
            appendPrivilegeStatement(node);
            return null;
        }

        @Override
        public Void visitDropUser(DropUser node, Integer indent) {
            builder.append("DROP USER ");
            if (node.ifExists()) {
                builder.append("IF EXISTS ");
            }
            builder.append(quoteIdentifierIfNeeded(node.name()));
            return null;
        }

        @Override
        public Void visitFunctionArgument(FunctionArgument node, Integer context) {
            String name = node.name();
            if (name != null) {
                builder.append(name).append(" ");
            }
            builder.append(node.type());
            return null;
        }

        @Override
        public Void visitClusteredBy(ClusteredBy node, Integer indent) {
            append(indent, "CLUSTERED");
            if (node.column().isPresent()) {
                builder.append(String.format(Locale.ENGLISH, " BY (%s)", node.column().get().toString()));
            }
            if (node.numberOfShards().isPresent()) {
                builder.append(String.format(Locale.ENGLISH, " INTO %s SHARDS", node.numberOfShards().get()));
            }
            return null;
        }

        @Override
        public Void visitGenericProperties(GenericProperties node, Integer indent) {
            int count = 0, max = node.properties().size();
            if (max > 0) {
                builder.append("WITH (\n");
                @SuppressWarnings("unchecked")
                TreeMap<String, Expression> sortedMap = new TreeMap(node.properties());
                for (Map.Entry<String, Expression> propertyEntry : sortedMap.entrySet()) {
                    builder.append(indentString(indent + 1));
                    String key = propertyEntry.getKey();
                    if (propertyEntry.getKey().contains(".")) {
                        key = String.format(Locale.ENGLISH, "\"%s\"", key);
                    }
                    builder.append(key).append(" = ");
                    propertyEntry.getValue().accept(this, indent);
                    if (++count < max) builder.append(",");
                    builder.append("\n");
                }
                append(indent, ")");
            }
            return null;
        }

        @Override
        protected Void visitLongLiteral(LongLiteral node, Integer indent) {
            builder.append(node.getValue());
            return null;
        }

        @Override
        protected Void visitIntegerLiteral(IntegerLiteral node, Integer indent) {
            builder.append(node.getValue());
            return null;
        }

        @Override
        protected Void visitStringLiteral(StringLiteral node, Integer indent) {
            builder.append(Literals.quoteStringLiteral(node.getValue()));
            return null;
        }

        @Override
        protected Void visitEscapedCharStringLiteral(EscapedCharStringLiteral node, Integer context) {
            builder.append(Literals.quoteEscapedStringLiteral(node.getRawValue()));
            return null;
        }

        @Override
        public Void visitColumnDefinition(ColumnDefinition<?> node, Integer indent) {
            ColumnDefinition<Expression> columnDefinition = (ColumnDefinition<Expression>) node;
            builder.append(quoteIdentifierIfNeeded(columnDefinition.ident()))
                .append(" ");
            ColumnType type = columnDefinition.type();
            if (type != null) {
                type.accept(this, indent);
            }
            if (columnDefinition.defaultExpression() != null) {
                builder.append(" DEFAULT ")
                    .append(formatStandaloneExpression(columnDefinition.defaultExpression(), parameters));
            }
            if (columnDefinition.generatedExpression() != null) {
                builder.append(" GENERATED ALWAYS AS ")
                    .append(formatStandaloneExpression(columnDefinition.generatedExpression(), parameters));
            }

            if (!columnDefinition.constraints().isEmpty()) {
                for (ColumnConstraint constraint : columnDefinition.constraints()) {
                    builder.append(" ");
                    constraint.accept(this, indent);
                }
            }
            return null;
        }

        @Override
        public Void visitColumnType(ColumnType node, Integer indent) {
            builder.append(node.name().toUpperCase(Locale.ENGLISH));
            if (node.parametrized()) {
                builder
                    .append("(")
                    .append(
                        node.parameters().stream()
                            .map(String::valueOf)
                            .collect(Collectors.joining(", ")))
                    .append(')');
            }
            return null;
        }

        @Override
        public Void visitObjectColumnType(ObjectColumnType node, Integer indent) {
            ObjectColumnType<Expression> objectColumnType = node;
            builder.append("OBJECT");
            if (objectColumnType.objectType().isPresent()) {
                builder.append('(');
                builder.append(objectColumnType.objectType().get().name());
                builder.append(')');
            }
            if (!objectColumnType.nestedColumns().isEmpty()) {
                builder.append(" AS ");
                appendNestedNodeList(objectColumnType.nestedColumns(), indent);
            }
            return null;
        }

        @Override
        public Void visitCollectionColumnType(CollectionColumnType node, Integer indent) {
            builder.append(node.name().toUpperCase(Locale.ENGLISH))
                .append("(");
            node.innerType().accept(this, indent);
            builder.append(")");
            return null;
        }

        @Override
        public Void visitIndexColumnConstraint(IndexColumnConstraint node, Integer indent) {
            builder.append("INDEX ");
            if (node.equals(IndexColumnConstraint.off())) {
                builder.append(node.indexMethod().toUpperCase(Locale.ENGLISH));
            } else {
                builder.append("USING ")
                    .append(node.indexMethod().toUpperCase(Locale.ENGLISH));
                if (!node.properties().isEmpty()) {
                    builder.append(" ");
                    node.properties().accept(this, indent);
                }
            }
            return null;
        }

        @Override
        public Void visitColumnStorageDefinition(ColumnStorageDefinition node, Integer indent) {
            builder.append("STORAGE ");
            if (!node.properties().isEmpty()) {
                node.properties().accept(this, indent);
            }
            return null;
        }

        @Override
        public Void visitPrimaryKeyColumnConstraint(PrimaryKeyColumnConstraint node, Integer indent) {
            builder.append("PRIMARY KEY");
            return null;
        }

        @Override
        public Void visitNotNullColumnConstraint(NotNullColumnConstraint node, Integer indent) {
            builder.append("NOT NULL");
            return null;
        }

        @Override
        public Void visitPrimaryKeyConstraint(PrimaryKeyConstraint node, Integer indent) {
            builder.append("PRIMARY KEY ");
            appendFlatNodeList(node.columns(), indent);
            return null;
        }

        private void visitCheckConstraint(@Nullable String uniqueName, String expressionStr) {
            if (uniqueName != null) {
                builder.append("CONSTRAINT ").append(uniqueName).append(" ");
            }
            builder.append("CHECK(").append(expressionStr).append(")");
        }

        @Override
        public Void visitCheckConstraint(CheckConstraint<?> node, Integer indent) {
            visitCheckConstraint(node.name(), node.expressionStr());
            return null;
        }

        @Override
        public Void visitCheckColumnConstraint(CheckColumnConstraint<?> node, Integer indent) {
            visitCheckConstraint(node.name(), node.expressionStr());
            return null;
        }

        @Override
        public Void visitIndexDefinition(IndexDefinition node, Integer indent) {
            builder.append("INDEX ")
                .append(quoteIdentifierIfNeeded(node.ident()))
                .append(" USING ")
                .append(node.method().toUpperCase(Locale.ENGLISH))
                .append(" ");
            appendFlatNodeList(node.columns(), indent);
            if (!node.properties().isEmpty()) {
                builder.append(" ");
                node.properties().accept(this, indent);
            }
            return null;
        }

        @Override
        public Void visitPartitionedBy(PartitionedBy node, Integer indent) {
            append(indent, "PARTITIONED BY ");
            appendFlatNodeList(node.columns(), indent);
            return null;
        }

        @Override
        protected Void visitUnion(Union node, Integer context) {
            node.getLeft().accept(this, context);
            builder.append(" UNION ");
            if (!node.isDistinct()) {
                builder.append(" ALL");
            }
            builder.append(" ");
            node.getRight().accept(this, context);
            return null;
        }

        @Override
        protected Void visitJoin(Join node, Integer indent) {
            JoinCriteria criteria = node.getCriteria().orElse(null);
            String type = node.getType().toString();
            if (criteria instanceof NaturalJoin) {
                type = "NATURAL " + type;
            }

            builder.append('(');
            node.getLeft().accept(this, indent);

            builder.append('\n');
            append(indent, type).append(" JOIN ");

            node.getRight().accept(this, indent);

            if (criteria instanceof JoinUsing) {
                JoinUsing using = (JoinUsing) criteria;
                builder.append(" USING (")
                    .append(String.join(", ", using.getColumns()))
                    .append(")");
            } else if (criteria instanceof JoinOn) {
                JoinOn on = (JoinOn) criteria;
                builder.append(" ON (")
                    .append(formatStandaloneExpression(on.getExpression(), parameters))
                    .append(")");
            } else if (node.getType() != Join.Type.CROSS && !(criteria instanceof NaturalJoin)) {
                throw new UnsupportedOperationException("unknown join criteria: " + criteria);
            }

            builder.append(")");

            return null;
        }

        @Override
        protected Void visitAliasedRelation(AliasedRelation node, Integer indent) {
            node.getRelation().accept(this, indent);
            builder.append(' ')
                .append(node.getAlias());
            appendAliasColumns(builder, node.getColumnNames());
            return null;
        }

        @Override
        protected Void visitTableSubquery(TableSubquery node, Integer indent) {
            builder.append('(')
                .append('\n');

            node.getQuery().accept(this, indent + 1);

            append(indent, ")");

            return null;
        }

        @Override
        public Void visitDropRepository(DropRepository node, Integer indent) {
            builder.append("DROP REPOSITORY ")
                .append(quoteIdentifierIfNeeded(node.name()));
            return null;
        }

        @Override
        public Void visitCreateSnapshot(CreateSnapshot<?> node, Integer indent) {
            builder.append("CREATE SNAPSHOT ")
                .append(formatQualifiedName(node.name()));
            if (!node.tables().isEmpty()) {
                builder.append(" TABLE ");
                int count = 0, max = node.tables().size();
                for (Table table : node.tables()) {
                    table.accept(this, indent);
                    if (++count < max) builder.append(",");
                }
            } else {
                builder.append(" ALL");
            }
            if (!node.properties().isEmpty()) {
                builder.append(' ');
                node.properties().accept(this, indent);
            }
            return null;
        }

        @Override
        public Void visitDropTable(DropTable<?> node, Integer indent) {
            builder.append("DROP TABLE ");
            if (node.dropIfExists()) {
                builder.append("IF EXISTS ");
            }
            node.table().accept(this, indent);
            return null;
        }

        @Override
        public Void visitDropBlobTable(DropBlobTable<?> node, Integer indent) {
            builder.append("DROP BLOB TABLE ");
            if (node.ignoreNonExistentTable()) {
                builder.append("IF EXISTS ");
            }
            node.table().accept(this, indent);
            return null;
        }

        @Override
        public Void visitDropView(DropView node, Integer indent) {
            builder.append("DROP VIEW ");
            if (node.ifExists()) {
                builder.append("IF EXISTS ");
            }
            builder.append(node.names().stream()
                .map(Formatter::formatQualifiedName)
                .collect(COMMA_JOINER));
            return null;
        }

        @Override
        public Void visitIntervalLiteral(IntervalLiteral node, Integer indent) {
            builder.append(IntervalLiteral.format(node));
            return null;
        }

        @Override
        public Void visitDropAnalyzer(DropAnalyzer node, Integer indent) {
            builder.append("DROP ANALYZER ")
                .append(quoteIdentifierIfNeeded(node.name()));
            return null;
        }

        @Override
        public Void visitDropFunction(DropFunction node, Integer indent) {
            builder.append("DROP FUNCTION ");
            if (node.exists()) {
                builder.append("IF EXISTS ");
            }
            builder.append(formatQualifiedName(node.name()));
            appendFlatNodeList(node.arguments(), indent);
            return null;
        }

        @Override
        public Void visitDropSnapshot(DropSnapshot node, Integer indent) {
            builder.append("DROP REPOSITORY ")
                .append(formatQualifiedName(node.name()));
            return null;
        }

        @Override
        public Void visitCreatePublication(CreatePublication createPublication, Integer context) {
            builder.append("CREATE PUBLICATION ")
                .append(quoteIdentifierIfNeeded(createPublication.name()));
            if (createPublication.isForAllTables()) {
                builder.append(" FOR ALL TABLES");
            } else if (createPublication.tables().isEmpty() == false) {
                builder.append(" FOR TABLE ");
                builder.append(
                    createPublication.tables().stream()
                        .map(Formatter::formatQualifiedName)
                        .collect(COMMA_JOINER)
                );
            }
            return null;
        }

        @Override
        public Void visitDropPublication(DropPublication dropPublication, Integer context) {
            builder.append("DROP PUBLICATION ");
            if (dropPublication.ifExists()) {
                builder.append(" IF EXISTS ");
            }
            builder.append(quoteIdentifierIfNeeded(dropPublication.name()));
            return null;
        }

        @Override
        public Void visitAlterPublication(AlterPublication alterPublication, Integer context) {
            builder.append("ALTER PUBLICATION ")
                .append(quoteIdentifierIfNeeded(alterPublication.name()));
            builder.append(" ")
                .append(alterPublication.operation())
                .append(" ");
            builder.append("TABLE ");
            builder.append(
                alterPublication.tables().stream()
                    .map(Formatter::formatQualifiedName)
                    .collect(COMMA_JOINER)
            );
            return null;
        }

        @Override
        public Void visitCreateSubscription(CreateSubscription<?> createSubscription,
                                            Integer context) {
            var subscription = (CreateSubscription<Expression>) createSubscription;
            builder.append("CREATE SUBSCRIPTION ")
                .append(quoteIdentifierIfNeeded(subscription.name()))
                .append(" CONNECTION ");
            subscription.connectionInfo().accept(this, context);
            builder.append(" PUBLICATION ");
            builder.append(
                subscription.publications().stream()
                    .map(Formatter::quoteIdentifierIfNeeded)
                    .collect(COMMA_JOINER)
            );
            if (!subscription.properties().isEmpty()) {
                builder.append(" ");
                subscription.properties().accept(this, context);
            }
            return null;
        }

        @Override
        public Void visitDropSubscription(DropSubscription dropSubscription, Integer context) {
            builder.append("DROP SUBSCRIPTION ");
            if (dropSubscription.ifExists()) {
                builder.append(" IF EXISTS ");
            }
            builder.append(quoteIdentifierIfNeeded(dropSubscription.name()));
            return null;
        }

        @Override
        public Void visitAlterSubscription(AlterSubscription alterSubscription, Integer context) {
            builder.append("ALTER SUBSCRIPTION ")
                .append(quoteIdentifierIfNeeded(alterSubscription.name()))
                .append(" ")
                .append(alterSubscription.mode());
            return null;
        }

        @Override
        public Void visitWith(With with, Integer context) {
            builder.append("WITH ");
            var queriesIt = with.withQueries().iterator();
            while (queriesIt.hasNext()) {
                queriesIt.next().accept(this, context);
                if (queriesIt.hasNext()) {
                    builder.append(",");
                }
            }
            builder.append("\n");
            return null;
        }

        @Override
        public Void visitWithQuery(WithQuery withQuery, Integer context) {
            builder.append(withQuery.name());
            if (withQuery.columnNames().isEmpty() == false) {
                builder.append("(")
                    .append(withQuery.columnNames().stream().collect(COMMA_JOINER))
                    .append(")");
            }
            builder.append(" AS (");
            withQuery.query().accept(this, context);
            builder.append(")");
            return null;
        }

        @Override
        public Void visitWindow(Window window, Integer indent) {
            append(indent, "(");
            if (window.windowRef() != null) {
                append(indent, window.windowRef());
            }
            if (!window.getPartitions().isEmpty()) {
                append(indent, " PARTITION BY ");
                var partitions = window.getPartitions().iterator();
                while (partitions.hasNext()) {
                    partitions.next().accept(this, indent);
                    if (partitions.hasNext()) {
                        append(indent, ", ");
                    }
                }
            }
            if (!window.getOrderBy().isEmpty()) {
                append(indent, " ORDER BY ");
                var sortItems = window.getOrderBy().iterator();
                while (sortItems.hasNext()) {
                    sortItems.next().accept(this, indent);
                    if (sortItems.hasNext()) {
                        append(indent, ", ");
                    }
                }
            }
            window.getWindowFrame().map(frame -> frame.accept(this, indent));
            append(indent, ")");
            return null;
        }

        @Override
        public Void visitWindowFrame(WindowFrame frame, Integer indent) {
            append(indent, " ");
            append(indent, frame.mode().name());

            append(indent, " ");
            Expression startOffset = frame.getStart().getValue();
            if (startOffset != null) {
                startOffset.accept(this, indent);
                append(indent, " ");
            }
            append(indent, frame.getStart().getType().name());

            frame.getEnd().map(end -> {
                append(indent, " AND ");
                Expression endOffset = end.getValue();
                if (endOffset != null) {
                    endOffset.accept(this, indent);
                    append(indent, " ");
                }
                append(indent, end.getType().name());
                return null;
            });
            return null;
        }

        @Override
        protected Void visitSortItem(SortItem node, Integer indent) {
            node.getSortKey().accept(this, indent);
            return null;
        }

        @Override
        public Void visitSetSessionAuthorizationStatement(SetSessionAuthorizationStatement node,
                                                          Integer context) {
            var user = node.user();
            builder
                .append("SET ")
                .append(node.scope())
                .append(" SESSION AUTHORIZATION ")
                .append(user != null ? quoteIdentifierIfNeeded(user) : "DEFAULT");
            return null;
        }

        @Override
        public Void visitDeclareCursor(DeclareCursor declareCursor, Integer context) {
            builder
                .append("DECLARE ")
                .append(declareCursor.getCursorName())
                .append(" CURSOR FOR ")
                .append(declareCursor.getQuery().accept(this, context));
            return null;
        }

        @Override
        public Void visitFetchFromCursor(FetchFromCursor fetchFromCursor, Integer context) {
            builder
                .append("FETCH FORWARD")
                .append(fetchFromCursor.count())
                .append(" FROM ")
                .append(fetchFromCursor.getCursorName());
            return null;
        }

        private void appendPrivilegesList(List<String> privilegeTypes) {
            int j = 0;
            for (String privilegeType : privilegeTypes) {
                builder.append(privilegeType);
                if (j < privilegeTypes.size() - 1) {
                    builder.append(", ");
                }
                j++;
            }
        }

        private void appendUsersList(List<String> userNames) {
            for (int i = 0; i < userNames.size(); i++) {
                builder.append(quoteIdentifierIfNeeded(userNames.get(i)));
                if (i < userNames.size() - 1) {
                    builder.append(", ");
                }
            }
        }

        private void appendTableOrSchemaNames(List<QualifiedName> tableOrSchemaNames) {
            for (int i = 0; i < tableOrSchemaNames.size(); i++) {
                builder.append(quoteIdentifierIfNeeded(tableOrSchemaNames.get(i).toString()));
                if (i < tableOrSchemaNames.size() - 1) {
                    builder.append(", ");
                }
            }
        }

        private void appendPrivilegeStatement(PrivilegeStatement node) {
            if (node.privileges().isEmpty()) {
                builder.append(" ALL ");
            } else {
                appendPrivilegesList(node.privileges());
            }

            if (!node.clazz().equals("CLUSTER")) {
                builder.append(" ON " + node.clazz() + " ");
                appendTableOrSchemaNames(node.privilegeIdents());
            }

            if (node instanceof RevokePrivilege) {
                builder.append(" FROM ");
            } else {
                builder.append(" TO ");
            }
            appendUsersList(node.userNames());
        }

        private static String formatQualifiedName(QualifiedName name) {
            return name.getParts().stream()
                .map(Formatter::quoteIdentifierIfNeeded)
                .collect(Collectors.joining("."));
        }

        private static String quoteIdentifierIfNeeded(String identifier) {
            return Arrays.stream(identifier.split("\\."))
                .map(Identifiers::quote)
                .collect(Collectors.joining("."));
        }

        private void appendFlatNodeList(List<? extends Node> nodes, Integer indent) {
            int count = 0, max = nodes.size();
            builder.append("(");
            for (Node node : nodes) {
                node.accept(this, indent);
                if (++count < max) builder.append(", ");
            }
            builder.append(")");
        }

        private void appendNestedNodeList(List<? extends Node> nodes, Integer indent) {
            int count = 0, max = nodes.size();
            builder.append("(\n");
            for (Node node : nodes) {
                builder.append(indentString(indent + 1));
                node.accept(this, indent + 1);
                if (++count < max) builder.append(",");
                builder.append("\n");
            }
            append(indent, ")");
        }

        private StringBuilder append(int indent, String value) {
            return builder.append(indentString(indent)).append(value);
        }

        private static String indentString(int indent) {
            return String.join("", Collections.nCopies(indent, INDENT));
        }
    }

    static String formatSortItem(SortItem sortItem, List<Expression> parameters) {
        StringBuilder sb = new StringBuilder();
        sb.append(formatStandaloneExpression(sortItem.getSortKey(), parameters));
        switch (sortItem.getOrdering()) {
            case ASCENDING:
                sb.append(" ASC");
                break;
            case DESCENDING:
                sb.append(" DESC");
                break;
            default:
                throw new UnsupportedOperationException("unknown ordering: " + sortItem.getOrdering());
        }

        switch (sortItem.getNullOrdering()) {
            case FIRST:
                sb.append(" NULLS FIRST");
                break;
            case LAST:
                sb.append(" NULLS LAST");
                break;
            default:
                break;
        }
        return sb.toString();
    }

    private static void appendAliasColumns(StringBuilder builder, List<String> columns) {
        if (!columns.isEmpty()) {
            builder.append(" (")
                .append(String.join(", ", columns))
                .append(')');
        }
    }
}
