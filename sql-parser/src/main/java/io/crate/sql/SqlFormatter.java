/*
 * Licensed to CRATE Technology GmbH ("Crate") under one or more contributor
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

import com.google.common.base.Function;
import com.google.common.base.Joiner;
import com.google.common.base.Splitter;
import com.google.common.base.Strings;
import com.google.common.collect.Iterables;
import io.crate.sql.tree.*;

import java.util.*;

import static com.google.common.collect.Iterables.transform;
import static io.crate.sql.ExpressionFormatter.expressionFormatterFunction;
import static io.crate.sql.ExpressionFormatter.formatExpression;

public final class SqlFormatter {
    private static final String INDENT = "   ";

    private SqlFormatter() {
    }

    public static String formatSql(Node root) {
        StringBuilder builder = new StringBuilder();
        new Formatter(builder).process(root, 0);
        return builder.toString();
    }

    private static class Formatter extends AstVisitor<Void, Integer> {
        private final StringBuilder builder;

        public Formatter(StringBuilder builder) {
            this.builder = builder;
        }

        @Override
        protected Void visitNode(Node node, Integer indent) {
            throw new UnsupportedOperationException("not yet implemented: " + node);
        }

        @Override
        public Void visitCopyFrom(CopyFrom node, Integer indent) {
            append(indent, "COPY ");
            process(node.table(), indent);
            append(indent, " FROM ");
            process(node.path(), indent);
            if (node.genericProperties().isPresent()) {
                append(indent, " ");
                process(node.genericProperties().get(), indent);
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
            append(indent, "EXPLAIN ");
            for (ExplainOption explainOption : node.getOptions()) {
                process(explainOption, indent);
            }
            process(node.getStatement(), indent);
            return null;
        }

        @Override
        protected Void visitExpression(Expression node, Integer indent) {
            builder.append(formatExpression(node));
            return null;
        }

        @Override
        protected Void visitQuery(Query node, Integer indent) {
            if (node.getWith().isPresent()) {
                With with = node.getWith().get();
                append(indent, "WITH");
                if (with.isRecursive()) {
                    builder.append(" RECURSIVE");
                }
                builder.append("\n  ");
                Iterator<WithQuery> queries = with.getQueries().iterator();
                while (queries.hasNext()) {
                    WithQuery query = queries.next();
                    append(indent, query.getName());
                    appendAliasColumns(builder, query.getColumnNames());
                    builder.append(" AS ");
                    process(new TableSubquery(query.getQuery()), indent);
                    builder.append('\n');
                    if (queries.hasNext()) {
                        builder.append(", ");
                    }
                }
            }

            process(node.getQueryBody(), indent);

            if (!node.getOrderBy().isEmpty()) {
                append(indent,
                    "ORDER BY " + Joiner.on(", ").join(transform(node.getOrderBy(), orderByFormatterFunction())))
                    .append('\n');
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
            process(node.getSelect(), indent);

            if (node.getFrom() != null) {
                append(indent, "FROM");
                if (node.getFrom().size() > 1) {
                    builder.append('\n');
                    append(indent, "  ");
                    Iterator<Relation> relations = node.getFrom().iterator();
                    while (relations.hasNext()) {
                        process(relations.next(), indent);
                        if (relations.hasNext()) {
                            builder.append('\n');
                            append(indent, ", ");
                        }
                    }
                } else {
                    builder.append(' ');
                    process(Iterables.getOnlyElement(node.getFrom()), indent);
                }
            }

            builder.append('\n');

            if (node.getWhere().isPresent()) {
                append(indent, "WHERE " + formatExpression(node.getWhere().get()))
                    .append('\n');
            }

            if (!node.getGroupBy().isEmpty()) {
                append(indent,
                    "GROUP BY " + Joiner.on(", ").join(transform(node.getGroupBy(), expressionFormatterFunction())))
                    .append('\n');
            }

            if (node.getHaving().isPresent()) {
                append(indent, "HAVING " + formatExpression(node.getHaving().get()))
                    .append('\n');
            }

            if (!node.getOrderBy().isEmpty()) {
                append(indent,
                    "ORDER BY " + Joiner.on(", ").join(transform(node.getOrderBy(), orderByFormatterFunction())))
                    .append('\n');
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

                    process(item, indent);
                    first = false;
                }
            } else {
                builder.append(' ');
                process(Iterables.getOnlyElement(node.getSelectItems()), indent);
            }

            builder.append('\n');
            return null;
        }

        @Override
        protected Void visitSingleColumn(SingleColumn node, Integer indent) {
            builder.append(formatExpression(node.getExpression()));
            if (node.getAlias().isPresent()) {
                builder.append(' ')
                    .append('"')
                    .append(node.getAlias().get())
                    .append('"'); // TODO: handle quoting properly
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
            Iterator<Expression> iterator = node.arguments().iterator();
            while (iterator.hasNext()) {
                Expression expression = iterator.next();
                process(expression, context);
                if (iterator.hasNext()) {
                    builder.append(", ");
                }
            }
            builder.append(")");
            return null;
        }

        @Override
        protected Void visitTable(Table node, Integer indent) {
            if (node.excludePartitions()) {
                builder.append("ONLY ");
            }
            builder.append(quoteIdentifierIfNeeded(node.getName().toString()));
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

            if (!node.crateTableOptions().isEmpty()) {
                builder.append("\n");
                int count = 0, max = node.crateTableOptions().size();
                for (CrateTableOption option : node.crateTableOptions()) {
                    option.accept(this, indent);
                    if (++count < max) builder.append("\n");
                }
            }

            if (node.properties().isPresent() && !node.properties().get().isEmpty()) {
                builder.append("\n");
                node.properties().get().accept(this, indent);
            }
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
            builder.append(String.format(Locale.ENGLISH, "%d", node.getValue()));
            return null;
        }

        @Override
        protected Void visitStringLiteral(StringLiteral node, Integer indent) {
            builder.append(Literals.quoteStringLiteral(node.getValue()));
            return null;
        }

        @Override
        public Void visitColumnDefinition(ColumnDefinition node, Integer indent) {
            builder.append(quoteIdentifierIfNeeded(node.ident()))
                .append(" ");
            if (node.type() != null) {
                node.type().accept(this, indent);
            }
            if (node.expression() != null) {
                builder.append(" GENERATED ALWAYS AS ")
                    .append(formatExpression(node.expression()));
            }

            if (!node.constraints().isEmpty()) {
                for (ColumnConstraint constraint : node.constraints()) {
                    builder.append(" ");
                    constraint.accept(this, indent);
                }
            }
            return null;
        }

        @Override
        public Void visitColumnType(ColumnType node, Integer indent) {
            builder.append(node.name().toUpperCase(Locale.ENGLISH));
            return null;
        }

        @Override
        public Void visitObjectColumnType(ObjectColumnType node, Integer indent) {
            builder.append("OBJECT");
            if (node.objectType().isPresent()) {
                builder.append(String.format(Locale.ENGLISH, " (%s)", node.objectType().get().toUpperCase(Locale.ENGLISH)));
            }
            if (!node.nestedColumns().isEmpty()) {
                builder.append(" AS ");
                appendNestedNodeList(node.nestedColumns(), indent);
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
            if (node.equals(IndexColumnConstraint.OFF)) {
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
                process(node.properties(), indent);
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
        protected Void visitJoin(Join node, Integer indent) {
            JoinCriteria criteria = node.getCriteria().orNull();
            String type = node.getType().toString();
            if (criteria instanceof NaturalJoin) {
                type = "NATURAL " + type;
            }

            builder.append('(');
            process(node.getLeft(), indent);

            builder.append('\n');
            append(indent, type).append(" JOIN ");

            process(node.getRight(), indent);

            if (criteria instanceof JoinUsing) {
                JoinUsing using = (JoinUsing) criteria;
                builder.append(" USING (")
                    .append(Joiner.on(", ").join(using.getColumns()))
                    .append(")");
            } else if (criteria instanceof JoinOn) {
                JoinOn on = (JoinOn) criteria;
                builder.append(" ON (")
                    .append(formatExpression(on.getExpression()))
                    .append(")");
            } else if (node.getType() != Join.Type.CROSS && !(criteria instanceof NaturalJoin)) {
                throw new UnsupportedOperationException("unknown join criteria: " + criteria);
            }

            builder.append(")");

            return null;
        }

        @Override
        protected Void visitUnion(Union node, Integer context) {
            for (int i = 0; i < node.getRelations().size(); i++) {
                process(node.getRelations().get(i), context);
                if (i < node.getRelations().size() - 1) {
                    builder.append("UNION ");
                    if (!node.isDistinct()) {
                        builder.append("ALL ");
                    }
                }
            }
            return null;
        }

        @Override
        protected Void visitAliasedRelation(AliasedRelation node, Integer indent) {
            process(node.getRelation(), indent);

            builder.append(' ')
                .append(node.getAlias());

            appendAliasColumns(builder, node.getColumnNames());

            return null;
        }

        @Override
        protected Void visitSampledRelation(SampledRelation node, Integer indent) {
            process(node.getRelation(), indent);

            builder.append(" TABLESAMPLE ")
                .append(node.getType())
                .append(" (")
                .append(node.getSamplePercentage())
                .append(')');

            if (node.getColumnsToStratifyOn().isPresent()) {
                builder.append(" STRATIFY ON ")
                    .append(" (")
                    .append(Joiner.on(",").join(node.getColumnsToStratifyOn().get()));
                builder.append(')');
            }

            return null;
        }

        @Override
        protected Void visitTableSubquery(TableSubquery node, Integer indent) {
            builder.append('(')
                .append('\n');

            process(node.getQuery(), indent + 1);

            append(indent, ")");

            return null;
        }

        @Override
        public Void visitDropRepository(DropRepository node, Integer indent) {
            builder.append("DROP REPOSITORY ")
                .append(quoteIdentifierIfNeeded(node.repository()));
            return null;
        }

        @Override
        public Void visitCreateSnapshot(CreateSnapshot node, Integer indent) {
            builder.append("CREATE SNAPSHOT ")
                .append(quoteIdentifierIfNeeded(node.name().toString()));
            if (node.tableList().isPresent()) {
                builder.append(" TABLE ");
                int count = 0, max = node.tableList().get().size();
                for (Table table : node.tableList().get()) {
                    table.accept(this, indent);
                    if (++count < max) builder.append(",");
                }
            } else {
                builder.append(" ALL");
            }
            if (node.properties().isPresent()) {
                builder.append(' ');
                node.properties().get().accept(this, indent);
            }
            return null;
        }

        private String quoteIdentifierIfNeeded(String identifier) {
            List<String> quoted = new ArrayList<>();
            for (String part : Splitter.on(".").split(identifier)) {
                quoted.add(Identifiers.quote(part));
            }
            return Joiner.on(".").join(quoted);
        }

        private Void appendFlatNodeList(List<? extends Node> nodes, Integer indent) {
            int count = 0, max = nodes.size();
            builder.append("(");
            for (Node node : nodes) {
                node.accept(this, indent);
                if (++count < max) builder.append(", ");
            }
            builder.append(")");
            return null;
        }

        private Void appendNestedNodeList(List<? extends Node> nodes, Integer indent) {
            int count = 0, max = nodes.size();
            builder.append("(\n");
            for (Node node : nodes) {
                builder.append(indentString(indent + 1));
                node.accept(this, indent + 1);
                if (++count < max) builder.append(",");
                builder.append("\n");
            }
            append(indent, ")");
            return null;
        }

        private StringBuilder append(int indent, String value) {
            return builder.append(indentString(indent)).append(value);
        }

        private static String indentString(int indent) {
            return Strings.repeat(INDENT, indent);
        }
    }

    static Function<SortItem, String> orderByFormatterFunction() {
        return new Function<SortItem, String>() {
            @Override
            public String apply(SortItem input) {
                StringBuilder builder = new StringBuilder();

                builder.append(formatExpression(input.getSortKey()));

                switch (input.getOrdering()) {
                    case ASCENDING:
                        builder.append(" ASC");
                        break;
                    case DESCENDING:
                        builder.append(" DESC");
                        break;
                    default:
                        throw new UnsupportedOperationException("unknown ordering: " + input.getOrdering());
                }

                switch (input.getNullOrdering()) {
                    case FIRST:
                        builder.append(" NULLS FIRST");
                        break;
                    case LAST:
                        builder.append(" NULLS LAST");
                        break;
                    case UNDEFINED:
                        // no op
                        break;
                    default:
                        throw new UnsupportedOperationException("unknown null ordering: " + input.getNullOrdering());
                }

                return builder.toString();
            }
        };
    }

    private static void appendAliasColumns(StringBuilder builder, List<String> columns) {
        if ((columns != null) && (!columns.isEmpty())) {
            builder.append(" (");
            Joiner.on(", ").appendTo(builder, columns);
            builder.append(')');
        }
    }

}
