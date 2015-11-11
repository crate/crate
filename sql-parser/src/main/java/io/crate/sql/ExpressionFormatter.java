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
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Ordering;
import com.google.common.collect.TreeMultimap;
import io.crate.sql.tree.*;

import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.Map;

import static com.google.common.collect.Iterables.transform;
import static io.crate.sql.SqlFormatter.formatSql;
import static io.crate.sql.SqlFormatter.orderByFormatterFunction;

public final class ExpressionFormatter {

    private static final Joiner COMMA_JOINER = Joiner.on(", ");
    private static final Joiner WHITESPACE_JOINER = Joiner.on(' ');

    private ExpressionFormatter() {}

    public static String formatExpression(Expression expression)
    {
        return new Formatter().process(expression, null);
    }

    public static Function<Expression, String> expressionFormatterFunction()
    {
        return new Function<Expression, String>()
        {
            @Override
            public String apply(Expression input)
            {
                return ExpressionFormatter.formatExpression(input);
            }
        };
    }

    public static class Formatter
            extends AstVisitor<String, Void>
    {



        @Override
        protected String visitNode(Node node, Void context)
        {
            throw new UnsupportedOperationException(String.format(Locale.ENGLISH, "cannot handle node '%s'", node.toString()));
        }

        @Override
        protected String visitExpression(Expression node, Void context)
        {
            throw new UnsupportedOperationException(String.format("not yet implemented: %s.visit%s", getClass().getName(), node.getClass().getSimpleName()));
        }

        @Override
        protected String visitCurrentTime(CurrentTime node, Void context)
        {
            StringBuilder builder = new StringBuilder();
            switch (node.getType()) {
                case TIME:
                    builder.append("current_time");
                    break;
                case DATE:
                    builder.append("current_date");
                    break;
                case TIMESTAMP:
                    builder.append("current_timestamp");
                    break;
                default:
                    throw new UnsupportedOperationException("not yet implemented: " + node.getType());
            }

            if (node.getPrecision().isPresent()) {
                builder.append('(')
                        .append(node.getPrecision().get())
                        .append(')');
            }

            return builder.toString();
        }

        @Override
        protected String visitExtract(Extract node, Void context)
        {
            return "EXTRACT(" + node.getField() + " FROM " + process(node.getExpression(), context) + ")";
        }

        @Override
        protected String visitBooleanLiteral(BooleanLiteral node, Void context)
        {
            return String.valueOf(node.getValue());
        }

        @Override
        protected String visitSubscriptExpression(SubscriptExpression node, Void context) {
            return String.format("%s[%s]", node.name(), node.index());
        }

        @Override
        public String visitParameterExpression(ParameterExpression node, Void context) {
            return String.format("$%s", node.position());
        }

        @Override
        protected String visitStringLiteral(StringLiteral node, Void context)
        {
            return "'" + node.getValue().replace("'", "''") + "'";
        }

        @Override
        protected String visitLongLiteral(LongLiteral node, Void context)
        {
            return Long.toString(node.getValue());
        }

        @Override
        protected String visitDoubleLiteral(DoubleLiteral node, Void context)
        {
            return Double.toString(node.getValue());
        }

        @Override
        protected String visitTimeLiteral(TimeLiteral node, Void context)
        {
            return "TIME '" + node.getValue() + "'";
        }

        @Override
        protected String visitTimestampLiteral(TimestampLiteral node, Void context)
        {
            return "TIMESTAMP '" + node.getValue() + "'";
        }

        @Override
        protected String visitNullLiteral(NullLiteral node, Void context)
        {
            return "null";
        }

        @Override
        protected String visitDateLiteral(DateLiteral node, Void context)
        {
            return "DATE '" + node.getValue() + "'";
        }

        @Override
        public String visitArrayLiteral(ArrayLiteral node, Void context) {
            StringBuilder builder = new StringBuilder("[");
            boolean first = true;
            for (Expression element : node.values()) {
                if (!first) {
                    builder.append(",");
                } else {
                    first = false;
                }
                builder.append(element.accept(this, context));

            }
            return builder.append("]").toString();
        }

        @Override
        public String visitObjectLiteral(ObjectLiteral node, Void context) {
            StringBuilder builder = new StringBuilder("{");
            boolean first = true;
            TreeMultimap<String, Expression> sorted = TreeMultimap.create(
                    Ordering.natural().nullsLast(),
                    Ordering.usingToString().nullsLast()
            );
            sorted.putAll(node.values());
            for (Map.Entry<String, Expression> entry : sorted.entries()) {
                if (!first) {
                    builder.append(", ");
                } else {
                    first = false;
                }
                builder.append(formatIdentifier(entry.getKey()))
                       .append("= ")
                       .append(entry.getValue().accept(this, context));

            }
            return builder.append("}").toString();
        }

        @Override
        protected String visitSubqueryExpression(SubqueryExpression node, Void context)
        {
            return "(" + formatSql(node.getQuery()) + ")";
        }

        @Override
        protected String visitExists(ExistsPredicate node, Void context)
        {
            return "EXISTS (" + formatSql(node.getSubquery()) + ")";
        }

        @Override
        protected String visitQualifiedNameReference(QualifiedNameReference node, Void context)
        {
            List<String> parts = new ArrayList<>();
            for (String part : node.getName().getParts()) {
                parts.add(formatIdentifier(part));
            }
            return Joiner.on('.').join(parts);
        }

        @Override
        public String visitInputReference(InputReference node, Void context)
        {
            // add colon so this won't parse
            return ":input(" + node.getInput().getChannel() + ")";
        }

        @Override
        protected String visitFunctionCall(FunctionCall node, Void context)
        {
            StringBuilder builder = new StringBuilder();

            String arguments = joinExpressions(node.getArguments());
            if (node.getArguments().isEmpty() && "count".equalsIgnoreCase(node.getName().getSuffix())) {
                arguments = "*";
            }
            if (node.isDistinct()) {
                arguments = "DISTINCT " + arguments;
            }

            builder.append(node.getName())
                    .append('(').append(arguments).append(')');

            if (node.getWindow().isPresent()) {
                builder.append(" OVER ").append(visitWindow(node.getWindow().get(), null));
            }

            return builder.toString();
        }

        @Override
        protected String visitLogicalBinaryExpression(LogicalBinaryExpression node, Void context)
        {
            return formatBinaryExpression(node.getType().toString(), node.getLeft(), node.getRight());
        }

        @Override
        protected String visitNotExpression(NotExpression node, Void context)
        {
            return "(NOT " + process(node.getValue(), null) + ")";
        }

        @Override
        protected String visitComparisonExpression(ComparisonExpression node, Void context)
        {
            return formatBinaryExpression(node.getType().getValue(), node.getLeft(), node.getRight());
        }

        @Override
        protected String visitIsNullPredicate(IsNullPredicate node, Void context)
        {
            return "(" + process(node.getValue(), null) + " IS NULL)";
        }

        @Override
        protected String visitIsNotNullPredicate(IsNotNullPredicate node, Void context)
        {
            return "(" + process(node.getValue(), null) + " IS NOT NULL)";
        }

        @Override
        protected String visitNullIfExpression(NullIfExpression node, Void context)
        {
            return "NULLIF(" + process(node.getFirst(), null) + ", " + process(node.getSecond(), null) + ')';
        }

        @Override
        protected String visitIfExpression(IfExpression node, Void context)
        {
            StringBuilder builder = new StringBuilder();
            builder.append("IF(")
                    .append(process(node.getCondition(), context))
                    .append(", ")
                    .append(process(node.getTrueValue(), context));
            if (node.getFalseValue().isPresent()) {
                builder.append(", ")
                        .append(process(node.getFalseValue().get(), context));
            }
            builder.append(")");
            return builder.toString();
        }

        @Override
        protected String visitCoalesceExpression(CoalesceExpression node, Void context)
        {
            return "COALESCE(" + joinExpressions(node.getOperands()) + ")";
        }

        @Override
        protected String visitNegativeExpression(NegativeExpression node, Void context)
        {
            return "-" + process(node.getValue(), null);
        }

        @Override
        protected String visitArithmeticExpression(ArithmeticExpression node, Void context)
        {
            return formatBinaryExpression(node.getType().getValue(), node.getLeft(), node.getRight());
        }

        @Override
        protected String visitLikePredicate(LikePredicate node, Void context)
        {
            StringBuilder builder = new StringBuilder();

            builder.append('(')
                    .append(process(node.getValue(), null))
                    .append(" LIKE ")
                    .append(process(node.getPattern(), null));

            if (node.getEscape() != null) {
                builder.append(" ESCAPE ")
                        .append(process(node.getEscape(), null));
            }

            builder.append(')');

            return builder.toString();
        }

        @Override
        public String visitArrayLikePredicate(ArrayLikePredicate node, Void context) {
            StringBuilder builder = new StringBuilder();
            builder.append('(')
                    .append(process(node.getPattern(), null))
                    .append(node.inverse() ? " NOT" : "")
                    .append(" LIKE ")
                    .append(node.quantifier().name())
                    .append(" (")
                    .append(process(node.getValue(), null))
                    .append(") ");
            if (node.getEscape() != null) {
                builder.append("ESCAPE ")
                        .append(process(node.getEscape(), null));
            }
            builder.append(')');
            return builder.toString();
        }

        @Override
        public String visitMatchPredicate(MatchPredicate node, Void context)
        {
            StringBuilder builder = new StringBuilder();
            builder.append("MATCH (");
            if (node.idents().size() == 1) {
                builder.append(process(node.idents().get(0).columnIdent(), context));
            } else {
                builder.append("(");
                List<MatchPredicateColumnIdent> idents = node.idents();
                for (int i = 0, identsSize = idents.size(); i < identsSize; i++) {
                    MatchPredicateColumnIdent ident = idents.get(i);
                    builder.append(ident.accept(this, null));
                    if (i < (identsSize-1)) {
                        builder.append(", ");
                    }
                }
                builder.append(")");
            }
            builder.append(", ").append(process(node.value(), context));
            builder.append(")");
            if (node.matchType() != null) {
                builder.append(" USING ").append(node.matchType()).append(" ");
                if (node.properties().properties().size() > 0) {
                    builder.append(process(node.properties(), context));
                }
            }
            return builder.toString();
        }

        @Override
        public String visitMatchPredicateColumnIdent(MatchPredicateColumnIdent node, Void context) {
            String column = process(node.columnIdent(), null);
            if (!(node.boost() instanceof NullLiteral)) {
                column = column + " " + node.boost().toString();
            }
            return column;
        }

        @Override
        public String visitGenericProperties(GenericProperties node, Void context) {
            StringBuilder builder = new StringBuilder().append(" WITH (");
            String properties = COMMA_JOINER.join(transform(node.properties().entrySet(), new Function<Map.Entry<String, Expression>, Object>()
            {
                @Override
                public Object apply(Map.Entry<String, Expression> input)
                {
                    return input.getKey() + "=" + process(input.getValue(), null);
                }
            }));
            builder.append(properties);
            builder.append(")");
            return builder.toString();
        }

        @Override
        protected String visitAllColumns(AllColumns node, Void context)
        {
            if (node.getPrefix().isPresent()) {
                return node.getPrefix().get() + ".*";
            }

            return "*";
        }

        @Override
        public String visitCast(Cast node, Void context)
        {
            return "CAST(" + process(node.getExpression(), context) + " AS " + process(node.getType(), context) + ")";
        }

        @Override
        protected String visitTryCast(TryCast node, Void context) {
            return "TRY_CAST(" + process(node.getExpression(), context) + " AS " + process(node.getType(), context) + ")";
        }

        @Override
        protected String visitSearchedCaseExpression(SearchedCaseExpression node, Void context)
        {
            ImmutableList.Builder<String> parts = ImmutableList.builder();
            parts.add("CASE");
            for (WhenClause whenClause : node.getWhenClauses()) {
                parts.add(process(whenClause, context));
            }
            if (node.getDefaultValue() != null) {
                parts.add("ELSE")
                        .add(process(node.getDefaultValue(), context));
            }
            parts.add("END");

            return "(" + WHITESPACE_JOINER.join(parts.build()) + ")";
        }

        @Override
        protected String visitSimpleCaseExpression(SimpleCaseExpression node, Void context)
        {
            ImmutableList.Builder<String> parts = ImmutableList.builder();

            parts.add("CASE")
                    .add(process(node.getOperand(), context));

            for (WhenClause whenClause : node.getWhenClauses()) {
                parts.add(process(whenClause, context));
            }
            if (node.getDefaultValue() != null) {
                parts.add("ELSE")
                        .add(process(node.getDefaultValue(), context));
            }
            parts.add("END");

            return "(" + WHITESPACE_JOINER.join(parts.build()) + ")";
        }

        @Override
        protected String visitWhenClause(WhenClause node, Void context)
        {
            return "WHEN " + process(node.getOperand(), context) + " THEN " + process(node.getResult(), context);
        }

        @Override
        protected String visitBetweenPredicate(BetweenPredicate node, Void context)
        {
            return "(" + process(node.getValue(), context) + " BETWEEN " +
                    process(node.getMin(), context) + " AND " + process(node.getMax(), context) + ")";
        }

        @Override
        protected String visitInPredicate(InPredicate node, Void context)
        {
            return "(" + process(node.getValue(), context) + " IN " + process(node.getValueList(), context) + ")";
        }

        @Override
        protected String visitInListExpression(InListExpression node, Void context)
        {
            return "(" + joinExpressions(node.getValues()) + ")";
        }

        // TODO: add tests for window clause formatting, as these are not really expressions
        @Override
        public String visitWindow(Window node, Void context)
        {
            List<String> parts = new ArrayList<>();

            if (!node.getPartitionBy().isEmpty()) {
                parts.add("PARTITION BY " + joinExpressions(node.getPartitionBy()));
            }
            if (!node.getOrderBy().isEmpty()) {
                parts.add("ORDER BY " + COMMA_JOINER.join(transform(node.getOrderBy(), orderByFormatterFunction())));
            }
            if (node.getFrame().isPresent()) {
                parts.add(process(node.getFrame().get(), null));
            }

            return '(' + WHITESPACE_JOINER.join(parts) + ')';
        }

        @Override
        public String visitWindowFrame(WindowFrame node, Void context)
        {
            StringBuilder builder = new StringBuilder();

            builder.append(node.getType().toString()).append(' ');

            if (node.getEnd().isPresent()) {
                builder.append("BETWEEN ")
                        .append(process(node.getStart(), null))
                        .append(" AND ")
                        .append(process(node.getEnd().get(), null));
            }
            else {
                builder.append(process(node.getStart(), null));
            }

            return builder.toString();
        }

        @Override
        public String visitFrameBound(FrameBound node, Void context)
        {
            switch (node.getType()) {
                case UNBOUNDED_PRECEDING:
                    return "UNBOUNDED PRECEDING";
                case PRECEDING:
                    return process(node.getValue().get(), null) + " PRECEDING";
                case CURRENT_ROW:
                    return "CURRENT ROW";
                case FOLLOWING:
                    return process(node.getValue().get(), null) + " FOLLOWING";
                case UNBOUNDED_FOLLOWING:
                    return "UNBOUNDED FOLLOWING";
            }
            throw new IllegalArgumentException("unhandled type: " + node.getType());
        }

        @Override
        public String visitColumnType(ColumnType node, Void context) {
            return node.name();
        }

        @Override
        public String visitCollectionColumnType(CollectionColumnType node, Void context) {
            return node.name() + "(" + process(node.innerType(), context) + ")";
        }

        @Override
        public String visitObjectColumnType(ObjectColumnType node, Void context) {
            return node.name();
        }

        private String formatBinaryExpression(String operator, Expression left, Expression right)
        {
            return '(' + process(left, null) + ' ' + operator + ' ' + process(right, null) + ')';
        }

        private String joinExpressions(List<Expression> expressions)
        {
            return COMMA_JOINER.join(transform(expressions, new Function<Expression, Object>()
            {
                @Override
                public Object apply(Expression input)
                {
                    return process(input, null);
                }
            }));
        }

        private static String formatIdentifier(String s)
        {
            // TODO: handle escaping properly
            return '"' + s + '"';
        }
    }
}
