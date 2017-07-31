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

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Ordering;
import com.google.common.collect.TreeMultimap;
import io.crate.sql.tree.*;

import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collector;
import java.util.stream.Collectors;

import static io.crate.sql.SqlFormatter.formatSql;

public final class ExpressionFormatter {

    private static final Collector<CharSequence, ?, String> COMMA_JOINER = Collectors.joining(", ");

    private static final Set<String> FUNCTION_CALLS_WITHOUT_PARENTHESIS = ImmutableSet.of(
        "current_catalog", "current_schema", "current_user", "session_user", "user");

    private ExpressionFormatter() {
    }

    public static String formatExpression(Expression expression) {
        return new Formatter().process(expression, null);
    }

    public static class Formatter extends AstVisitor<String, Void> {

        @Override
        protected String visitNode(Node node, Void context) {
            throw new UnsupportedOperationException(String.format(Locale.ENGLISH, "cannot handle node '%s'", node.toString()));
        }

        @Override
        protected String visitExpression(Expression node, Void context) {
            throw new UnsupportedOperationException(String.format(Locale.ENGLISH,
                "not yet implemented: %s.visit%s", getClass().getName(), node.getClass().getSimpleName()));
        }

        @Override
        public String visitArrayComparisonExpression(ArrayComparisonExpression node, Void context) {
            StringBuilder builder = new StringBuilder();

            String array = node.getRight().toString();
            String left = node.getLeft().toString();
            String type = node.getType().getValue();

            builder.append(left + " " + type + " ANY(" + array + ")");
            return builder.toString();
        }

        @Override
        protected String visitCurrentTime(CurrentTime node, Void context) {
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
        protected String visitExtract(Extract node, Void context) {
            return "EXTRACT(" + node.getField() + " FROM " + process(node.getExpression(), context) + ")";
        }

        @Override
        protected String visitBooleanLiteral(BooleanLiteral node, Void context) {
            return String.valueOf(node.getValue());
        }

        @Override
        protected String visitSubscriptExpression(SubscriptExpression node, Void context) {
            return node.name() + "[" + node.index() + "]";
        }

        @Override
        public String visitParameterExpression(ParameterExpression node, Void context) {
            return "$" + node.position();
        }

        @Override
        protected String visitStringLiteral(StringLiteral node, Void context) {
            return Literals.quoteStringLiteral(node.getValue());
        }

        @Override
        protected String visitLongLiteral(LongLiteral node, Void context) {
            return Long.toString(node.getValue());
        }

        @Override
        protected String visitDoubleLiteral(DoubleLiteral node, Void context) {
            return Double.toString(node.getValue());
        }

        @Override
        protected String visitTimeLiteral(TimeLiteral node, Void context) {
            return "TIME '" + node.getValue() + "'";
        }

        @Override
        protected String visitTimestampLiteral(TimestampLiteral node, Void context) {
            return "TIMESTAMP '" + node.getValue() + "'";
        }

        @Override
        protected String visitNullLiteral(NullLiteral node, Void context) {
            return "null";
        }

        @Override
        protected String visitDateLiteral(DateLiteral node, Void context) {
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
        protected String visitSubqueryExpression(SubqueryExpression node, Void context) {
            return "(" + formatSql(node.getQuery()) + ")";
        }

        @Override
        protected String visitExists(ExistsPredicate node, Void context) {
            return "EXISTS (" + formatSql(node.getSubquery()) + ")";
        }

        @Override
        protected String visitQualifiedNameReference(QualifiedNameReference node, Void context) {
            return node.getName().getParts().stream()
                .map(Formatter::formatIdentifier)
                .collect(Collectors.joining("."));
        }

        @Override
        protected String visitFunctionCall(FunctionCall node, Void context) {
            StringBuilder builder = new StringBuilder();

            String arguments = joinExpressions(node.getArguments());
            if (node.getArguments().isEmpty() && "count".equalsIgnoreCase(node.getName().getSuffix())) {
                arguments = "*";
            }
            if (node.isDistinct()) {
                arguments = "DISTINCT " + arguments;
            }

            builder.append(node.getName());
            if (!FUNCTION_CALLS_WITHOUT_PARENTHESIS.contains(node.getName().toString())) {
                builder.append('(').append(arguments).append(')');
            }

            return builder.toString();
        }

        @Override
        protected String visitLogicalBinaryExpression(LogicalBinaryExpression node, Void context) {
            return formatBinaryExpression(node.getType().toString(), node.getLeft(), node.getRight());
        }

        @Override
        protected String visitNotExpression(NotExpression node, Void context) {
            return "(NOT " + process(node.getValue(), null) + ")";
        }

        @Override
        protected String visitComparisonExpression(ComparisonExpression node, Void context) {
            return formatBinaryExpression(node.getType().getValue(), node.getLeft(), node.getRight());
        }

        @Override
        protected String visitIsNullPredicate(IsNullPredicate node, Void context) {
            return "(" + process(node.getValue(), null) + " IS NULL)";
        }

        @Override
        protected String visitIsNotNullPredicate(IsNotNullPredicate node, Void context) {
            return "(" + process(node.getValue(), null) + " IS NOT NULL)";
        }

        @Override
        protected String visitIfExpression(IfExpression node, Void context) {
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
        protected String visitNegativeExpression(NegativeExpression node, Void context) {
            return "- " + process(node.getValue(), null);
        }

        @Override
        protected String visitArithmeticExpression(ArithmeticExpression node, Void context) {
            return formatBinaryExpression(node.getType().getValue(), node.getLeft(), node.getRight());
        }

        @Override
        protected String visitLikePredicate(LikePredicate node, Void context) {
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
        public String visitMatchPredicate(MatchPredicate node, Void context) {
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
                    if (i < (identsSize - 1)) {
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
            return " WITH (" +
                node.properties().entrySet().stream()
                    .map(prop -> prop.getKey() + "=" + process(prop.getValue(), null))
                    .collect(COMMA_JOINER) +
                ")";
        }

        @Override
        protected String visitAllColumns(AllColumns node, Void context) {
            if (node.getPrefix().isPresent()) {
                return node.getPrefix().get() + ".*";
            }

            return "*";
        }

        @Override
        public String visitCast(Cast node, Void context) {
            return "CAST(" + process(node.getExpression(), context) + " AS " + process(node.getType(), context) + ")";
        }

        @Override
        protected String visitTryCast(TryCast node, Void context) {
            return "TRY_CAST(" + process(node.getExpression(), context) + " AS " + process(node.getType(), context) +
                ")";
        }

        @Override
        protected String visitSearchedCaseExpression(SearchedCaseExpression node, Void context) {
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

            return "(" + String.join(" ", parts.build()) + ")";
        }

        @Override
        protected String visitSimpleCaseExpression(SimpleCaseExpression node, Void context) {
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

            return "(" + String.join(" ", parts.build()) + ")";
        }

        @Override
        protected String visitWhenClause(WhenClause node, Void context) {
            return "WHEN " + process(node.getOperand(), context) + " THEN " + process(node.getResult(), context);
        }

        @Override
        protected String visitBetweenPredicate(BetweenPredicate node, Void context) {
            return "(" + process(node.getValue(), context) + " BETWEEN " +
                process(node.getMin(), context) + " AND " + process(node.getMax(), context) + ")";
        }

        @Override
        protected String visitInPredicate(InPredicate node, Void context) {
            return "(" + process(node.getValue(), context) + " IN " + process(node.getValueList(), context) + ")";
        }

        @Override
        protected String visitInListExpression(InListExpression node, Void context) {
            return "(" + joinExpressions(node.getValues()) + ")";
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

        private String formatBinaryExpression(String operator, Expression left, Expression right) {
            return '(' + process(left, null) + ' ' + operator + ' ' + process(right, null) + ')';
        }

        private String joinExpressions(List<Expression> expressions) {
            return expressions.stream()
                .map(expression -> process(expression, null))
                .collect(COMMA_JOINER);
        }

        private static String formatIdentifier(String s) {
            return Identifiers.quote(s);
        }
    }
}
