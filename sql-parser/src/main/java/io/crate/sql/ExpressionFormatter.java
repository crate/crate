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
import io.crate.sql.tree.AllColumns;
import io.crate.sql.tree.ArithmeticExpression;
import io.crate.sql.tree.ArrayComparisonExpression;
import io.crate.sql.tree.ArrayLikePredicate;
import io.crate.sql.tree.ArrayLiteral;
import io.crate.sql.tree.ArraySubQueryExpression;
import io.crate.sql.tree.AstVisitor;
import io.crate.sql.tree.BetweenPredicate;
import io.crate.sql.tree.BooleanLiteral;
import io.crate.sql.tree.Cast;
import io.crate.sql.tree.CollectionColumnType;
import io.crate.sql.tree.ColumnType;
import io.crate.sql.tree.ComparisonExpression;
import io.crate.sql.tree.CurrentTime;
import io.crate.sql.tree.DoubleLiteral;
import io.crate.sql.tree.EscapedCharStringLiteral;
import io.crate.sql.tree.ExistsPredicate;
import io.crate.sql.tree.Expression;
import io.crate.sql.tree.Extract;
import io.crate.sql.tree.FrameBound;
import io.crate.sql.tree.FunctionCall;
import io.crate.sql.tree.GenericProperties;
import io.crate.sql.tree.IfExpression;
import io.crate.sql.tree.InListExpression;
import io.crate.sql.tree.InPredicate;
import io.crate.sql.tree.IntervalLiteral;
import io.crate.sql.tree.IsNotNullPredicate;
import io.crate.sql.tree.IsNullPredicate;
import io.crate.sql.tree.LikePredicate;
import io.crate.sql.tree.LogicalBinaryExpression;
import io.crate.sql.tree.LongLiteral;
import io.crate.sql.tree.MatchPredicate;
import io.crate.sql.tree.MatchPredicateColumnIdent;
import io.crate.sql.tree.NegativeExpression;
import io.crate.sql.tree.Node;
import io.crate.sql.tree.NotExpression;
import io.crate.sql.tree.NullLiteral;
import io.crate.sql.tree.ObjectColumnType;
import io.crate.sql.tree.ObjectLiteral;
import io.crate.sql.tree.ParameterExpression;
import io.crate.sql.tree.QualifiedNameReference;
import io.crate.sql.tree.SearchedCaseExpression;
import io.crate.sql.tree.SimpleCaseExpression;
import io.crate.sql.tree.SortItem;
import io.crate.sql.tree.StringLiteral;
import io.crate.sql.tree.SubqueryExpression;
import io.crate.sql.tree.SubscriptExpression;
import io.crate.sql.tree.TryCast;
import io.crate.sql.tree.WhenClause;
import io.crate.sql.tree.Window;
import io.crate.sql.tree.WindowFrame;

import javax.annotation.Nullable;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collector;
import java.util.stream.Collectors;

import static io.crate.sql.SqlFormatter.formatSql;

public final class ExpressionFormatter {

    private static final Formatter DEFAULT_FORMATTER = new Formatter();

    private static final Collector<CharSequence, ?, String> COMMA_JOINER = Collectors.joining(", ");

    private static final Set<String> FUNCTION_CALLS_WITHOUT_PARENTHESIS = ImmutableSet.of(
        "current_catalog", "current_schema", "current_user", "session_user", "user");

    private ExpressionFormatter() {
    }

    /**
     * Formats the given expression and removes the outer most parenthesis, which are optional from an expression
     * correctness perspective, but clutter for the user (in case of nested expressions the inner expression will still
     * be enclosed in parenthesis, as that is a requirement for correctness, but the outer most expression will not be
     * surrounded by parenthesis)
     */
    public static String formatStandaloneExpression(Expression expression, @Nullable List<Expression> parameters) {
        return formatStandaloneExpression(expression, parameters, DEFAULT_FORMATTER);
    }

    public static String formatStandaloneExpression(Expression expression) {
        return formatStandaloneExpression(expression, null, DEFAULT_FORMATTER);
    }

    public static <T extends Formatter> String formatStandaloneExpression(Expression expression,
                                                                          @Nullable List<Expression> parameters,
                                                                          T formatter) {
        String formattedExpression = formatter.process(expression, parameters);
        if (formattedExpression.startsWith("(") && formattedExpression.endsWith(")")) {
            return formattedExpression.substring(1, formattedExpression.length() - 1);
        } else {
            return formattedExpression;
        }
    }

    public static String formatExpression(Expression expression) {
        return expression.accept(DEFAULT_FORMATTER, null);
    }

    public static class Formatter extends AstVisitor<String, List<Expression>> {

        @Override
        protected String visitNode(Node node, @Nullable List<Expression> parameters) {
            throw new UnsupportedOperationException(String.format(Locale.ENGLISH, "cannot handle node '%s'", node.toString()));
        }

        @Override
        protected String visitExpression(Expression node, @Nullable List<Expression> parameters) {
            throw new UnsupportedOperationException(String.format(Locale.ENGLISH,
                "not yet implemented: %s.visit%s", getClass().getName(), node.getClass().getSimpleName()));
        }

        @Override
        public String visitIntervalLiteral(IntervalLiteral node, List<Expression> context) {
            StringBuilder builder = new StringBuilder("INTERVAL ")
                .append(node.getValue())
                .append(" ")
                .append(node.getStartField().name());
            node.getEndField().map(end -> builder.append(" TO " + end.name()));
            return builder.toString();
        }

        @Override
        public String visitArrayComparisonExpression(ArrayComparisonExpression node, @Nullable List<Expression> parameters) {
            StringBuilder builder = new StringBuilder();

            String array = node.getRight().toString();
            String left = node.getLeft().toString();
            String type = node.getType().getValue();

            builder.append(left + " " + type + " ANY(" + array + ")");
            return builder.toString();
        }

        @Override
        protected String visitArraySubQueryExpression(ArraySubQueryExpression node, @Nullable List<Expression> parameters) {
            StringBuilder builder = new StringBuilder();
            String subqueryExpression = node.subqueryExpression().toString();

            return builder.append("ARRAY(").append(subqueryExpression).append(")").toString();
        }

        @Override
        protected String visitCurrentTime(CurrentTime node, @Nullable List<Expression> parameters) {
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
        protected String visitExtract(Extract node, @Nullable List<Expression> parameters) {
            return "EXTRACT(" + node.getField() + " FROM " + process(node.getExpression(), parameters) + ")";
        }

        @Override
        protected String visitBooleanLiteral(BooleanLiteral node, @Nullable List<Expression> parameters) {
            return String.valueOf(node.getValue());
        }

        @Override
        protected String visitSubscriptExpression(SubscriptExpression node, @Nullable List<Expression> parameters) {
            return node.name() + "[" + node.index() + "]";
        }

        @Override
        public String visitParameterExpression(ParameterExpression node, @Nullable List<Expression> parameters) {
            if (parameters == null) {
                return "$" + node.position();
            } else {
                int index = node.index();
                if (index >= parameters.size()) {
                    throw new IllegalArgumentException(
                        "Invalid parameter number " + node.position() +
                        ". Only " + parameters.size() + " parameters are available");
                }
                return parameters.get(index).accept(this, parameters);
            }
        }

        @Override
        protected String visitStringLiteral(StringLiteral node, @Nullable List<Expression> parameters) {
            return Literals.quoteStringLiteral(node.getValue());
        }

        @Override
        protected String visitEscapedCharStringLiteral(EscapedCharStringLiteral node, @Nullable List<Expression> parameters) {
            return Literals.quoteEscapedStringLiteral(node.getRawValue());
        }

        @Override
        protected String visitLongLiteral(LongLiteral node, @Nullable List<Expression> parameters) {
            return Long.toString(node.getValue());
        }

        @Override
        protected String visitDoubleLiteral(DoubleLiteral node, @Nullable List<Expression> parameters) {
            return Double.toString(node.getValue());
        }

        @Override
        protected String visitNullLiteral(NullLiteral node, @Nullable List<Expression> parameters) {
            return "NULL";
        }

        @Override
        public String visitArrayLiteral(ArrayLiteral node, @Nullable List<Expression> parameters) {
            StringBuilder builder = new StringBuilder("[");
            boolean first = true;
            for (Expression element : node.values()) {
                if (!first) {
                    builder.append(", ");
                } else {
                    first = false;
                }
                builder.append(element.accept(this, parameters));

            }
            return builder.append("]").toString();
        }

        @Override
        public String visitObjectLiteral(ObjectLiteral node, @Nullable List<Expression> parameters) {
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
                    .append(entry.getValue().accept(this, parameters));

            }
            return builder.append("}").toString();
        }

        @Override
        protected String visitSubqueryExpression(SubqueryExpression node, @Nullable List<Expression> parameters) {
            return "(" + formatSql(node.getQuery()) + ")";
        }

        @Override
        protected String visitExists(ExistsPredicate node, @Nullable List<Expression> parameters) {
            return "EXISTS (" + formatSql(node.getSubquery()) + ")";
        }

        @Override
        protected String visitQualifiedNameReference(QualifiedNameReference node, @Nullable List<Expression> parameters) {
            return node.getName().getParts().stream()
                .map(Formatter::formatIdentifier)
                .collect(Collectors.joining("."));
        }

        @Override
        protected String visitFunctionCall(FunctionCall node, @Nullable List<Expression> parameters) {
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

            if (node.getWindow().isPresent()) {
                builder.append(" OVER ").append(visitWindow(node.getWindow().get(), parameters));
            }
            return builder.toString();
        }

        @Override
        public String visitWindow(Window node, @Nullable List<Expression> parameters) {
            StringBuilder sb = new StringBuilder("(");
            if (node.windowRef() != null) {
                sb.append(node.windowRef()).append(" ");
            }
            if (!node.getPartitions().isEmpty()) {
                sb.append("PARTITION BY ");
                sb.append(joinExpressions(node.getPartitions()));
                sb.append(" ");
            }
            if (!node.getOrderBy().isEmpty()) {
                sb.append(formatOrderBy(node.getOrderBy(), parameters));
            }
            if (node.getWindowFrame().isPresent()) {
                sb.append(process(node.getWindowFrame().get(), parameters));
            }
            if (Character.isWhitespace(sb.charAt(sb.length() - 1))) {
                sb.setLength(sb.length() - 1);
            }
            sb.append(')');
            return sb.toString();
        }

        private static String formatOrderBy(List<SortItem> orderBy, @Nullable List<Expression> parameters) {
            return "ORDER BY " + orderBy.stream()
                .map(e -> SqlFormatter.formatSortItem(e, parameters))
                .collect(Collectors.joining(", "));
        }

        @Override
        public String visitWindowFrame(WindowFrame node, @Nullable List<Expression> parameters) {
            StringBuilder builder = new StringBuilder(" ");

            builder.append(node.getType().toString()).append(' ');

            if (node.getEnd().isPresent()) {
                builder.append("BETWEEN ")
                    .append(process(node.getStart(), parameters))
                    .append(" AND ")
                    .append(process(node.getEnd().get(), parameters));
            } else {
                builder.append(process(node.getStart(), parameters));
            }
            return builder.toString();
        }

        @Override
        public String visitFrameBound(FrameBound node, @Nullable List<Expression> parameters) {
            switch (node.getType()) {
                case UNBOUNDED_PRECEDING:
                    return "UNBOUNDED PRECEDING";
                case PRECEDING:
                    return process(node.getValue(), parameters) + " PRECEDING";
                case CURRENT_ROW:
                    return "CURRENT ROW";
                case FOLLOWING:
                    return process(node.getValue(), parameters) + " FOLLOWING";
                case UNBOUNDED_FOLLOWING:
                    return "UNBOUNDED FOLLOWING";
                default:
                    throw new IllegalArgumentException("unhandled type: " + node.getType());
            }
        }

        @Override
        protected String visitLogicalBinaryExpression(LogicalBinaryExpression node, @Nullable List<Expression> parameters) {
            return formatBinaryExpression(
                node.getType().toString(),
                node.getLeft(),
                node.getRight(),
                parameters
            );
        }

        @Override
        protected String visitNotExpression(NotExpression node, @Nullable List<Expression> parameters) {
            return "(NOT " + process(node.getValue(), parameters) + ")";
        }

        @Override
        protected String visitComparisonExpression(ComparisonExpression node, @Nullable List<Expression> parameters) {
            return formatBinaryExpression(
                node.getType().getValue(),
                node.getLeft(),
                node.getRight(),
                parameters
            );
        }

        @Override
        protected String visitIsNullPredicate(IsNullPredicate node, @Nullable List<Expression> parameters) {
            return "(" + process(node.getValue(), parameters) + " IS NULL)";
        }

        @Override
        protected String visitIsNotNullPredicate(IsNotNullPredicate node, @Nullable List<Expression> parameters) {
            return "(" + process(node.getValue(), parameters) + " IS NOT NULL)";
        }

        @Override
        protected String visitIfExpression(IfExpression node, @Nullable List<Expression> parameters) {
            StringBuilder builder = new StringBuilder();
            builder.append("IF(")
                .append(process(node.getCondition(), parameters))
                .append(", ")
                .append(process(node.getTrueValue(), parameters));
            if (node.getFalseValue().isPresent()) {
                builder.append(", ")
                    .append(process(node.getFalseValue().get(), parameters));
            }
            builder.append(")");
            return builder.toString();
        }

        @Override
        protected String visitNegativeExpression(NegativeExpression node, @Nullable List<Expression> parameters) {
            return "- " + process(node.getValue(), parameters);
        }

        @Override
        protected String visitArithmeticExpression(ArithmeticExpression node, @Nullable List<Expression> parameters) {
            return formatBinaryExpression(
                node.getType().getValue(),
                node.getLeft(),
                node.getRight(),
                parameters);
        }

        @Override
        protected String visitLikePredicate(LikePredicate node, @Nullable List<Expression> parameters) {
            StringBuilder builder = new StringBuilder();

            builder.append('(')
                .append(process(node.getValue(), parameters))
                .append(" LIKE ")
                .append(process(node.getPattern(), parameters));

            if (node.getEscape() != null) {
                builder.append(" ESCAPE ")
                    .append(process(node.getEscape(), parameters));
            }

            builder.append(')');

            return builder.toString();
        }

        @Override
        public String visitArrayLikePredicate(ArrayLikePredicate node, @Nullable List<Expression> parameters) {
            StringBuilder builder = new StringBuilder();
            builder.append('(')
                .append(process(node.getPattern(), parameters))
                .append(node.inverse() ? " NOT" : "")
                .append(" LIKE ")
                .append(node.quantifier().name())
                .append(" (")
                .append(process(node.getValue(), parameters))
                .append(") ");
            if (node.getEscape() != null) {
                builder.append("ESCAPE ")
                    .append(process(node.getEscape(), parameters));
            }
            builder.append(')');
            return builder.toString();
        }

        @Override
        public String visitMatchPredicate(MatchPredicate node, @Nullable List<Expression> parameters) {
            StringBuilder builder = new StringBuilder();
            builder.append("MATCH (");
            if (node.idents().size() == 1) {
                builder.append(process(node.idents().get(0).columnIdent(), parameters));
            } else {
                builder.append("(");
                List<MatchPredicateColumnIdent> idents = node.idents();
                for (int i = 0, identsSize = idents.size(); i < identsSize; i++) {
                    MatchPredicateColumnIdent ident = idents.get(i);
                    builder.append(ident.accept(this, parameters));
                    if (i < (identsSize - 1)) {
                        builder.append(", ");
                    }
                }
                builder.append(")");
            }
            builder.append(", ").append(process(node.value(), parameters));
            builder.append(")");
            if (node.matchType() != null) {
                builder.append(" USING ").append(node.matchType()).append(" ");
                if (node.properties().properties().size() > 0) {
                    builder.append(process(node.properties(), parameters));
                }
            }
            return builder.toString();
        }

        @Override
        public String visitMatchPredicateColumnIdent(MatchPredicateColumnIdent node, @Nullable List<Expression> parameters) {
            String column = process(node.columnIdent(), null);
            if (!(node.boost() instanceof NullLiteral)) {
                column = column + " " + node.boost().toString();
            }
            return column;
        }

        @Override
        public String visitGenericProperties(GenericProperties node, @Nullable List<Expression> parameters) {
            return " WITH (" +
                node.properties().entrySet().stream()
                    .map(prop -> prop.getKey() + "=" + process(prop.getValue(), null))
                    .collect(COMMA_JOINER) +
                ")";
        }

        @Override
        protected String visitAllColumns(AllColumns node, @Nullable List<Expression> parameters) {
            if (node.getPrefix().isPresent()) {
                return node.getPrefix().get() + ".*";
            }

            return "*";
        }

        @Override
        public String visitCast(Cast node, @Nullable List<Expression> parameters) {
            return "CAST(" + process(node.getExpression(), parameters) + " AS " + process(node.getType(), parameters) + ")";
        }

        @Override
        protected String visitTryCast(TryCast node, @Nullable List<Expression> parameters) {
            return "TRY_CAST(" + process(node.getExpression(), parameters) + " AS " + process(node.getType(), parameters) +
                ")";
        }

        @Override
        protected String visitSearchedCaseExpression(SearchedCaseExpression node, @Nullable List<Expression> parameters) {
            ImmutableList.Builder<String> parts = ImmutableList.builder();
            parts.add("CASE");
            for (WhenClause whenClause : node.getWhenClauses()) {
                parts.add(process(whenClause, parameters));
            }
            if (node.getDefaultValue() != null) {
                parts.add("ELSE")
                    .add(process(node.getDefaultValue(), parameters));
            }
            parts.add("END");

            return "(" + String.join(" ", parts.build()) + ")";
        }

        @Override
        protected String visitSimpleCaseExpression(SimpleCaseExpression node, @Nullable List<Expression> parameters) {
            ImmutableList.Builder<String> parts = ImmutableList.builder();
            parts.add("CASE")
                .add(process(node.getOperand(), parameters));
            for (WhenClause whenClause : node.getWhenClauses()) {
                parts.add(process(whenClause, parameters));
            }
            if (node.getDefaultValue() != null) {
                parts.add("ELSE")
                    .add(process(node.getDefaultValue(), parameters));
            }
            parts.add("END");

            return "(" + String.join(" ", parts.build()) + ")";
        }

        @Override
        protected String visitWhenClause(WhenClause node, @Nullable List<Expression> parameters) {
            return "WHEN " + process(node.getOperand(), parameters) + " THEN " + process(node.getResult(), parameters);
        }

        @Override
        protected String visitBetweenPredicate(BetweenPredicate node, @Nullable List<Expression> parameters) {
            return "(" + process(node.getValue(), parameters) + " BETWEEN " +
                process(node.getMin(), parameters) + " AND " + process(node.getMax(), parameters) + ")";
        }

        @Override
        protected String visitInPredicate(InPredicate node, @Nullable List<Expression> parameters) {
            return "(" + process(node.getValue(), parameters) + " IN " + process(node.getValueList(), parameters) + ")";
        }

        @Override
        protected String visitInListExpression(InListExpression node, @Nullable List<Expression> parameters) {
            return "(" + joinExpressions(node.getValues()) + ")";
        }

        @Override
        public String visitColumnType(ColumnType node, @Nullable List<Expression> parameters) {
            return node.name();
        }

        @Override
        public String visitCollectionColumnType(CollectionColumnType node, @Nullable List<Expression> parameters) {
            return node.name() + "(" + process(node.innerType(), parameters) + ")";
        }

        @Override
        public String visitObjectColumnType(ObjectColumnType node, @Nullable List<Expression> parameters) {
            return node.name();
        }

        private String formatBinaryExpression(String operator,
                                              Expression left,
                                              Expression right,
                                              @Nullable List<Expression> parameters) {
            return '(' + process(left, parameters) + ' ' + operator + ' ' + process(right, parameters) + ')';
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
