/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package io.crate.sql.parser;

import com.google.common.collect.ImmutableList;
import io.crate.sql.parser.antlr.v4.SqlBaseBaseVisitor;
import io.crate.sql.parser.antlr.v4.SqlBaseLexer;
import io.crate.sql.parser.antlr.v4.SqlBaseParser;
import io.crate.sql.tree.*;
import org.antlr.v4.runtime.ParserRuleContext;
import org.antlr.v4.runtime.Token;
import org.antlr.v4.runtime.tree.ParseTree;
import org.antlr.v4.runtime.tree.TerminalNode;

import java.util.Arrays;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;

import static java.util.Objects.requireNonNull;
import static java.util.stream.Collectors.toList;

class AstBuilder extends SqlBaseBaseVisitor<Node> {
    private int parameterPosition = 1;

    @Override
    public Node visitSingleStatement(SqlBaseParser.SingleStatementContext context) {
        return visit(context.statement());
    }

    @Override
    public Node visitSingleExpression(SqlBaseParser.SingleExpressionContext context) {
        return visit(context.expression());
    }

//    // ******************* statements **********************
//
//    @Override
//    public Node visitCreateTableAsSelect(SqlBaseParser.CreateTableAsSelectContext context) {
//        return new CreateTableAsSelect(getLocation(context), getQualifiedName(context.qualifiedName()), (Query) visit(context.query()), context.EXISTS() != null, processTableProperties(context.tableProperties()), context.NO() == null);
//    }
//
//    @Override
//    public Node visitCreateTable(SqlBaseParser.CreateTableContext context) {
//        return new CreateTable(getLocation(context),
//            getQualifiedName(context.qualifiedName()),
//            visit(context.tableElement(), TableElement.class),
//            context.EXISTS() != null,
//            processTableProperties(context.tableProperties()));
//    }

//    private Map<String, Expression> processTableProperties(SqlBaseParser.TablePropertiesContext tablePropertiesContext) {
//        ImmutableMap.Builder<String, Expression> properties = ImmutableMap.builder();
//        if (tablePropertiesContext != null) {
//            for (SqlBaseParser.TablePropertyContext tablePropertyContext : tablePropertiesContext.tableProperty()) {
//                properties.put(tablePropertyContext.identifier().getText(), (Expression) visit(tablePropertyContext.expression()));
//            }
//        }
//        return properties.build();
//    }

    @Override
    public Node visitShowCreateTable(SqlBaseParser.ShowCreateTableContext context) {
        return new ShowCreateTable(new Table(getQualifiedName(context.qualifiedName())));
    }

    @Override
    public Node visitDropTable(SqlBaseParser.DropTableContext context) {
        return new DropTable(new Table(getQualifiedName(context.qualifiedName())), context.EXISTS() != null);
    }
//
//    @Override
//    public Node visitInsertInto(SqlBaseParser.InsertIntoContext context) {
//        return new Insert(
//            getQualifiedName(context.qualifiedName()),
//            Optional.ofNullable(getColumnAliases(context.columnAliases())),
//            (Query) visit(context.query()));
//    }

    @Override
    public Node visitDelete(SqlBaseParser.DeleteContext context) {
        return new Delete(
            (Relation) visit(context.aliasedRelation()),
            visitIfPresent(context.booleanExpression(), Expression.class)
        );
    }

    @Override
    public Node visitUpdate(SqlBaseParser.UpdateContext context) {
        return new Update(
            (Relation) visit(context.aliasedRelation()),
            visit(context.assignment(), Assignment.class),
            visitIfPresent(context.where, Expression.class)
        );
    }

    @Override
    public Node visitAssignment(SqlBaseParser.AssignmentContext context) {
        return new Assignment((Expression) visit(context.valueExpression()), (Expression) visit(context.expression()));
    }

    @Override
    public Node visitSet(SqlBaseParser.SetContext context) {
        if (context.LOCAL() != null) {
            return new SetStatement(SetStatement.Scope.LOCAL, (Assignment) visit(context.setAssignment()));
        } else {
            return new SetStatement(SetStatement.Scope.SESSION, (Assignment) visit(context.setAssignment()));
        }
    }

    @Override
    public Node visitSetAssignment(SqlBaseParser.SetAssignmentContext context) {
        QualifiedNameReference name = new QualifiedNameReference(getQualifiedName(context.name));
        if (context.setSettingValue().DEFAULT() != null) {
            return new Assignment(name, ImmutableList.of());
        }
        return new Assignment(name, visit(context.setSettingValue().setExpression(), Expression.class));
    }

    @Override
    public Node visitSetExpression(SqlBaseParser.SetExpressionContext context) {
        if (context.ON() != null) {
            return BooleanLiteral.TRUE_LITERAL;
        } else if (context.identifier() != null) {
            return visit(context.identifier());
        } else {
            return visit(context.simpleLiteral());
        }
    }

    @Override
    public Node visitKillStatement(SqlBaseParser.KillStatementContext context) {
        if (context.ALL() != null) {
            return new KillStatement();
        }

        return new KillStatement((Expression) visit(context.parameterOrLiteral()));
    }

//    @Override
//    public Node visitRenameTable(SqlBaseParser.RenameTableContext context) {
//        return new RenameTable(getLocation(context), getQualifiedName(context.from), getQualifiedName(context.to));
//    }
//
//    @Override
//    public Node visitRenameColumn(SqlBaseParser.RenameColumnContext context) {
//        return new RenameColumn(getLocation(context), getQualifiedName(context.tableName), context.from.getText(), context.to.getText());
//    }
//
//    @Override
//    public Node visitAddColumn(SqlBaseParser.AddColumnContext context) {
//        return new AddColumn(getLocation(context), getQualifiedName(context.qualifiedName()), (ColumnDefinition) visit(context.columnDefinition()));
//    }
//
//
//    // ********************** query expressions ********************
//
//    @Override
//    public Node visitQuery(SqlBaseParser.QueryContext context) {
//        Query body = (Query) visit(context.queryNoWith());
//
//        return new Query(
//            getLocation(context),
//            visitIfPresent(context.with(), With.class),
//            body.getQueryBody(),
//            body.getOrderBy(),
//            body.getLimit());
//    }
//
//    @Override
//    public Node visitWith(SqlBaseParser.WithContext context) {
//        return new With(getLocation(context), context.RECURSIVE() != null, visit(context.namedQuery(), WithQuery.class));
//    }
//
//    @Override
//    public Node visitNamedQuery(SqlBaseParser.NamedQueryContext context) {
//        return new WithQuery(getLocation(context), context.name.getText(), (Query) visit(context.query()), Optional.ofNullable(getColumnAliases(context.columnAliases())));
//    }
//
//    @Override
//    public Node visitQueryNoWith(SqlBaseParser.QueryNoWithContext context) {
//        QueryBody term = (QueryBody) visit(context.queryTerm());
//
//        if (term instanceof QuerySpecification) {
//            // When we have a simple query specification
//            // followed by order by limit, fold the order by and limit
//            // clauses into the query specification (analyzer/planner
//            // expects this structure to resolve references with respect
//            // to columns defined in the query specification)
//            QuerySpecification query = (QuerySpecification) term;
//
//            return new Query(
//                getLocation(context),
//                Optional.<With>empty(),
//                new QuerySpecification(
//                    getLocation(context),
//                    query.getSelect(),
//                    query.getFrom(),
//                    query.getWhere(),
//                    query.getGroupBy(),
//                    query.getHaving(),
//                    visit(context.sortItem(), SortItem.class),
//                    getTextIfPresent(context.limit)),
//                ImmutableList.of(),
//                Optional.<String>empty());
//        }
//
//        return new Query(
//            getLocation(context),
//            Optional.<With>empty(),
//            term,
//            visit(context.sortItem(), SortItem.class),
//            getTextIfPresent(context.limit));
//    }
//
//    @Override
//    public Node visitQuerySpecification(SqlBaseParser.QuerySpecificationContext context) {
//        Optional<Relation> from = Optional.empty();
//        List<SelectItem> selectItems = visit(context.selectItem(), SelectItem.class);
//
//        List<Relation> relations = visit(context.relation(), Relation.class);
//        if (!relations.isEmpty()) {
//            // synthesize implicit join nodes
//            Iterator<Relation> iterator = relations.iterator();
//            Relation relation = iterator.next();
//
//            while (iterator.hasNext()) {
//                relation = new Join(getLocation(context), Join.Type.IMPLICIT, relation, iterator.next(), Optional.<JoinCriteria>empty());
//            }
//
//            from = Optional.of(relation);
//        }
//
//        return new QuerySpecification(
//            getLocation(context),
//            new Select(getLocation(context.SELECT()), isDistinct(context.setQuantifier()), selectItems),
//            from,
//            visitIfPresent(context.where, Expression.class),
//            visitIfPresent(context.groupBy(), GroupBy.class),
//            visitIfPresent(context.having, Expression.class),
//            ImmutableList.of(),
//            Optional.empty());
//    }
//
//    @Override
//    public Node visitGroupBy(SqlBaseParser.GroupByContext context) {
//        return new GroupBy(getLocation(context), isDistinct(context.setQuantifier()), visit(context.groupingElement(), GroupingElement.class));
//    }
//
//    @Override
//    public Node visitSingleGroupingSet(SqlBaseParser.SingleGroupingSetContext context) {
//        return new SimpleGroupBy(getLocation(context), visit(context.groupingExpressions().expression(), Expression.class));
//    }
//
//    @Override
//    public Node visitRollup(SqlBaseParser.RollupContext context) {
//        return new Rollup(getLocation(context), context.qualifiedName().stream()
//            .map(AstBuilder::getQualifiedName)
//            .collect(toList()));
//    }
//
//    @Override
//    public Node visitCube(SqlBaseParser.CubeContext context) {
//        return new Cube(getLocation(context), context.qualifiedName().stream()
//            .map(AstBuilder::getQualifiedName)
//            .collect(toList()));
//    }
//
//    @Override
//    public Node visitMultipleGroupingSets(SqlBaseParser.MultipleGroupingSetsContext context) {
//        return new GroupingSets(getLocation(context), context.groupingSet().stream()
//            .map(groupingSet -> groupingSet.qualifiedName().stream()
//                .map(AstBuilder::getQualifiedName)
//                .collect(toList()))
//            .collect(toList()));
//    }
//
//    @Override
//    public Node visitSetOperation(SqlBaseParser.SetOperationContext context) {
//        QueryBody left = (QueryBody) visit(context.left);
//        QueryBody right = (QueryBody) visit(context.right);
//
//        boolean distinct = context.setQuantifier() == null || context.setQuantifier().DISTINCT() != null;
//
//        switch (context.operator.getType()) {
//            case SqlBaseLexer.UNION:
//                return new Union(getLocation(context.UNION()), ImmutableList.of(left, right), distinct);
//            case SqlBaseLexer.INTERSECT:
//                return new Intersect(getLocation(context.INTERSECT()), ImmutableList.of(left, right), distinct);
//            case SqlBaseLexer.EXCEPT:
//                return new Except(getLocation(context.EXCEPT()), left, right, distinct);
//        }
//
//        throw new IllegalArgumentException("Unsupported set operation: " + context.operator.getText());
//    }
//
//    @Override
//    public Node visitSelectAll(SqlBaseParser.SelectAllContext context) {
//        if (context.qualifiedName() != null) {
//            return new AllColumns(getLocation(context), getQualifiedName(context.qualifiedName()));
//        }
//
//        return new AllColumns(getLocation(context));
//    }
//
//    @Override
//    public Node visitSelectSingle(SqlBaseParser.SelectSingleContext context) {
//        Optional<String> alias = getTextIfPresent(context.identifier());
//
//        return new SingleColumn(getLocation(context), (Expression) visit(context.expression()), alias);
//    }
//
//    @Override
//    public Node visitTable(SqlBaseParser.TableContext context) {
//        return new Table(getLocation(context), getQualifiedName(context.qualifiedName()));
//    }
//
//    @Override
//    public Node visitSubquery(SqlBaseParser.SubqueryContext context) {
//        return new TableSubquery(getLocation(context), (Query) visit(context.queryNoWith()));
//    }
//
//    @Override
//    public Node visitInlineTable(SqlBaseParser.InlineTableContext context) {
//        return new Values(getLocation(context), visit(context.expression(), Expression.class));
//    }
//
    @Override
    public Node visitExplain(SqlBaseParser.ExplainContext context) {
        return new Explain((Statement) visit(context.statement()), visit(context.explainOption(), ExplainOption.class));
    }

    @Override
    public Node visitExplainFormat(SqlBaseParser.ExplainFormatContext context) {
        switch (context.value.getType()) {
            case SqlBaseLexer.GRAPHVIZ:
                return new ExplainFormat(ExplainFormat.Type.GRAPHVIZ);
            case SqlBaseLexer.TEXT:
                return new ExplainFormat(ExplainFormat.Type.TEXT);
        }

        throw new IllegalArgumentException("Unsupported EXPLAIN format: " + context.value.getText());
    }

    @Override
    public Node visitExplainType(SqlBaseParser.ExplainTypeContext context) {
        switch (context.value.getType()) {
            case SqlBaseLexer.LOGICAL:
                return new ExplainType(ExplainType.Type.LOGICAL);
            case SqlBaseLexer.DISTRIBUTED:
                return new ExplainType(ExplainType.Type.DISTRIBUTED);
        }

        throw new IllegalArgumentException("Unsupported EXPLAIN type: " + context.value.getText());
    }

    @Override
    public Node visitShowTables(SqlBaseParser.ShowTablesContext context) {
        return new ShowTables(
            Optional.ofNullable(context.qualifiedName()).map(AstBuilder::getQualifiedName),
            getTextIfPresent(context.pattern).map(AstBuilder::unquote),
            visitIfPresent(context.booleanExpression(), Expression.class)
        );
    }

    @Override
    public Node visitShowSchemas(SqlBaseParser.ShowSchemasContext context) {
        return new ShowSchemas(
            getTextIfPresent(context.pattern).map(AstBuilder::unquote),
            visitIfPresent(context.booleanExpression(), Expression.class)
        );
    }

    @Override
    public Node visitShowColumns(SqlBaseParser.ShowColumnsContext context) {
        return new ShowColumns(
            context.qualifiedName().stream().map(AstBuilder::getQualifiedName).collect(toList()),
            getTextIfPresent(context.pattern).map(AstBuilder::unquote),
            visitIfPresent(context.booleanExpression(), Expression.class)
        );
    }

//    @Override
//    public Node visitSetSession(SqlBaseParser.SetSessionContext context) {
//        return new SetSession(getLocation(context), getQualifiedName(context.qualifiedName()), (Expression) visit(context.expression()));
//    }
//
//    @Override
//    public Node visitResetSession(SqlBaseParser.ResetSessionContext context) {
//        return new ResetSession(getLocation(context), getQualifiedName(context.qualifiedName()));
//    }
//
//    // ***************** boolean expressions ******************

    @Override
    public Node visitLogicalNot(SqlBaseParser.LogicalNotContext context) {
        return new NotExpression((Expression) visit(context.booleanExpression()));
    }

    @Override
    public Node visitLogicalBinary(SqlBaseParser.LogicalBinaryContext context) {
        return new LogicalBinaryExpression(
            getLogicalBinaryOperator(context.operator),
            (Expression) visit(context.left),
            (Expression) visit(context.right));
    }

//    // *************** from clause *****************
//
//    @Override
//    public Node visitJoinRelation(SqlBaseParser.JoinRelationContext context) {
//        Relation left = (Relation) visit(context.left);
//        Relation right;
//
//        if (context.CROSS() != null) {
//            right = (Relation) visit(context.right);
//            return new Join(getLocation(context), Join.Type.CROSS, left, right, Optional.<JoinCriteria>empty());
//        }
//
//        JoinCriteria criteria;
//        if (context.NATURAL() != null) {
//            right = (Relation) visit(context.right);
//            criteria = new NaturalJoin();
//        } else {
//            right = (Relation) visit(context.rightRelation);
//            if (context.joinCriteria().ON() != null) {
//                criteria = new JoinOn((Expression) visit(context.joinCriteria().booleanExpression()));
//            } else if (context.joinCriteria().USING() != null) {
//                List<String> columns = context.joinCriteria()
//                    .identifier().stream()
//                    .map(ParseTree::getText)
//                    .collect(toList());
//
//                criteria = new JoinUsing(columns);
//            } else {
//                throw new IllegalArgumentException("Unsupported join criteria");
//            }
//        }
//
//        Join.Type joinType;
//        if (context.joinType().LEFT() != null) {
//            joinType = Join.Type.LEFT;
//        } else if (context.joinType().RIGHT() != null) {
//            joinType = Join.Type.RIGHT;
//        } else if (context.joinType().FULL() != null) {
//            joinType = Join.Type.FULL;
//        } else {
//            joinType = Join.Type.INNER;
//        }
//
//        return new Join(getLocation(context), joinType, left, right, Optional.of(criteria));
//    }

    @Override
    public Node visitSampledRelation(SqlBaseParser.SampledRelationContext context) {
        Relation child = (Relation) visit(context.aliasedRelation());

        if (context.TABLESAMPLE() == null) {
            return child;
        }

        return new SampledRelation(
            child,
            getSamplingMethod((Token) context.sampleType().getChild(0).getPayload()),
            (Expression) visit(context.percentage),
            com.google.common.base.Optional.absent() // TODO fix that
        );
    }

    @Override
    public Node visitAliasedRelation(SqlBaseParser.AliasedRelationContext context) {
        Relation child = (Relation) visit(context.relationPrimary());

        if (context.identifier() == null) {
            return child;
        }

        return new AliasedRelation(child, context.identifier().getText(), getColumnAliases(context.columnAliases()));
    }

    @Override
    public Node visitTableName(SqlBaseParser.TableNameContext context) {
        return new Table(getQualifiedName(context.qualifiedName()));
    }

//    @Override
//    public Node visitSubqueryRelation(SqlBaseParser.SubqueryRelationContext context) {
//        return new TableSubquery(getLocation(context), (Query) visit(context.query()));
//    }
//
//    @Override
//    public Node visitUnnest(SqlBaseParser.UnnestContext context) {
//        return new Unnest(getLocation(context), visit(context.expression(), Expression.class), context.ORDINALITY() != null);
//    }
//
//    @Override
//    public Node visitParenthesizedRelation(SqlBaseParser.ParenthesizedRelationContext context) {
//        return visit(context.relation());
//    }
//
    // ********************* predicates *******************

    @Override
    public Node visitPredicated(SqlBaseParser.PredicatedContext context) {
        if (context.predicate() != null) {
            return visit(context.predicate());
        }

        return visit(context.valueExpression);
    }

    @Override
    public Node visitComparison(SqlBaseParser.ComparisonContext context) {
        return new ComparisonExpression(
            getComparisonOperator(((TerminalNode) context.comparisonOperator().getChild(0)).getSymbol()),
            (Expression) visit(context.value),
            (Expression) visit(context.right));
    }

//    @Override
//    public Node visitDistinctFrom(SqlBaseParser.DistinctFromContext context) {
//        Expression expression = new ComparisonExpression(
//            getLocation(context),
//            ComparisonExpressionType.IS_DISTINCT_FROM,
//            (Expression) visit(context.value),
//            (Expression) visit(context.right));
//
//        if (context.NOT() != null) {
//            expression = new NotExpression(getLocation(context), expression);
//        }
//
//        return expression;
//    }
//
//    @Override
//    public Node visitBetween(SqlBaseParser.BetweenContext context) {
//        Expression expression = new BetweenPredicate(
//            getLocation(context),
//            (Expression) visit(context.value),
//            (Expression) visit(context.lower),
//            (Expression) visit(context.upper));
//
//        if (context.NOT() != null) {
//            expression = new NotExpression(getLocation(context), expression);
//        }
//
//        return expression;
//    }

    @Override
    public Node visitNullPredicate(SqlBaseParser.NullPredicateContext context) {
        Expression child = (Expression) visit(context.value);

        if (context.NOT() == null) {
            return new IsNullPredicate(child);
        }

        return new IsNotNullPredicate(child);
    }

    @Override
    public Node visitLike(SqlBaseParser.LikeContext context) {
        Expression escape = null;
        if (context.escape != null) {
            escape = (Expression) visit(context.escape);
        }

        Expression result = new LikePredicate(
            (Expression) visit(context.value),
            (Expression) visit(context.pattern),
            escape
        );

        if (context.NOT() != null) {
            result = new NotExpression(result);
        }

        return result;
    }
//
//    @Override
//    public Node visitInList(SqlBaseParser.InListContext context) {
//        Expression result = new InPredicate(
//            getLocation(context),
//            (Expression) visit(context.value),
//            new InListExpression(getLocation(context), visit(context.expression(), Expression.class)));
//
//        if (context.NOT() != null) {
//            result = new NotExpression(getLocation(context), result);
//        }
//
//        return result;
//    }
//
//    @Override
//    public Node visitInSubquery(SqlBaseParser.InSubqueryContext context) {
//        Expression result = new InPredicate(
//            getLocation(context),
//            (Expression) visit(context.value),
//            new SubqueryExpression(getLocation(context), (Query) visit(context.query())));
//
//        if (context.NOT() != null) {
//            result = new NotExpression(getLocation(context), result);
//        }
//
//        return result;
//    }
//
//    @Override
//    public Node visitExists(SqlBaseParser.ExistsContext context) {
//        return new ExistsPredicate(getLocation(context), (Query) visit(context.query()));
//    }
//
//    @Override
//    public Node visitQuantifiedComparison(SqlBaseParser.QuantifiedComparisonContext context) {
//        return new QuantifiedComparisonExpression(
//            getLocation(context.comparisonOperator()),
//            getComparisonOperator(((TerminalNode) context.comparisonOperator().getChild(0)).getSymbol()),
//            getComparisonQuantifier(((TerminalNode) context.comparisonQuantifier().getChild(0)).getSymbol()),
//            (Expression) visit(context.value),
//            new SubqueryExpression(getLocation(context.query()), (Query) visit(context.query())));
//    }
//
//    // ************** value expressions **************
//
//    @Override
//    public Node visitArithmeticUnary(SqlBaseParser.ArithmeticUnaryContext context) {
//        Expression child = (Expression) visit(context.valueExpression());
//
//        switch (context.operator.getType()) {
//            case SqlBaseLexer.MINUS:
//                return ArithmeticUnaryExpression.negative(getLocation(context), child);
//            case SqlBaseLexer.PLUS:
//                return ArithmeticUnaryExpression.positive(getLocation(context), child);
//            default:
//                throw new UnsupportedOperationException("Unsupported sign: " + context.operator.getText());
//        }
//    }
//
//    @Override
//    public Node visitArithmeticBinary(SqlBaseParser.ArithmeticBinaryContext context) {
//        return new ArithmeticBinaryExpression(
//            getLocation(context.operator),
//            getArithmeticBinaryOperator(context.operator),
//            (Expression) visit(context.left),
//            (Expression) visit(context.right));
//    }
//
//    @Override
//    public Node visitConcatenation(SqlBaseParser.ConcatenationContext context) {
//        return new FunctionCall(
//            getLocation(context.CONCAT()),
//            QualifiedName.of("concat"), ImmutableList.of(
//            (Expression) visit(context.left),
//            (Expression) visit(context.right)));
//    }
//
//    @Override
//    public Node visitAtTimeZone(SqlBaseParser.AtTimeZoneContext context) {
//        return new AtTimeZone(
//            getLocation(context.AT()),
//            (Expression) visit(context.valueExpression()),
//            (Expression) visit(context.timeZoneSpecifier()));
//    }
//
//    @Override
//    public Node visitTimeZoneInterval(SqlBaseParser.TimeZoneIntervalContext context) {
//        return visit(context.interval());
//    }
//
//    @Override
//    public Node visitTimeZoneString(SqlBaseParser.TimeZoneStringContext context) {
//        return new StringLiteral(getLocation(context), unquote(context.STRING().getText()));
//    }
//
//    // ********************* primary expressions **********************
//
//    @Override
//    public Node visitImplicitRowConstructor(SqlBaseParser.ImplicitRowConstructorContext context) {
//        List<Expression> items = visit(context.expression(), Expression.class);
//        if (items.size() == 1) {
//            return items.get(0);
//        }
//
//        return new Row(getLocation(context), items);
//    }
//
//    @Override
//    public Node visitRowConstructor(SqlBaseParser.RowConstructorContext context) {
//        return new Row(getLocation(context), visit(context.expression(), Expression.class));
//    }
//
//    @Override
//    public Node visitArrayConstructor(SqlBaseParser.ArrayConstructorContext context) {
//        return new ArrayConstructor(getLocation(context), visit(context.expression(), Expression.class));
//    }
//
//    @Override
//    public Node visitCast(SqlBaseParser.CastContext context) {
//        boolean isTryCast = context.TRY_CAST() != null;
//        return new Cast(getLocation(context), (Expression) visit(context.expression()), getType(context.type()), isTryCast);
//    }
//
//    @Override
//    public Node visitSpecialDateTimeFunction(SqlBaseParser.SpecialDateTimeFunctionContext context) {
//        CurrentTime.Type type = getDateTimeFunctionType(context.name);
//
//        if (context.precision != null) {
//            return new CurrentTime(getLocation(context), type, Integer.parseInt(context.precision.getText()));
//        }
//
//        return new CurrentTime(getLocation(context), type);
//    }
//
//    @Override
//    public Node visitExtract(SqlBaseParser.ExtractContext context) {
//        String fieldString = context.identifier().getText();
//        Extract.Field field;
//        try {
//            field = Extract.Field.valueOf(fieldString.toUpperCase());
//        } catch (IllegalArgumentException e) {
//            throw new ParsingException(format("Invalid EXTRACT field: %s", fieldString), null, context.getStart().getLine(), context.getStart().getCharPositionInLine());
//        }
//        return new Extract(getLocation(context), (Expression) visit(context.valueExpression()), field);
//    }
//
//    @Override
//    public Node visitSubstring(SqlBaseParser.SubstringContext context) {
//        return new FunctionCall(getLocation(context), QualifiedName.of("substr"), visit(context.valueExpression(), Expression.class));
//    }
//
//    @Override
//    public Node visitPosition(SqlBaseParser.PositionContext context) {
//        List<Expression> arguments = Lists.reverse(visit(context.valueExpression(), Expression.class));
//        return new FunctionCall(getLocation(context), QualifiedName.of("strpos"), arguments);
//    }
//
//    @Override
//    public Node visitNormalize(SqlBaseParser.NormalizeContext context) {
//        Expression str = (Expression) visit(context.valueExpression());
//        String normalForm = Optional.ofNullable(context.normalForm()).map(ParserRuleContext::getText).orElse("NFC");
//        return new FunctionCall(getLocation(context), QualifiedName.of("normalize"), ImmutableList.of(str, new StringLiteral(getLocation(context), normalForm)));
//    }

    @Override
    public Node visitSubscript(SqlBaseParser.SubscriptContext context) {
        return new SubscriptExpression((Expression) visit(context.value), (Expression) visit(context.index));
    }

    @Override
    public Node visitSubqueryExpression(SqlBaseParser.SubqueryExpressionContext context) {
        return new SubqueryExpression((Query) visit(context.query()));
    }

    @Override
    public Node visitDereference(SqlBaseParser.DereferenceContext context) {
        List<String> parts = context
            .identifier().stream()
            .map(ParseTree::getText)
            .collect(toList());
        return new QualifiedNameReference(QualifiedName.of(parts));
    }

    @Override
    public Node visitColumnReference(SqlBaseParser.ColumnReferenceContext context) {
        return new QualifiedNameReference(QualifiedName.of(Arrays.asList(context.getText().split("\\."))));
    }

    @Override
    public Node visitQuotedIdentifierAlternative(SqlBaseParser.QuotedIdentifierAlternativeContext context) {
        return new QualifiedNameReference(QualifiedName.of(context.getText()));
    }

    @Override
    public Node visitUnquotedIdentifier(SqlBaseParser.UnquotedIdentifierContext context) {
        return new QualifiedNameReference(QualifiedName.of(context.getText()));
    }

    //    @Override
//    public Node visitSimpleCase(SqlBaseParser.SimpleCaseContext context) {
//        return new SimpleCaseExpression(
//            getLocation(context),
//            (Expression) visit(context.valueExpression()),
//            visit(context.whenClause(), WhenClause.class),
//            visitIfPresent(context.elseExpression, Expression.class));
//    }
//
//    @Override
//    public Node visitSearchedCase(SqlBaseParser.SearchedCaseContext context) {
//        return new SearchedCaseExpression(
//            getLocation(context),
//            visit(context.whenClause(), WhenClause.class),
//            visitIfPresent(context.elseExpression, Expression.class));
//    }
//
//    @Override
//    public Node visitWhenClause(SqlBaseParser.WhenClauseContext context) {
//        return new WhenClause(getLocation(context), (Expression) visit(context.condition), (Expression) visit(context.result));
//    }
//
    // TODO fix
    @Override
    public Node visitFunctionCall(SqlBaseParser.FunctionCallContext context) {
        Optional<Window> window = visitIfPresent(context.over(), Window.class);

        QualifiedName name = getQualifiedName(context.qualifiedName());

        boolean distinct = isDistinct(context.setQuantifier());

        if (name.toString().equalsIgnoreCase("if")) {
            check(context.expression().size() == 2 || context.expression().size() == 3, "Invalid number of arguments for 'if' function", context);
            check(!window.isPresent(), "OVER clause not valid for 'if' function", context);
            check(!distinct, "DISTINCT not valid for 'if' function", context);

            Expression elseExpression = null;
            if (context.expression().size() == 3) {
                elseExpression = (Expression) visit(context.expression(2));
            }

            return new IfExpression(
                (Expression) visit(context.expression(0)),
                (Expression) visit(context.expression(1)),
                elseExpression);
        }
//        if (name.toString().equalsIgnoreCase("nullif")) {
//            check(context.expression().size() == 2, "Invalid number of arguments for 'nullif' function", context);
//            check(!window.isPresent(), "OVER clause not valid for 'nullif' function", context);
//            check(!distinct, "DISTINCT not valid for 'nullif' function", context);
//
//            return new NullIfExpression(
//                getLocation(context),
//                (Expression) visit(context.expression(0)),
//                (Expression) visit(context.expression(1)));
//        }
//        if (name.toString().equalsIgnoreCase("coalesce")) {
//            check(!window.isPresent(), "OVER clause not valid for 'coalesce' function", context);
//            check(!distinct, "DISTINCT not valid for 'coalesce' function", context);
//
//            return new CoalesceExpression(getLocation(context), visit(context.expression(), Expression.class));
//        }
        if (name.toString().equalsIgnoreCase("try")) {
            check(context.expression().size() == 1, "The 'try' function must have exactly one argument", context);
            check(!window.isPresent(), "OVER clause not valid for 'try' function", context);
            check(!distinct, "DISTINCT not valid for 'try' function", context);

//            return new TryExpression(getLocation(context), (Expression) visit(getOnlyElement(context.expression())));
        }

        return new FunctionCall(
            getQualifiedName(context.qualifiedName()),
            window,
            distinct,
            visit(context.expression(), Expression.class));
    }

//    @Override
//    public Node visitLambda(SqlBaseParser.LambdaContext context) {
//        List<LambdaArgumentDeclaration> arguments = context.identifier().stream()
//            .map(SqlBaseParser.IdentifierContext::getText)
//            .map(LambdaArgumentDeclaration::new)
//            .collect(toList());
//
//        Expression body = (Expression) visit(context.expression());
//
//        return new LambdaExpression(arguments, body);
//    }
//
//    @Override
//    public Node visitFilter(SqlBaseParser.FilterContext context) {
//        return visit(context.booleanExpression());
//    }
//
//    @Override
//    public Node visitOver(SqlBaseParser.OverContext context) {
//        return new Window(
//            getLocation(context),
//            visit(context.partition, Expression.class),
//            visit(context.sortItem(), SortItem.class),
//            visitIfPresent(context.windowFrame(), WindowFrame.class));
//    }
//
//    @Override
//    public Node visitColumnDefinition(SqlBaseParser.ColumnDefinitionContext context) {
//        return new ColumnDefinition(context.identifier().getText(), getType(context.type()));
//    }

//    @Override
//    public Node visitLikeClause(SqlBaseParser.LikeClauseContext context) {
//        return new LikeClause(
//            getLocation(context),
//            getQualifiedName(context.qualifiedName()),
//            Optional.ofNullable(context.optionType)
//                .map(AstBuilder::getPropertiesOption));
//    }
//
//    @Override
//    public Node visitSortItem(SqlBaseParser.SortItemContext context) {
//        return new SortItem(
//            getLocation(context),
//            (Expression) visit(context.expression()),
//            Optional.ofNullable(context.ordering)
//                .map(AstBuilder::getOrderingType)
//                .orElse(SortItem.Ordering.ASCENDING),
//            Optional.ofNullable(context.nullOrdering)
//                .map(AstBuilder::getNullOrderingType)
//                .orElse(SortItem.NullOrdering.UNDEFINED));
//    }
//
//    @Override
//    public Node visitWindowFrame(SqlBaseParser.WindowFrameContext context) {
//        return new WindowFrame(
//            getLocation(context),
//            getFrameType(context.frameType),
//            (FrameBound) visit(context.start),
//            visitIfPresent(context.end, FrameBound.class));
//    }
//
//    @Override
//    public Node visitUnboundedFrame(SqlBaseParser.UnboundedFrameContext context) {
//        return new FrameBound(getLocation(context), getUnboundedFrameBoundType(context.boundType));
//    }
//
//    @Override
//    public Node visitBoundedFrame(SqlBaseParser.BoundedFrameContext context) {
//        return new FrameBound(getLocation(context), getBoundedFrameBoundType(context.boundType), (Expression) visit(context.expression()));
//    }
//
//    @Override
//    public Node visitCurrentRowBound(SqlBaseParser.CurrentRowBoundContext context) {
//        return new FrameBound(getLocation(context), FrameBound.Type.CURRENT_ROW);
//    }
//
//    // ************** literals **************

    @Override
    public Node visitNullLiteral(SqlBaseParser.NullLiteralContext context) {
        return NullLiteral.INSTANCE;
    }

    @Override
    public Node visitStringLiteral(SqlBaseParser.StringLiteralContext context) {
        return new StringLiteral(unquote(context.STRING().getText()));
    }

    @Override
    public Node visitBinaryLiteral(SqlBaseParser.BinaryLiteralContext context) {
        String raw = context.BINARY_LITERAL().getText();
        return Literal.fromObject(Boolean.valueOf(unquote(raw.substring(1))));
    }

//    @Override
//    public Node visitTypeConstructor(SqlBaseParser.TypeConstructorContext context) {
//        String value = unquote(context.STRING().getText());
//
//        if (context.DOUBLE_PRECISION() != null) {
//            // TODO: Temporary hack that should be removed with new planner.
//            return new GenericLiteral(getLocation(context), "DOUBLE", value);
//        }
//
//        String type = context.identifier().getText();
//        if (type.equalsIgnoreCase("time")) {
//            return new TimeLiteral(getLocation(context), value);
//        }
//        if (type.equalsIgnoreCase("timestamp")) {
//            return new TimestampLiteral(getLocation(context), value);
//        }
//        if (type.equalsIgnoreCase("decimal")) {
//            return new DecimalLiteral(getLocation(context), value);
//        }
//        if (type.equalsIgnoreCase("char")) {
//            return new CharLiteral(getLocation(context), value);
//        }
//
//        return new GenericLiteral(getLocation(context), type, value);
//    }
//
    @Override
    public Node visitIntegerLiteral(SqlBaseParser.IntegerLiteralContext context) {
        return new LongLiteral(context.getText());
    }

    @Override
    public Node visitDecimalLiteral(SqlBaseParser.DecimalLiteralContext context) {
        return new DoubleLiteral(context.getText());
    }

    @Override
    public Node visitBooleanValue(SqlBaseParser.BooleanValueContext context) {
        return Literal.fromObject(Boolean.valueOf(context.getText()));
    }

    //    @Override
//    public Node visitInterval(SqlBaseParser.IntervalContext context) {
//        return new IntervalLiteral(
//            getLocation(context),
//            unquote(context.STRING().getText()),
//            Optional.ofNullable(context.sign)
//                .map(AstBuilder::getIntervalSign)
//                .orElse(IntervalLiteral.Sign.POSITIVE),
//            getIntervalFieldType((Token) context.from.getChild(0).getPayload()),
//            Optional.ofNullable(context.to)
//                .map((x) -> x.getChild(0).getPayload())
//                .map(Token.class::cast)
//                .map(AstBuilder::getIntervalFieldType));
//    }
//
//    @Override
//    public Node visitParameter(SqlBaseParser.ParameterContext context) {
//        Parameter parameter = new Parameter(getLocation(context), parameterPosition);
//        parameterPosition++;
//        return parameter;
//    }

    @Override
    public Node visitParameterPlaceholder(SqlBaseParser.ParameterPlaceholderContext context) {
        return new ParameterExpression(parameterPosition++);
    }

    @Override
    public Node visitPositionalParameter(SqlBaseParser.PositionalParameterContext context) {
        return new ParameterExpression(Integer.valueOf(context.INTEGER_VALUE().getText()));
    }

    //
//    // ***************** arguments *****************
//
//    @Override
//    public Node visitPositionalArgument(SqlBaseParser.PositionalArgumentContext context) {
//        return new CallArgument(getLocation(context), (Expression) visit(context.expression()));
//    }
//
//    @Override
//    public Node visitNamedArgument(SqlBaseParser.NamedArgumentContext context) {
//        return new CallArgument(getLocation(context), context.identifier().getText(), (Expression) visit(context.expression()));
//    }
//
//    // ***************** helpers *****************

    @Override
    protected Node defaultResult() {
        return null;
    }

    @Override
    protected Node aggregateResult(Node aggregate, Node nextResult) {
        if (nextResult == null) {
            throw new UnsupportedOperationException("not yet implemented");
        }

        if (aggregate == null) {
            return nextResult;
        }

        throw new UnsupportedOperationException("not yet implemented");
    }

    private <T> Optional<T> visitIfPresent(ParserRuleContext context, Class<T> clazz) {
        return Optional.ofNullable(context)
            .map(this::visit)
            .map(clazz::cast);
    }

    private <T> List<T> visit(List<? extends ParserRuleContext> contexts, Class<T> clazz) {
        return contexts.stream()
            .map(this::visit)
            .map(clazz::cast)
            .collect(toList());
    }

    private static String unquote(String value) {
        return value.substring(1, value.length() - 1)
            .replace("''", "'");
    }

//    private static LikeClause.PropertiesOption getPropertiesOption(Token token) {
//        switch (token.getType()) {
//            case SqlBaseLexer.INCLUDING:
//                return LikeClause.PropertiesOption.INCLUDING;
//            case SqlBaseLexer.EXCLUDING:
//                return LikeClause.PropertiesOption.EXCLUDING;
//        }
//        throw new IllegalArgumentException("Unsupported LIKE option type: " + token.getText());
//    }
//
    private static QualifiedName getQualifiedName(SqlBaseParser.QualifiedNameContext context) {
        List<String> parts = context
            .identifier().stream()
            .map(ParseTree::getText)
            .collect(toList());

        return QualifiedName.of(parts);
    }

    private static boolean isDistinct(SqlBaseParser.SetQuantifierContext setQuantifier) {
        return setQuantifier != null && setQuantifier.DISTINCT() != null;
    }

    private static Optional<String> getTextIfPresent(ParserRuleContext context) {
        return Optional.ofNullable(context).map(ParseTree::getText);
    }

    private static Optional<String> getTextIfPresent(Token token) {
        return Optional.ofNullable(token).map(Token::getText);
    }

    private static List<String> getColumnAliases(SqlBaseParser.ColumnAliasesContext columnAliasesContext) {
        if (columnAliasesContext == null) {
            return null;
        }

        return columnAliasesContext
            .identifier().stream()
            .map(ParseTree::getText)
            .collect(toList());
    }

//    private static ArithmeticBinaryExpression.Type getArithmeticBinaryOperator(Token operator) {
//        switch (operator.getType()) {
//            case SqlBaseLexer.PLUS:
//                return ArithmeticBinaryExpression.Type.ADD;
//            case SqlBaseLexer.MINUS:
//                return ArithmeticBinaryExpression.Type.SUBTRACT;
//            case SqlBaseLexer.ASTERISK:
//                return ArithmeticBinaryExpression.Type.MULTIPLY;
//            case SqlBaseLexer.SLASH:
//                return ArithmeticBinaryExpression.Type.DIVIDE;
//            case SqlBaseLexer.PERCENT:
//                return ArithmeticBinaryExpression.Type.MODULUS;
//        }
//
//        throw new UnsupportedOperationException("Unsupported operator: " + operator.getText());
//    }

    private static ComparisonExpression.Type getComparisonOperator(Token symbol) {
        switch (symbol.getType()) {
            case SqlBaseLexer.EQ:
                return ComparisonExpression.Type.EQUAL;
            case SqlBaseLexer.NEQ:
                return ComparisonExpression.Type.NOT_EQUAL;
            case SqlBaseLexer.LT:
                return ComparisonExpression.Type.LESS_THAN;
            case SqlBaseLexer.LTE:
                return ComparisonExpression.Type.LESS_THAN_OR_EQUAL;
            case SqlBaseLexer.GT:
                return ComparisonExpression.Type.GREATER_THAN;
            case SqlBaseLexer.GTE:
                return ComparisonExpression.Type.GREATER_THAN_OR_EQUAL;
        }

        throw new IllegalArgumentException("Unsupported operator: " + symbol.getText());
    }
//
//    private static CurrentTime.Type getDateTimeFunctionType(Token token) {
//        switch (token.getType()) {
//            case SqlBaseLexer.CURRENT_DATE:
//                return CurrentTime.Type.DATE;
//            case SqlBaseLexer.CURRENT_TIME:
//                return CurrentTime.Type.TIME;
//            case SqlBaseLexer.CURRENT_TIMESTAMP:
//                return CurrentTime.Type.TIMESTAMP;
//            case SqlBaseLexer.LOCALTIME:
//                return CurrentTime.Type.LOCALTIME;
//            case SqlBaseLexer.LOCALTIMESTAMP:
//                return CurrentTime.Type.LOCALTIMESTAMP;
//        }
//
//        throw new IllegalArgumentException("Unsupported special function: " + token.getText());
//    }
//
//    private static IntervalLiteral.IntervalField getIntervalFieldType(Token token) {
//        switch (token.getType()) {
//            case SqlBaseLexer.YEAR:
//                return IntervalLiteral.IntervalField.YEAR;
//            case SqlBaseLexer.MONTH:
//                return IntervalLiteral.IntervalField.MONTH;
//            case SqlBaseLexer.DAY:
//                return IntervalLiteral.IntervalField.DAY;
//            case SqlBaseLexer.HOUR:
//                return IntervalLiteral.IntervalField.HOUR;
//            case SqlBaseLexer.MINUTE:
//                return IntervalLiteral.IntervalField.MINUTE;
//            case SqlBaseLexer.SECOND:
//                return IntervalLiteral.IntervalField.SECOND;
//        }
//
//        throw new IllegalArgumentException("Unsupported interval field: " + token.getText());
//    }
//
//    private static IntervalLiteral.Sign getIntervalSign(Token token) {
//        switch (token.getType()) {
//            case SqlBaseLexer.MINUS:
//                return IntervalLiteral.Sign.NEGATIVE;
//            case SqlBaseLexer.PLUS:
//                return IntervalLiteral.Sign.POSITIVE;
//        }
//
//        throw new IllegalArgumentException("Unsupported sign: " + token.getText());
//    }
//
//    private static WindowFrame.Type getFrameType(Token type) {
//        switch (type.getType()) {
//            case SqlBaseLexer.RANGE:
//                return WindowFrame.Type.RANGE;
//            case SqlBaseLexer.ROWS:
//                return WindowFrame.Type.ROWS;
//        }
//
//        throw new IllegalArgumentException("Unsupported frame type: " + type.getText());
//    }
//
//    private static FrameBound.Type getBoundedFrameBoundType(Token token) {
//        switch (token.getType()) {
//            case SqlBaseLexer.PRECEDING:
//                return FrameBound.Type.PRECEDING;
//            case SqlBaseLexer.FOLLOWING:
//                return FrameBound.Type.FOLLOWING;
//        }
//
//        throw new IllegalArgumentException("Unsupported bound type: " + token.getText());
//    }
//
//    private static FrameBound.Type getUnboundedFrameBoundType(Token token) {
//        switch (token.getType()) {
//            case SqlBaseLexer.PRECEDING:
//                return FrameBound.Type.UNBOUNDED_PRECEDING;
//            case SqlBaseLexer.FOLLOWING:
//                return FrameBound.Type.UNBOUNDED_FOLLOWING;
//        }
//
//        throw new IllegalArgumentException("Unsupported bound type: " + token.getText());
//    }

    private static SampledRelation.Type getSamplingMethod(Token token) {
        switch (token.getType()) {
            case SqlBaseLexer.BERNOULLI:
                return SampledRelation.Type.BERNOULLI;
            case SqlBaseLexer.SYSTEM:
                return SampledRelation.Type.SYSTEM;
        }

        throw new IllegalArgumentException("Unsupported sampling method: " + token.getText());
    }

    private static LogicalBinaryExpression.Type getLogicalBinaryOperator(Token token) {
        switch (token.getType()) {
            case SqlBaseLexer.AND:
                return LogicalBinaryExpression.Type.AND;
            case SqlBaseLexer.OR:
                return LogicalBinaryExpression.Type.OR;
        }

        throw new IllegalArgumentException("Unsupported operator: " + token.getText());
    }

//    private static SortItem.NullOrdering getNullOrderingType(Token token) {
//        switch (token.getType()) {
//            case SqlBaseLexer.FIRST:
//                return SortItem.NullOrdering.FIRST;
//            case SqlBaseLexer.LAST:
//                return SortItem.NullOrdering.LAST;
//        }
//
//        throw new IllegalArgumentException("Unsupported ordering: " + token.getText());
//    }
//
//    private static SortItem.Ordering getOrderingType(Token token) {
//        switch (token.getType()) {
//            case SqlBaseLexer.ASC:
//                return SortItem.Ordering.ASCENDING;
//            case SqlBaseLexer.DESC:
//                return SortItem.Ordering.DESCENDING;
//        }
//
//        throw new IllegalArgumentException("Unsupported ordering: " + token.getText());
//    }
//
//    private static QuantifiedComparisonExpression.Quantifier getComparisonQuantifier(Token symbol) {
//        switch (symbol.getType()) {
//            case SqlBaseLexer.ALL:
//                return QuantifiedComparisonExpression.Quantifier.ALL;
//            case SqlBaseLexer.ANY:
//                return QuantifiedComparisonExpression.Quantifier.ANY;
//            case SqlBaseLexer.SOME:
//                return QuantifiedComparisonExpression.Quantifier.SOME;
//        }
//
//        throw new IllegalArgumentException("Unsupported quantifier: " + symbol.getText());
//    }

    private static String getType(SqlBaseParser.TypeContext type) {
        if (type.baseType() != null) {
            String signature = type.baseType().getText();
            if (type.baseType().DOUBLE_PRECISION() != null) {
                // TODO: Temporary hack that should be removed with new planner.
                signature = "DOUBLE";
            }
            if (!type.typeParameter().isEmpty()) {
                String typeParameterSignature = type
                    .typeParameter()
                    .stream()
                    .map(AstBuilder::typeParameterToString)
                    .collect(Collectors.joining(","));
                signature += "(" + typeParameterSignature + ")";
            }
            return signature;
        }

        if (type.ARRAY() != null) {
            return "ARRAY(" + getType(type.type(0)) + ")";
        }

        if (type.MAP() != null) {
            return "MAP(" + getType(type.type(0)) + "," + getType(type.type(1)) + ")";
        }

        if (type.ROW() != null) {
            StringBuilder builder = new StringBuilder("(");
            for (int i = 0; i < type.identifier().size(); i++) {
                if (i != 0) {
                    builder.append(",");
                }
                builder.append(type.identifier(i).getText())
                    .append(" ")
                    .append(getType(type.type(i)));
            }
            builder.append(")");
            return "ROW" + builder.toString();
        }

        throw new IllegalArgumentException("Unsupported type specification: " + type.getText());
    }

    private static String typeParameterToString(SqlBaseParser.TypeParameterContext typeParameter) {
        if (typeParameter.INTEGER_VALUE() != null) {
            return typeParameter.INTEGER_VALUE().toString();
        }
        if (typeParameter.type() != null) {
            return getType(typeParameter.type());
        }
        throw new IllegalArgumentException("Unsupported typeParameter: " + typeParameter.getText());
    }

    private static void check(boolean condition, String message, ParserRuleContext context) {
        if (!condition) {
            throw new ParsingException(message, null, context.getStart().getLine(), context.getStart().getCharPositionInLine());
        }
    }

    public static NodeLocation getLocation(TerminalNode terminalNode) {
        requireNonNull(terminalNode, "terminalNode is null");
        return getLocation(terminalNode.getSymbol());
    }

    public static NodeLocation getLocation(ParserRuleContext parserRuleContext) {
        requireNonNull(parserRuleContext, "parserRuleContext is null");
        return getLocation(parserRuleContext.getStart());
    }

    public static NodeLocation getLocation(Token token) {
        requireNonNull(token, "token is null");
        return new NodeLocation(token.getLine(), token.getCharPositionInLine());
    }
}

