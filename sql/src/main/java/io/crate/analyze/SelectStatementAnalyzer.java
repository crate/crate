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

package io.crate.analyze;

import com.google.common.base.Preconditions;
import io.crate.metadata.ReferenceIdent;
import io.crate.metadata.ReferenceInfo;
import io.crate.metadata.TableIdent;
import io.crate.planner.symbol.Function;
import io.crate.planner.symbol.Symbol;
import io.crate.planner.symbol.SymbolType;
import io.crate.sql.tree.*;

import java.util.ArrayList;
import java.util.List;

public class SelectStatementAnalyzer extends StatementAnalyzer<SelectAnalysis> {

    private static PrimaryKeyVisitor primaryKeyVisitor = new PrimaryKeyVisitor();

    @Override
    protected Symbol visitSelect(Select node, SelectAnalysis context) {
        context.outputSymbols(new ArrayList<Symbol>(node.getSelectItems().size()));
        context.outputNames(new ArrayList<String>(node.getSelectItems().size()));

        for (SelectItem item : node.getSelectItems()) {
            process(item, context);
        }

        return null;
    }

    @Override
    protected Symbol visitSingleColumn(SingleColumn node, SelectAnalysis context) {
        Symbol symbol = process(node.getExpression(), context);
        context.outputSymbols().add(symbol);

        if (node.getAlias().isPresent()) {
            context.addAlias(node.getAlias().get(), symbol);
        } else {
            context.addAlias(outputNameFormatter.process(node.getExpression(), null), symbol);
        }

        return null;
    }

    @Override
    protected Symbol visitAllColumns(AllColumns node, SelectAnalysis context) {
        Symbol symbol;
        for (ReferenceInfo referenceInfo : context.table().columns()) {
            symbol = context.allocateReference(referenceInfo.ident());
            context.outputSymbols().add(symbol);
            context.addAlias(referenceInfo.ident().columnIdent().name(), symbol);
        }

        return null;
    }

    protected Symbol visitQualifiedNameReference(QualifiedNameReference node, SelectAnalysis context) {
        Symbol symbol = context.symbolFromAlias(node.getSuffix().getSuffix());
        if (symbol != null) {
            return symbol;
        }
        ReferenceIdent ident;
        List<String> parts = node.getName().getParts();
        switch (parts.size()) {
            case 1:
                ident = new ReferenceIdent(context.table().ident(), parts.get(0));
                break;
            case 3:
                ident = new ReferenceIdent(new TableIdent(parts.get(0), parts.get(1)), parts.get(2));
                break;
            default:
                throw new UnsupportedOperationException("unsupported name reference: " + node);
        }
        return context.allocateReference(ident);
    }


    protected Symbol visitQuerySpecification(QuerySpecification node, SelectAnalysis context) {
        // visit the from first, since this qualifies the select
        if (node.getFrom() != null) {
            for (Relation relation : node.getFrom()) {
                process(relation, context);
            }
        }

        // the parsers sql grammer makes sure that only a integer matches after limit/offset so
        // parseInt can't fail here.
        if (node.getLimit().isPresent()) {
            context.limit(Integer.parseInt(node.getLimit().get()));
        }
        if (node.getOffset().isPresent()) {
            context.offset(Integer.parseInt(node.getOffset().get()));
        }

        if (node.getWhere().isPresent()) {
            processWhereClause(node.getWhere().get(), context);
        }

        process(node.getSelect(), context);

        if (!node.getGroupBy().isEmpty()) {
            analyzeGroupBy(node.getGroupBy(), context);
        }

        Preconditions.checkArgument(node.getHaving().isPresent() == false, "having clause is not yet supported");

        if (node.getOrderBy().size() > 0) {
            List<Symbol> sortSymbols = new ArrayList<>(node.getOrderBy().size());
            context.reverseFlags(new boolean[node.getOrderBy().size()]);
            int i = 0;
            for (SortItem sortItem : node.getOrderBy()) {

                sortSymbols.add(process(sortItem, context));
                context.reverseFlags()[i++] = sortItem.getOrdering() == SortItem.Ordering.DESCENDING;
            }
            context.sortSymbols(sortSymbols);
        }
        return null;
    }

    @Override
    public Symbol visitDelete(Delete node, SelectAnalysis context) {
        context.isDelete(true);

        process(node.getTable(), context);

        if (node.getWhere().isPresent()) {
            processWhereClause(node.getWhere().get(), context);
        }

        return null;
    }

    private void processWhereClause(Expression whereExpression, SelectAnalysis context) {
        Function whereClause = context.whereClause(process(whereExpression, context));
        if (whereClause != null) {
            PrimaryKeyVisitor.Context pkc = primaryKeyVisitor.process(context.table(), whereClause);
            if (pkc != null) {
                if (pkc.noMatch()) {
                    context.noMatch(pkc.noMatch());
                } else {
                    context.primaryKeyLiterals(pkc.keyLiterals());
                }
            }
        }
    }

    private void analyzeGroupBy(List<Expression> groupByExpressions, SelectAnalysis context) {
        List<Symbol> groupBy = new ArrayList<>(groupByExpressions.size());
        for (Expression expression : groupByExpressions) {
            Symbol s = process(expression, context);
            int idx;
            if (s.symbolType() == SymbolType.LONG_LITERAL) {
                idx = ((io.crate.planner.symbol.LongLiteral) s).value().intValue() - 1;
                if (idx < 1) {
                    throw new IllegalArgumentException(
                            String.format("GROUP BY position %s is not in select list", idx));
                }
            } else {
                idx = context.outputSymbols().indexOf(s);
            }

            if (idx >= 0) {
                try {
                    s = context.outputSymbols().get(idx);
                } catch (ArrayIndexOutOfBoundsException e) {
                    throw new IllegalArgumentException(
                            String.format("GROUP BY position %s is not in select list", idx));
                }
            }

            if (s.symbolType() == SymbolType.FUNCTION && ((Function) s).info().isAggregate()) {
                throw new IllegalArgumentException("Aggregate functions are not allowed in GROUP BY");
            }

            groupBy.add(s);
        }
        context.groupBy(groupBy);

        ensureOutputSymbolsInGroupBy(context);
    }

    @Override
    protected Symbol visitNegativeExpression(NegativeExpression node, SelectAnalysis context) {
        // in statements like "where x = -1" the  positive (expression)IntegerLiteral (1)
        // is just wrapped inside a negativeExpression
        // the visitor here swaps it to get -1 in a (symbol)LiteralInteger
        return negativeLiteralVisitor.process(process(node.getValue(), context), null);
    }

    private void ensureOutputSymbolsInGroupBy(SelectAnalysis context) {
        for (Symbol symbol : context.outputSymbols()) {
            if (symbol.symbolType() == SymbolType.FUNCTION && ((Function) symbol).info().isAggregate()) {
                continue;
            }
            if (!context.groupBy().contains(symbol)) {
                throw new IllegalArgumentException(
                        String.format("column %s must appear in the GROUP BY clause or be used in an aggregation function", symbol));
            }
        }
    }

    @Override
    protected Symbol visitSortItem(SortItem node, SelectAnalysis context) {
        return super.visitSortItem(node, context);
    }

    @Override
    protected Symbol visitQuery(Query node, SelectAnalysis context) {
        context.query(node);
        return super.visitQuery(node, context);
    }
}
