/*
 * Licensed to Crate under one or more contributor license agreements.
 * See the NOTICE file distributed with this work for additional
 * information regarding copyright ownership.  Crate licenses this file
 * to you under the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.  You may
 * obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
 * implied.  See the License for the specific language governing
 * permissions and limitations under the License.
 *
 * However, if you have executed another commercial license agreement
 * with Crate these terms will supersede the license and you may use the
 * software solely pursuant to the terms of the relevant commercial
 * agreement.
 */

package io.crate.analyze;

import io.crate.analyze.relations.AbstractTableRelation;
import io.crate.analyze.relations.AnalyzedRelation;
import io.crate.analyze.relations.AnalyzedRelationVisitor;
import io.crate.analyze.relations.AnalyzedView;
import io.crate.analyze.relations.JoinPair;
import io.crate.analyze.relations.OrderedLimitedRelation;
import io.crate.analyze.relations.QueriedRelation;
import io.crate.analyze.relations.TableFunctionRelation;
import io.crate.analyze.relations.UnionSelect;
import io.crate.expression.symbol.Field;
import io.crate.expression.symbol.Function;
import io.crate.expression.symbol.Literal;
import io.crate.expression.symbol.Symbol;
import io.crate.expression.symbol.SymbolType;
import io.crate.expression.symbol.format.SymbolPrinter;
import io.crate.metadata.Reference;
import io.crate.planner.node.dql.join.JoinType;
import io.crate.sql.Identifiers;
import io.crate.sql.tree.QualifiedName;

import javax.annotation.Nullable;
import java.util.Iterator;
import java.util.List;
import java.util.stream.Collectors;

public final class SQLPrinter {

    private final Visitor visitor;

    public SQLPrinter(SymbolPrinter symbolPrinter) {
        visitor = new Visitor(symbolPrinter);
    }

    public String format(AnalyzedStatement stmt) {
        StringBuilder sb = new StringBuilder();
        if (canPrint(stmt)) {
            visitor.process((AnalyzedRelation) stmt, sb);
        } else {
            throw new UnsupportedOperationException("Cannot format " + stmt);
        }
        return sb.toString();
    }

    public boolean canPrint(AnalyzedStatement stmt) {
        return stmt instanceof QueriedRelation;
    }

    public static class Visitor extends AnalyzedRelationVisitor<StringBuilder, Void> {

        private final SymbolPrinter symbolPrinter;

        public Visitor(SymbolPrinter symbolPrinter) {
            this.symbolPrinter = symbolPrinter;
            symbolPrinter.registerSqlPrinterVisitor(this);
        }


        @Override
        public Void visitQueriedTable(QueriedTable<?> queriedTable, StringBuilder sb) {
            return printSelect(queriedTable, sb);
        }

        @Override
        public Void visitView(AnalyzedView analyzedView, StringBuilder sb) {
            sb.append(analyzedView.name());
            return null;
        }

        @Override
        public Void visitUnionSelect(UnionSelect unionSelect, StringBuilder sb) {
            process(unionSelect.left(), sb);
            sb.append(" UNION ALL ");
            process(unionSelect.right(), sb);
            return null;
        }

        @Override
        public Void visitOrderedLimitedRelation(OrderedLimitedRelation relation, StringBuilder sb) {
            process(relation.childRelation(), sb);
            addOrderBy(sb, relation.orderBy(), relation.outputs(), relation.fields());
            clauseAndSymbol(sb, "LIMIT", relation.limit());
            clauseAndSymbol(sb, "OFFSET", relation.offset());
            return null;
        }

        @Override
        public Void visitQueriedSelectRelation(QueriedSelectRelation relation, StringBuilder sb) {
            return printSelect(relation, sb);
        }

        @Override
        public Void visitMultiSourceSelect(MultiSourceSelect mss, StringBuilder sb) {
            return printSelect(mss, sb);
        }

        private Void printSelect(QueriedRelation relation, StringBuilder sb) {
            sb.append("SELECT ");
            addOutputs(relation, sb);
            addFrom(sb, relation);
            clauseAndQuery(sb, "WHERE", relation.where());
            addGroupBy(sb, relation.groupBy(), relation.outputs(), relation.fields());
            clauseAndQuery(sb, "HAVING", relation.having());
            addOrderBy(sb, relation.orderBy(), relation.outputs(), relation.fields());
            clauseAndSymbol(sb, "LIMIT", relation.limit());
            clauseAndSymbol(sb, "OFFSET", relation.offset());
            return null;
        }

        private String printSymbol(Symbol symbol) {
            if (symbol instanceof Field) {
                Field field = ((Field) symbol);
                if (field.relation() instanceof UnionSelect) {
                    // do not fully qualify field names in union select
                    return Identifiers.quoteIfNeeded(field.path().outputName());
                } else if (field.relation() instanceof AnalyzedView) {
                    // use the name of the view instead of the underlying relation
                    return ((AnalyzedView) field.relation()).name() + "." + Identifiers.quoteIfNeeded(field.path().outputName());
                } else {
                    return quoteQualifiedName(field.relation().getQualifiedName())
                           + "." + Identifiers.quoteIfNeeded(field.path().outputName());
                }
            }
            if (symbol instanceof Reference && "".equals(((Reference) symbol).ident().tableIdent().schema())) {
                return ((Reference) symbol).column().sqlFqn();
            }
            return symbolPrinter.printQualified(symbol);
        }

        private void clauseAndSymbol(StringBuilder sb, String clause, @Nullable Symbol symbol) {
            if (symbol == null) {
                return;
            }
            sb.append(" ");
            sb.append(clause);
            sb.append(" ");
            sb.append(printSymbol(symbol));
        }

        private void clauseAndQuery(StringBuilder sb, String clause, @Nullable QueryClause query) {
            if (query == null) {
                return;
            }
            if (query.noMatch()) {
                clauseAndSymbol(sb, clause, Literal.of(false));
                return;
            }
            if (!query.hasQuery()) {
                return;
            }
            clauseAndSymbol(sb, clause, query.query());
        }

        private void addOrderBy(StringBuilder sb, @Nullable OrderBy orderBy, List<Symbol> outputs, List<Field> fields) {
            if (orderBy == null || orderBy.orderBySymbols().isEmpty()) {
                return;
            }
            sb.append(" ORDER BY ");
            for (int i = 0; i < orderBy.orderBySymbols().size(); i++) {
                Symbol orderBySymbol = orderBy.orderBySymbols().get(i);
                sb.append(resolveOutputName(orderBySymbol, outputs, fields));
                sb.append(" ");
                sb.append(orderBy.reverseFlags()[i] ? "DESC" : "ASC");
                Boolean nullsFirst = orderBy.nullsFirst()[i];
                if (nullsFirst != null) {
                    sb.append(" ");
                    sb.append(nullsFirst ? "NULLS FIRST" : "NULLS LAST");
                }
                addCommaIfNotLast(sb, orderBy.orderBySymbols().size(), i);
            }
        }

        private void addGroupBy(StringBuilder sb, List<Symbol> groupKeys, List<Symbol> outputs, List<Field> fields) {
            if (groupKeys.isEmpty()) {
                return;
            }
            sb.append(" GROUP BY ");
            for (int i = 0; i < groupKeys.size(); i++) {
                Symbol groupKeySymbol = groupKeys.get(i);
                sb.append(resolveOutputName(groupKeySymbol, outputs, fields));
                addCommaIfNotLast(sb, groupKeys.size(), i);
            }
        }

        private static void addCommaIfNotLast(StringBuilder sb, int collectionSize, int idx) {
            if (idx + 1 < collectionSize) {
                sb.append(", ");
            }
        }

        private void addOutputs(QueriedRelation relation, StringBuilder sb) {
            List<Field> fields = relation.fields();
            List<Symbol> outputs = relation.outputs();
            for (int i = 0; i < fields.size(); i++) {
                addOutput(sb, fields.get(i), outputs.get(i));
                addCommaIfNotLast(sb, fields.size(), i);
            }
        }

        private void addOutput(StringBuilder sb, Field field, Symbol output) {
            if (output instanceof Reference) {
                Reference ref = (Reference) output;
                if (ref.column().sqlFqn().equals(field.path().outputName())) {
                    sb.append(printSymbol(ref));
                } else {
                    sb.append(printSymbol(ref));
                    sb.append(" AS ");
                    sb.append(field.path().outputName());
                }
            } else if (output instanceof Function || output instanceof Literal) {
                String name = printSymbol(output);
                sb.append(name);
                if (!name.equals(field.path().outputName())) {
                    sb.append(" AS ");
                    sb.append(Identifiers.quoteIfNeeded(field.path().outputName()));
                }
            } else if (output instanceof Field) {
                String name = printSymbol(output);
                sb.append(name);
                if (!field.path().outputName().equals(((Field) output).path().outputName())) {
                    sb.append(" AS ");
                    sb.append(Identifiers.quoteIfNeeded(field.path().outputName()));
                }
            } else {
                sb.append(printSymbol(output));
            }
        }

        private void addFrom(StringBuilder sb, QueriedRelation relation) {
            sb.append(" FROM ");
            if (relation instanceof QueriedTable) {
                AbstractTableRelation<?> tableRelation = ((QueriedTable) relation).tableRelation();
                if (tableRelation instanceof TableFunctionRelation) {
                    QualifiedName qName = tableRelation.getQualifiedName();
                    Function function = ((TableFunctionRelation) tableRelation).function();
                    if (qName.getParts().size() == 2 && qName.getParts().get(1).equals(function.info().ident().name())) {
                        sb.append(printSymbol(function));
                    } else {
                        sb.append(printSymbol(function));
                        sb.append(" AS ");
                        sb.append(quoteQualifiedName(qName));
                    }
                } else {
                    sb.append(tableRelation.tableInfo().ident().sqlFqn());
                }
            } else if (relation instanceof QueriedSelectRelation) {
                QueriedRelation subRelation = ((QueriedSelectRelation) relation).subRelation();
                if (subRelation instanceof AnalyzedView) {
                    process(subRelation, sb);
                } else {
                    sb.append("(");
                    process(subRelation, sb);
                    sb.append(") ");
                    sb.append(quoteQualifiedName(subRelation.getQualifiedName()));
                }
            } else if (relation instanceof MultiSourceSelect) {
                addJoinClause((MultiSourceSelect) relation, sb);
            } else {
                throw new IllegalStateException("Unknown relation in from clause: " + relation);
            }
        }

        private void addJoinClause(MultiSourceSelect mss, StringBuilder sb) {
            // first resolve implicit cross joins, e.g. select * from t1, t2
            Iterator<AnalyzedRelation> sourcesIt = mss.sources().values().iterator();
            int numberOfCrossJoins = mss.sources().size() - mss.joinPairs().size() - 1;
            int i = 0;
            while (i < numberOfCrossJoins) {
                if (i == 0) {
                    printRelation(sourcesIt.next(), sb);
                }
                sb.append(" CROSS JOIN ");
                printRelation(sourcesIt.next(), sb);
                i++;
            }
            // now print explicit join pairs
            if (mss.joinPairs().isEmpty()) {
                return;
            }
            if (i == 0) {
                JoinPair firstPair = mss.joinPairs().iterator().next();
                AnalyzedRelation leftRelation = mss.sources().get(firstPair.left());
                printRelation(leftRelation, sb);
            }
            for (JoinPair currentJoinPair : mss.joinPairs()) {
                sb.append(" ");
                sb.append(currentJoinPair.joinType());
                sb.append(" JOIN ");
                AnalyzedRelation rightRelation = mss.sources().get(currentJoinPair.right());
                printRelation(rightRelation, sb);
                if (currentJoinPair.joinType() != JoinType.CROSS) {
                    sb.append(" ON ");
                    sb.append(printSymbol(currentJoinPair.condition()));
                }
            }
        }

        private void printRelation(AnalyzedRelation relation, StringBuilder sb) {
            if (relation instanceof QueriedTable) {
                QueriedTable queriedTable = (QueriedTable) relation;
                String relationName = queriedTable.tableRelation().tableInfo().ident().toString();
                if (queriedTable.where().query() != null) {
                    // this source has a where clause, we need to process it in a subquery
                    sb.append("(");
                    printSelect(queriedTable, sb);
                    sb.append(")");
                } else {
                    sb.append(relationName);
                }
                String quotedQualifiedName = quoteQualifiedName(relation.getQualifiedName());
                if (!relationName.equals(quotedQualifiedName)) {
                    sb.append(" AS ");
                    sb.append(quotedQualifiedName);
                }
            } else if (relation instanceof QueriedSelectRelation) {
                QueriedRelation subRelation = ((QueriedSelectRelation) relation).subRelation();
                if (subRelation instanceof AnalyzedView) {
                    process(subRelation, sb);
                    String quotedQualifiedName = quoteQualifiedName(subRelation.getQualifiedName());
                    if (!((AnalyzedView) subRelation).name().toString().equals(quotedQualifiedName)) {
                        sb.append(" AS ");
                        sb.append(quotedQualifiedName);
                    }
                } else {
                    sb.append("(");
                    process(subRelation, sb);
                    sb.append(")");
                    sb.append(" AS ");
                    sb.append(quoteQualifiedName(subRelation.getQualifiedName()));
                }
            } else {
                throw new IllegalStateException("Unknown relation: " + relation);
            }
        }

        private String resolveOutputName(Symbol symbol, List<Symbol> outputs, List<Field> fields) {
            if (symbol.symbolType() == SymbolType.LITERAL) {
                int outputIndex = outputs.indexOf(symbol);
                if (outputIndex != -1) {
                    // If the Literal is aliased, then we have to use that alias
                    return Identifiers.quoteIfNeeded(fields.get(outputIndex).path().outputName());
                } else {
                    // Literals have to be quoted (otherwise it would be a positional group by / order by).
                    return Identifiers.quoteIfNeeded(printSymbol(symbol));
                }
            }
            return printSymbol(symbol);
        }

        @Override
        protected Void visitAnalyzedRelation(AnalyzedRelation relation, StringBuilder context) {
            throw new UnsupportedOperationException("Cannot format statement: " + relation);
        }
    }

    public static String quoteQualifiedName(QualifiedName qualifiedName) {
        return qualifiedName.getParts().stream().map(part -> {
            if (!part.startsWith("\"") && part.contains(".")) {
                return "\"" + part + "\"";
            }
            return part;
        }).collect(Collectors.joining("."));
    }
}
