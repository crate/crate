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

package io.crate.analyze.relations.select;

import io.crate.analyze.OutputNameFormatter;
import io.crate.analyze.expressions.ExpressionAnalysisContext;
import io.crate.analyze.expressions.ExpressionAnalyzer;
import io.crate.analyze.relations.AnalyzedRelation;
import io.crate.analyze.validator.SelectSymbolValidator;
import io.crate.expression.symbol.AliasSymbol;
import io.crate.expression.symbol.Symbol;
import io.crate.expression.symbol.Symbols;
import io.crate.metadata.ColumnIdent;
import io.crate.metadata.RelationName;
import io.crate.sql.tree.AllColumns;
import io.crate.sql.tree.DefaultTraversalVisitor;
import io.crate.sql.tree.QualifiedName;
import io.crate.sql.tree.SelectItem;
import io.crate.sql.tree.SingleColumn;

import java.util.List;
import java.util.Locale;
import java.util.Map;

public class SelectAnalyzer {

    public static final InnerVisitor INSTANCE = new InnerVisitor();

    public static SelectAnalysis analyzeSelectItems(List<SelectItem> selectItems,
                                                    Map<RelationName, AnalyzedRelation> sources,
                                                    ExpressionAnalyzer expressionAnalyzer,
                                                    ExpressionAnalysisContext expressionAnalysisContext) {
        SelectAnalysis selectAnalysis = new SelectAnalysis(
            selectItems.size(), sources, expressionAnalyzer, expressionAnalysisContext);
        selectItems.forEach(x -> x.accept(INSTANCE, selectAnalysis));
        SelectSymbolValidator.validate(selectAnalysis.outputSymbols());
        return selectAnalysis;
    }

    private static class InnerVisitor extends DefaultTraversalVisitor<Void, SelectAnalysis> {

        @Override
        protected Void visitSingleColumn(SingleColumn node, SelectAnalysis context) {
            Symbol symbol = context.toSymbol(node.getExpression());
            String alias = node.getAlias();
            if (alias != null) {
                context.add(new ColumnIdent(alias), new AliasSymbol(alias, symbol));
            } else {
                context.add(new ColumnIdent(OutputNameFormatter.format(node.getExpression())), symbol);
            }
            return null;
        }

        @Override
        protected Void visitAllColumns(AllColumns node, SelectAnalysis context) {
            if (node.getPrefix().isPresent()) {
                // prefix is either: <tableOrAlias>.* or <schema>.<table>

                QualifiedName prefix = node.getPrefix().get();
                AnalyzedRelation relation = context.sources().get(RelationName.of(prefix, null));
                if (relation != null) {
                    addAllFieldsFromRelation(context, relation);
                    return null;
                }

                int matches = 0;
                if (prefix.getParts().size() == 1) {
                    // e.g.  select mytable.* from foo.mytable; prefix is mytable, source is [foo, mytable]
                    // if prefix matches second part of qualified name this is okay
                    String prefixName = prefix.getParts().get(0);
                    for (Map.Entry<RelationName, AnalyzedRelation> entry : context.sources().entrySet()) {
                        RelationName relationName = entry.getKey();
                        // schema.table
                        if (relationName.schema() != null && prefixName.equals(relationName.name())) {
                            addAllFieldsFromRelation(context, entry.getValue());
                            matches++;
                        }
                    }
                }
                switch (matches) {
                    case 0:
                        throw new IllegalArgumentException(String.format(Locale.ENGLISH, "The relation \"%s\" is not in the FROM clause.", prefix));
                    case 1:
                        return null; // yay found something
                    default:
                        // e.g. select mytable.* from foo.mytable, bar.mytable
                        throw new IllegalArgumentException(String.format(Locale.ENGLISH, "The referenced relation \"%s\" is ambiguous.", prefix));
                }
            } else {
                for (AnalyzedRelation relation : context.sources().values()) {
                    addAllFieldsFromRelation(context, relation);
                }
            }
            return null;
        }

        private static void addAllFieldsFromRelation(SelectAnalysis context, AnalyzedRelation relation) {
            for (Symbol field : relation.outputs()) {
                var columnIdent = Symbols.pathFromSymbol(field);
                if (!columnIdent.isSystemColumn()) {
                    context.add(Symbols.pathFromSymbol(field), field);
                }
            }
        }
    }
}
