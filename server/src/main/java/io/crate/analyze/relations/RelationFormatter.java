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

package io.crate.analyze.relations;

import java.util.ArrayDeque;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;

import org.jetbrains.annotations.Nullable;

import io.crate.analyze.OrderBy;
import io.crate.analyze.QueriedSelectRelation;
import io.crate.expression.symbol.Literal;
import io.crate.expression.symbol.Symbol;
import io.crate.expression.symbol.format.Style;
import io.crate.metadata.RelationName;

public class RelationFormatter extends AnalyzedRelationVisitor<RelationFormatter.Context, Void> {

    private static final RelationFormatter INSTANCE = new RelationFormatter();

    public static String format(AnalyzedRelation relation, Style style) {
        Context context = new Context(style);
        relation.accept(INSTANCE, context);
        assert context.ancestors.isEmpty()
            : "Must have left all entered relations: " + context.ancestors;
        assert context.indent == 0
            : "Must have left all blocks. Intent is still: " + context.indent;
        return context.sb.toString();
    }

    private RelationFormatter() {
    }

    static class Context {
        private static final int TABSIZE = 4;

        final StringBuilder sb = new StringBuilder();
        final Style style;

        ArrayDeque<AnalyzedRelation> ancestors = new ArrayDeque<>();
        int indent = 0;

        Context(Style style) {
            this.style = style;
        }

        public Context append(String value) {
            sb.append(value);
            return this;
        }

        private Context text(String value) {
            if (indent > 0 && (sb.isEmpty() || sb.charAt(sb.length() - 1) == '\n')) {
                sb.append(" ".repeat(indent));
            }
            sb.append(value);
            return this;
        }

        private Context text(Symbol symbol) {
            return text(symbol.toString(style));
        }

        private Context lines(Iterable<Symbol> symbols) {
            Iterator<Symbol> it = symbols.iterator();
            while (it.hasNext()) {
                Symbol symbol = it.next();
                text(symbol);
                if (it.hasNext()) {
                    sb.append(",\n");
                }
            }
            return this;
        }

        private Context enterBlock(String text) {
            text(text);
            sb.append("\n");
            indent += TABSIZE;
            return this;
        }

        private Context maybeBlock(String text, @Nullable Symbol symbol) {
            if (symbol == null) {
                return this;
            }
            text(text);
            sb.append(" ");
            sb.append(symbol.toString(style));
            sb.append("\n");
            return this;
        }

        private Context leaveBlock() {
            sb.append("\n");
            indent -= TABSIZE;
            return this;
        }

        public Context enterRelation(QueriedSelectRelation relation) {
            if (!ancestors.isEmpty()) {
                enterBlock("(");
            }
            ancestors.push(relation);
            return this;
        }

        public Context leaveRelation() {
            ancestors.pop();
            if (!ancestors.isEmpty()) {
                indent -= TABSIZE;
                text(")");
            }
            return this;
        }
    }


    @Override
    public Void visitQueriedSelectRelation(QueriedSelectRelation relation, Context context) {
        context
            .enterRelation(relation)
            .enterBlock("SELECT" + (relation.isDistinct() ? " DISTINCT" : ""))
                .lines(relation.outputs())
            .leaveBlock()
            .enterBlock("FROM");

        List<JoinPair> joinPairs = relation.joinPairs();
        if (joinPairs.isEmpty()) {
            for (AnalyzedRelation source : relation.from()) {
                source.accept(this, context);
            }
        } else {
            LinkedHashMap<RelationName, AnalyzedRelation> relations = new LinkedHashMap<>();
            for (AnalyzedRelation source : relation.from()) {
                relations.put(source.relationName(), source);
            }
            for (JoinPair joinPair : joinPairs) {
                AnalyzedRelation left = relations.get(joinPair.left());
                AnalyzedRelation right = relations.get(joinPair.right());
                left.accept(this, context);
                context.append("\n");
                context.text(joinPair.joinType().toString() + " JOIN ");
                right.accept(this, context);
                Symbol condition = joinPair.condition();
                if (condition != null) {
                    context
                        .append(" ON ")
                        .append(condition.toString(context.style));
                }
            }
        }

        context.leaveBlock(); // FROM

        if (!relation.where().equals(Literal.BOOLEAN_TRUE)) {
            context
                .enterBlock("WHERE")
                .text(relation.where())
                .leaveBlock();
        }
        if (!relation.groupBy().isEmpty()) {
            context
                .enterBlock("GROUP BY")
                .lines(relation.groupBy())
                .leaveBlock();
        }
        OrderBy orderBy = relation.orderBy();
        if (orderBy != null) {
            context.enterBlock("ORDER BY");
            for (int i = 0; i < orderBy.orderBySymbols().size(); i++) {
                Symbol orderBySymbol = orderBy.orderBySymbols().get(i);
                boolean reverseFlag = orderBy.reverseFlags()[i];
                boolean nullsFirst = orderBy.nullsFirst()[i];
                context
                    .text(orderBySymbol)
                    .append(reverseFlag ? " DESC" : " ASC");
                if (reverseFlag != nullsFirst) {
                    context.append("NULLS");
                    context.append(nullsFirst ? " FIRST" : " LAST");
                }
                if (i + 1 < orderBy.orderBySymbols().size()) {
                    context.text(",\n");
                }
            }
            context.leaveBlock();
        }
        context.maybeBlock("LIMIT", relation.limit());
        context.maybeBlock("OFFSET", relation.offset());
        context.leaveRelation();
        return null;
    }

    @Override
    public Void visitTableRelation(TableRelation relation, Context context) {
        context.text(relation.relationName().fqn());
        return null;
    }

    @Override
    public Void visitDocTableRelation(DocTableRelation relation, Context context) {
        context.text(relation.relationName().fqn());
        return null;
    }

    @Override
    public Void visitAliasedAnalyzedRelation(AliasedAnalyzedRelation relation, Context context) {
        relation.relation().accept(this, context);
        context.append(" AS ");
        context.append(relation.relationName().fqn());
        return null;
    }
}
