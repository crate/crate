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

package io.crate.analyze.algebra;

import io.crate.collections.Lists2;
import io.crate.expression.symbol.Symbol;
import io.crate.sql.tree.AliasedRelation;
import io.crate.sql.tree.DefaultTraversalVisitor;
import io.crate.sql.tree.Expression;
import io.crate.sql.tree.Node;
import io.crate.sql.tree.Query;
import io.crate.sql.tree.QuerySpecification;
import io.crate.sql.tree.SortItem;
import io.crate.sql.tree.Table;
import io.crate.sql.tree.Union;

import java.util.List;

public final class Algebraist {

    public Operator transform(Node node) {
        return new Visitor().process(node, null);
    }

    private static class Context {
    }

    private static class Visitor extends DefaultTraversalVisitor<Operator, Context> {

        @Override
        protected Operator visitQuery(Query node, Context context) {
            var source = node.getQueryBody().accept(this, context);
            var maybeOrdered = applyOrdering(source, node.getOrderBy());
            var maybeWithOffset = node.getOffset()
                .map(offset -> (Operator) new Offset(maybeOrdered, resolveSymbol(offset)))
                .orElse(maybeOrdered);
            return node.getLimit()
                .map(limit -> (Operator) new Limit(maybeWithOffset, resolveSymbol(limit)))
                .orElse(maybeWithOffset);
        }

        @Override
        protected Operator visitQuerySpecification(QuerySpecification node, Context context) {
            var sources = Lists2.map(node.getFrom(), rel -> rel.accept(this, context));
            Operator source;
            switch (sources.size()) {
                case 0:
                    throw new UnsupportedOperationException("NYI: empty sources");
                case 1:
                    source = sources.get(0);
                    break;
                default:
                    source = new Join(sources);
                    break;
            }
            var resolveQualifiedName = new QualifiedNameResolver(sources);
            var where = node.getWhere()
                .map(condition -> (Operator) new Selection(source, resolveSymbol(condition)))
                .orElse(source);
            // apply projection
            var groupBy = applyGroupBy(where, node.getGroupBy());
            var having = node.getHaving()
                .map(condition -> (Operator) new Selection(groupBy, resolveSymbol(condition)))
                .orElse(groupBy);
            var orderBy = applyOrdering(having, node.getOrderBy());
            var offset = node.getOffset()
                .map(x -> (Operator) new Offset(orderBy, resolveSymbol(x)))
                .orElse(orderBy);
            return node.getLimit()
                .map(x -> (Operator) new Limit(offset, resolveSymbol(x)))
                .orElse(offset);
        }

        @Override
        protected Operator visitAliasedRelation(AliasedRelation node, Context context) {
            return super.visitAliasedRelation(node, context);
        }

        @Override
        protected Operator visitTable(Table node, Context context) {
            return super.visitTable(node, context);
        }

        @Override
        protected Operator visitUnion(Union node, Context context) {
            return super.visitUnion(node, context);
        }
    }

    private static Symbol resolveSymbol(Expression expression) {
        throw new UnsupportedOperationException("NYI resolveSymbol");
    }

    private static Operator applyGroupBy(Operator source, List<Expression> groupBy) {
        if (groupBy.isEmpty()) {
            return source;
        }
        throw new UnsupportedOperationException("NYI applyGroupBy");
    }

    /* Create an Order operator with expressions resolved by alias, ordinal-reference or column name.
     *
     * SELECT x, y as a ... ORDER BY 1, a, x
     */
    private static Operator applyOrdering(Operator operator, List<SortItem> orderBy) {
        if (orderBy.isEmpty()) {
            return operator;
        }
        throw new UnsupportedOperationException("NYI");
    }
}
