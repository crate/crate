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

package io.crate.testing;

import java.util.Arrays;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.StringJoiner;

import io.crate.analyze.OrderBy;
import io.crate.analyze.QueriedSelectRelation;
import io.crate.analyze.WhereClause;
import io.crate.common.collections.Lists2;
import io.crate.common.collections.Ordering;
import io.crate.expression.symbol.Literal;
import io.crate.expression.symbol.Symbol;
import io.crate.expression.symbol.format.Style;

public class SQLPrinter {

    public static String print(Object o) {
        if (o instanceof QueriedSelectRelation) {
            return print((QueriedSelectRelation) o);
        } else if (o instanceof OrderBy) {
            return print((OrderBy) o);
        } else if (o instanceof Symbol) {
            return print((Symbol) o);
        } else if (o instanceof HashSet<?> set) {
            Object[] elements = set.toArray();
            Arrays.sort(elements, Ordering.usingToString());
            return print(List.of(elements));
        } else if (o instanceof Collection<?> collection) {
            StringJoiner joiner = new StringJoiner(", ");
            for (var item : collection) {
                String str = item instanceof Symbol symbol ? symbol.toString(Style.QUALIFIED) : item.toString();
                joiner.add(str);
            }
            return joiner.toString();
        } else if (o == null) {
            return "null";
        } else if (o instanceof WhereClause) {
            WhereClause queryClause = (WhereClause) o;
            if (queryClause.hasQuery()) {
                return print(queryClause.query());
            }
        }
        return o.toString();
    }

    public static String print(Collection<Symbol> symbols) {
        return Lists2.joinOn(", ", symbols, x -> x.toString(Style.QUALIFIED));
    }


    public static String print(Symbol symbol) {
        StringBuilder sb = new StringBuilder();
        sb.append(symbol.toString(Style.QUALIFIED));
        return sb.toString();
    }

    public static String print(OrderBy orderBy) {
        StringBuilder sb = new StringBuilder();
        process(orderBy, sb);
        return sb.toString();
    }

    public static String print(QueriedSelectRelation relation) {
        StringBuilder sb = new StringBuilder();

        sb.append("SELECT ");
        sb.append(Lists2.joinOn(", ", relation.outputs(), x -> x.toString(Style.QUALIFIED)));

        if (relation.where() != Literal.BOOLEAN_TRUE) {
            sb.append(" WHERE ");
            sb.append(relation.where().toString(Style.QUALIFIED));
        }
        if (!relation.groupBy().isEmpty()) {
            sb.append(" GROUP BY ");
            sb.append(Lists2.joinOn(", ", relation.groupBy(), x -> x.toString(Style.QUALIFIED)));
        }
        Symbol having = relation.having();
        if (having != null) {
            sb.append(" HAVING ");
            sb.append(having.toString(Style.QUALIFIED));
        }
        OrderBy orderBy = relation.orderBy();
        if (orderBy != null) {
            sb.append(" ORDER BY ");
            process(orderBy, sb);
        }
        Symbol limit = relation.limit();
        if (limit != null) {
            sb.append(" LIMIT ");
            sb.append(print(limit));
        }
        Symbol offset = relation.offset();
        if (offset != null) {
            sb.append(" OFFSET ");
            sb.append(print(offset));
        }
        return sb.toString();
    }

    public static void process(OrderBy orderBy, StringBuilder sb) {
        int i = 0;
        for (Symbol symbol : orderBy.orderBySymbols()) {
            if (i > 0) {
                sb.append(", ");
            }
            sb.append(symbol.toString(Style.QUALIFIED));
            if (orderBy.reverseFlags()[i]) {
                sb.append(" DESC");
            }
            boolean nullsFirst = orderBy.nullsFirst()[i];
            if (orderBy.reverseFlags()[i] != nullsFirst) {
                sb.append(" NULLS");
                if (nullsFirst) {
                    sb.append(" FIRST");
                } else {
                    sb.append(" LAST");
                }
            }
            i++;
        }
    }
}
