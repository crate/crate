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

package io.crate.operation.collect.collectors;

import io.crate.analyze.OrderBy;
import io.crate.analyze.symbol.Symbol;
import io.crate.lucene.FieldTypeLookup;
import io.crate.metadata.ColumnIdent;
import io.crate.metadata.Reference;
import io.crate.operation.reference.doc.lucene.LuceneMissingValue;
import org.apache.lucene.search.BooleanClause;
import org.apache.lucene.search.BooleanQuery;
import org.apache.lucene.search.FieldDoc;
import org.apache.lucene.search.MatchAllDocsQuery;
import org.apache.lucene.search.Query;
import org.elasticsearch.index.mapper.MappedFieldType;
import org.elasticsearch.index.query.QueryShardContext;

import java.util.function.Function;

import static java.util.Objects.requireNonNull;

public class OptimizeQueryForSearchAfter implements Function<FieldDoc, Query> {

    private final OrderBy orderBy;
    private final Object[] missingValues;
    private final QueryShardContext queryShardContext;
    private final FieldTypeLookup fieldTypeLookup;

    public OptimizeQueryForSearchAfter(OrderBy orderBy,
                                       QueryShardContext queryShardContext,
                                       FieldTypeLookup fieldTypeLookup) {
        this.orderBy = orderBy;
        missingValues = new Object[orderBy.orderBySymbols().size()];
        this.queryShardContext = queryShardContext;
        this.fieldTypeLookup = fieldTypeLookup;
        for (int i = 0; i < orderBy.orderBySymbols().size(); i++) {
            missingValues[i] = LuceneMissingValue.missingValue(orderBy, i);
        }
    }

    @Override
    public Query apply(FieldDoc lastCollected) {
        BooleanQuery.Builder queryBuilder = new BooleanQuery.Builder();
        for (int i = 0; i < orderBy.orderBySymbols().size(); i++) {
            Symbol order = orderBy.orderBySymbols().get(i);
            Object value = lastCollected.fields[i];
            if (order instanceof Reference) {
                final ColumnIdent columnIdent = ((Reference) order).column();
                if (columnIdent.isSystemColumn()) {
                    // We can't optimize the initial query because the BooleanQuery
                    // must not contain system columns.
                    return null;
                }
                boolean nullsFirst = orderBy.nullsFirst()[i] == null ? false : orderBy.nullsFirst()[i];
                value = value == null || value.equals(missingValues[i]) ? null : value;
                if (nullsFirst && value == null) {
                    // no filter needed
                    continue;
                }
                String columnName = columnIdent.fqn();
                MappedFieldType fieldType = requireNonNull(
                    fieldTypeLookup.get(columnName), "Column must exist: " + columnName);

                Query orderQuery;
                // nulls already gone, so they should be excluded
                if (nullsFirst) {
                    BooleanQuery.Builder booleanQuery = new BooleanQuery.Builder();
                    booleanQuery.add(new MatchAllDocsQuery(), BooleanClause.Occur.MUST);
                    if (orderBy.reverseFlags()[i]) {
                        booleanQuery.add(fieldType.rangeQuery(null, value, false, true, queryShardContext), BooleanClause.Occur.MUST_NOT);
                    } else {
                        booleanQuery.add(fieldType.rangeQuery(value, null, true, false, queryShardContext), BooleanClause.Occur.MUST_NOT);
                    }
                    orderQuery = booleanQuery.build();
                } else {
                    if (orderBy.reverseFlags()[i]) {
                        orderQuery = fieldType.rangeQuery(value, null, false, false, queryShardContext);
                    } else {
                        orderQuery = fieldType.rangeQuery(null, value, false, false, queryShardContext);
                    }
                }
                queryBuilder.add(orderQuery, BooleanClause.Occur.MUST);
            }
        }
        BooleanQuery query = queryBuilder.build();
        if (query.clauses().size() > 0) {
            return query;
        } else {
            return null;
        }
    }
}
