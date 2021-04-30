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

package io.crate.lucene;

import io.crate.expression.symbol.Literal;
import io.crate.metadata.Reference;
import org.apache.lucene.search.BooleanClause;
import org.apache.lucene.search.BooleanQuery;
import org.apache.lucene.search.Query;
import org.elasticsearch.common.lucene.search.Queries;
import org.elasticsearch.index.mapper.MappedFieldType;

import java.io.IOException;

class AnyNeqQuery extends AbstractAnyQuery {

    @Override
    protected Query literalMatchesAnyArrayRef(Literal candidate, Reference array, LuceneQueryBuilder.Context context) throws IOException {
        // 1 != any ( col ) -->  gt 1 or lt 1
        String columnName = array.column().fqn();
        Object value = candidate.value();

        MappedFieldType fieldType = context.getFieldTypeOrNull(columnName);
        if (fieldType == null) {
            return Queries.newMatchNoDocsQuery("column does not exist in this index");
        }
        BooleanQuery.Builder query = new BooleanQuery.Builder();
        query.setMinimumNumberShouldMatch(1);
        query.add(
            fieldType.rangeQuery(value, null, false, false, null, null, context.queryShardContext),
            BooleanClause.Occur.SHOULD
        );
        query.add(
            fieldType.rangeQuery(null, value, false, false, null, null, context.queryShardContext),
            BooleanClause.Occur.SHOULD
        );
        return query.build();
    }

    @Override
    protected Query refMatchesAnyArrayLiteral(Reference candidate, Literal array, LuceneQueryBuilder.Context context) {
        //  col != ANY ([1,2,3]) --> not(col=1 and col=2 and col=3)
        String columnName = candidate.column().fqn();
        MappedFieldType fieldType = context.getFieldTypeOrNull(columnName);
        if (fieldType == null) {
            return Queries.newMatchNoDocsQuery("column does not exist in this index");
        }

        BooleanQuery.Builder andBuilder = new BooleanQuery.Builder();
        for (Object value : toIterable(array.value())) {
            andBuilder.add(fieldType.termQuery(value, context.queryShardContext()), BooleanClause.Occur.MUST);
        }
        return Queries.not(andBuilder.build());
    }
}
