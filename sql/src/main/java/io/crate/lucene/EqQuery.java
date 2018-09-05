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

package io.crate.lucene;

import io.crate.expression.symbol.Function;
import io.crate.expression.symbol.Literal;
import io.crate.metadata.Reference;
import io.crate.types.DataTypes;
import org.apache.lucene.search.BooleanClause;
import org.apache.lucene.search.BooleanQuery;
import org.apache.lucene.search.Query;
import org.elasticsearch.common.lucene.search.Queries;
import org.elasticsearch.index.mapper.MappedFieldType;

import java.util.List;

class EqQuery implements FunctionToQuery {

    @Override
    public Query apply(Function input, LuceneQueryBuilder.Context context) {
        RefAndLiteral refAndLiteral = RefAndLiteral.of(input);
        if (refAndLiteral == null) {
            return null;
        }
        Reference reference = refAndLiteral.reference();
        Literal literal = refAndLiteral.literal();
        String columnName = reference.column().fqn();
        MappedFieldType fieldType = context.getFieldTypeOrNull(columnName);
        if (fieldType == null) {
            if (reference.valueType().equals(DataTypes.OBJECT)) {
                return null; // fallback to generic filter for  "o = {x=10, y=20}"
            }
            // field doesn't exist, can't match
            return Queries.newMatchNoDocsQuery("column does not exist in this index");
        }
        if (DataTypes.isCollectionType(reference.valueType()) &&
            DataTypes.isCollectionType(literal.valueType())) {

            List values = LuceneQueryBuilder.asList(literal);
            if (values.isEmpty()) {
                return LuceneQueryBuilder.genericFunctionFilter(input, context);
            }
            Query termsQuery = LuceneQueryBuilder.termsQuery(fieldType, values, context.queryShardContext);

            // wrap boolTermsFilter and genericFunction filter in an additional BooleanFilter to control the ordering of the filters
            // termsFilter is applied first
            // afterwards the more expensive genericFunctionFilter
            BooleanQuery.Builder filterClauses = new BooleanQuery.Builder();
            filterClauses.add(termsQuery, BooleanClause.Occur.MUST);
            filterClauses.add(LuceneQueryBuilder.genericFunctionFilter(input, context), BooleanClause.Occur.MUST);
            return filterClauses.build();
        }
        return fieldType.termQuery(literal.value(), context.queryShardContext);
    }
}
