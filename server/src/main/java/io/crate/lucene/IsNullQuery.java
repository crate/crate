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

import io.crate.expression.symbol.Function;
import io.crate.expression.symbol.Symbol;
import io.crate.expression.symbol.SymbolType;
import io.crate.metadata.Reference;
import io.crate.sql.tree.ColumnPolicy;
import io.crate.types.ArrayType;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.ConstantScoreQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.TermQuery;
import org.elasticsearch.common.lucene.search.Queries;
import org.elasticsearch.index.mapper.FieldNamesFieldMapper;
import org.elasticsearch.index.mapper.MappedFieldType;
import org.elasticsearch.index.query.ExistsQueryBuilder;

class IsNullQuery implements FunctionToQuery {

    @Override
    public Query apply(Function input, LuceneQueryBuilder.Context context) {
        assert input != null : "input must not be null";
        assert input.arguments().size() == 1 : "function's number of arguments must be 1";
        Symbol arg = input.arguments().get(0);
        if (arg.symbolType() != SymbolType.REFERENCE) {
            return null;
        }
        Reference reference = (Reference) arg;
        String columnName = reference.column().fqn();

        // ExistsQueryBuilder.newFilter doesn't build the correct exists query for arrays
        // because ES isn't really aware of explicit array types
        if (reference.valueType() instanceof ArrayType) {
            MappedFieldType fieldType = context.getFieldTypeOrNull(columnName);
            if (fieldType != null) {
                return Queries.not(fieldType.existsQuery(context.queryShardContext));
            }
            MappedFieldType fieldNames = context.getFieldTypeOrNull(FieldNamesFieldMapper.NAME);
            if (fieldNames != null) {
                return Queries.not(new ConstantScoreQuery(
                    new TermQuery(new Term(FieldNamesFieldMapper.NAME, columnName))));
            }
        }
        if (reference.columnPolicy() == ColumnPolicy.IGNORED) {
            // There won't be any indexed field names for the children, need to use expensive query fallback
            return null;
        }
        return Queries.not(ExistsQueryBuilder.newFilter(context.queryShardContext, columnName));
    }
}
