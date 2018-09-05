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

import io.crate.expression.symbol.Literal;
import io.crate.metadata.Reference;
import io.crate.types.CollectionType;
import io.crate.types.DataTypes;
import org.apache.lucene.search.Query;
import org.elasticsearch.common.lucene.search.Queries;
import org.elasticsearch.index.mapper.MappedFieldType;

class AnyEqQuery extends AbstractAnyQuery {

    @Override
    protected Query applyArrayReference(Reference arrayReference, Literal literal, LuceneQueryBuilder.Context context) {
        MappedFieldType fieldType = context.getFieldTypeOrNull(arrayReference.column().fqn());
        if (fieldType == null) {
            if (CollectionType.unnest(arrayReference.valueType()).equals(DataTypes.OBJECT)) {
                return null; // fallback to generic query to enable {x=10} = any(objects)
            }
            return Queries.newMatchNoDocsQuery("column doesn't exist in this index");
        }
        return fieldType.termQuery(literal.value(), context.queryShardContext());
    }

    @Override
    protected Query applyArrayLiteral(Reference reference, Literal arrayLiteral, LuceneQueryBuilder.Context context) {
        String columnName = reference.column().fqn();
        return LuceneQueryBuilder.termsQuery(context.getFieldTypeOrNull(columnName), LuceneQueryBuilder.asList(arrayLiteral), context.queryShardContext);
    }
}
