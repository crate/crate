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
import io.crate.metadata.Reference;
import io.crate.types.CollectionType;
import io.crate.types.DataType;
import org.apache.lucene.search.Query;

class LikeQuery implements FunctionToQuery {

    @Override
    public Query apply(Function input, LuceneQueryBuilder.Context context) {
        RefAndLiteral refAndLiteral = RefAndLiteral.of(input);
        if (refAndLiteral == null) {
            return null;
        }
        return toQuery(refAndLiteral.reference(), refAndLiteral.literal().value(), context);
    }

    static Query toQuery(Reference reference, Object value, LuceneQueryBuilder.Context context) {
        DataType dataType = CollectionType.unnest(reference.valueType());
        return LikeQueryBuilder.like(
            dataType,
            context.getFieldTypeOrNull(reference.column().fqn()),
            value
        );
    }
}
