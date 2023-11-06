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

package io.crate.expression.operator.any;

import org.apache.lucene.search.BooleanClause;
import org.apache.lucene.search.BooleanClause.Occur;
import org.apache.lucene.search.BooleanQuery;
import org.apache.lucene.search.MatchNoDocsQuery;
import org.apache.lucene.search.Query;
import org.elasticsearch.common.lucene.search.Queries;
import org.elasticsearch.index.mapper.MappedFieldType;

import io.crate.expression.operator.EqOperator;
import io.crate.expression.predicate.IsNullPredicate;
import io.crate.expression.symbol.Function;
import io.crate.expression.symbol.Literal;
import io.crate.lucene.LuceneQueryBuilder.Context;
import io.crate.metadata.IndexType;
import io.crate.metadata.Reference;
import io.crate.metadata.functions.BoundSignature;
import io.crate.metadata.functions.Signature;
import io.crate.sql.tree.ComparisonExpression;
import io.crate.types.EqQuery;
import io.crate.types.StorageSupport;

public final class AnyNeqOperator extends AnyOperator {

    public static String NAME = OPERATOR_PREFIX + ComparisonExpression.Type.NOT_EQUAL.getValue();

    AnyNeqOperator(Signature signature, BoundSignature boundSignature) {
        super(signature, boundSignature);
    }

    @Override
    boolean matches(Object probe, Object candidate) {
        return leftType.compare(probe, candidate) != 0;
    }

    @Override
    protected Query refMatchesAnyArrayLiteral(Function any, Reference probe, Literal<?> candidates, Context context) {
        //  col != ANY ([1,2,3]) --> not(col=1 and col=2 and col=3)
        String columnName = probe.storageIdent();
        MappedFieldType fieldType = context.getFieldTypeOrNull(columnName);
        if (fieldType == null) {
            return new MatchNoDocsQuery("column does not exist in this index");
        }

        BooleanQuery.Builder andBuilder = new BooleanQuery.Builder();
        for (Object value : (Iterable<?>) candidates.value()) {
            if (value == null) {
                continue;
            }
            var fromPrimitive = EqOperator.fromPrimitive(
                probe.valueType(),
                columnName,
                value,
                probe.hasDocValues(),
                probe.indexType());
            if (fromPrimitive == null) {
                return null;
            }
            andBuilder.add(fromPrimitive, BooleanClause.Occur.MUST);
        }
        Query exists = IsNullPredicate.refExistsQuery(probe, context, false);
        return new BooleanQuery.Builder()
            .add(Queries.not(andBuilder.build()), Occur.MUST)
            .add(exists, Occur.FILTER)
            .build();
    }

    @Override
    @SuppressWarnings({"unchecked", "rawtypes"})
    protected Query literalMatchesAnyArrayRef(Function any, Literal<?> probe, Reference candidates, Context context) {
        // 1 != any ( col ) -->  gt 1 or lt 1
        String columnName = candidates.storageIdent();

        MappedFieldType fieldType = context.getFieldTypeOrNull(columnName);
        if (fieldType == null) {
            return new MatchNoDocsQuery("column does not exist in this index");
        }
        StorageSupport<?> storageSupport = probe.valueType().storageSupport();
        if (storageSupport == null) {
            return null;
        }
        EqQuery eqQuery = storageSupport.eqQuery();
        if (eqQuery == null) {
            return null;
        }
        Object value = probe.value();
        BooleanQuery.Builder query = new BooleanQuery.Builder();
        query.setMinimumNumberShouldMatch(1);
        var gt = eqQuery.rangeQuery(
            columnName,
            value,
            null,
            false,
            false,
            candidates.hasDocValues(),
            candidates.indexType() != IndexType.NONE);
        var lt = eqQuery.rangeQuery(
            columnName,
            null,
            value,
            false,
            false,
            candidates.hasDocValues(),
            candidates.indexType() != IndexType.NONE);
        if (lt == null || gt == null) {
            assert lt != null || gt == null : "If lt is null, gt must be null";
            return null;
        }
        query.add(gt, BooleanClause.Occur.SHOULD);
        query.add(lt, BooleanClause.Occur.SHOULD);
        return query.build();
    }
}
