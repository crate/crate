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

package io.crate.expression.scalar;

import java.util.Collection;
import java.util.Map;

import org.apache.lucene.search.BooleanClause.Occur;
import org.apache.lucene.search.BooleanQuery;
import org.apache.lucene.search.FieldExistsQuery;
import org.apache.lucene.search.Query;
import org.elasticsearch.common.lucene.search.Queries;

import io.crate.data.Input;
import io.crate.expression.predicate.IsNullPredicate;
import io.crate.expression.symbol.Function;
import io.crate.expression.symbol.Symbol;
import io.crate.lucene.LuceneQueryBuilder.Context;
import io.crate.metadata.FunctionType;
import io.crate.metadata.Functions;
import io.crate.metadata.NodeContext;
import io.crate.metadata.Reference;
import io.crate.metadata.Scalar;
import io.crate.metadata.TransactionContext;
import io.crate.metadata.functions.BoundSignature;
import io.crate.metadata.functions.Signature;
import io.crate.metadata.functions.TypeVariableConstraint;
import io.crate.types.ArrayType;
import io.crate.types.DataType;
import io.crate.types.DataTypes;
import io.crate.types.ObjectType;
import io.crate.types.TypeSignature;

public final class NullOrEmptyFunction extends Scalar<Boolean, Object> {

    public static void register(Functions.Builder module) {
        module.add(
            Signature.builder("null_or_empty", FunctionType.SCALAR)
                .argumentTypes(DataTypes.UNTYPED_OBJECT.getTypeSignature())
                .returnType(DataTypes.BOOLEAN.getTypeSignature())
                .features(Feature.DETERMINISTIC, Feature.NON_NULLABLE)
                .build(),
            NullOrEmptyFunction::new
        );
        module.add(
            Signature.builder("null_or_empty", FunctionType.SCALAR)
                .argumentTypes(TypeSignature.parse("array(E)"))
                .returnType(DataTypes.BOOLEAN.getTypeSignature())
                .typeVariableConstraints(TypeVariableConstraint.typeVariableOfAnyType("E"))
                .features(Feature.DETERMINISTIC, Feature.NON_NULLABLE)
                .build(),
            NullOrEmptyFunction::new
        );
    }


    private NullOrEmptyFunction(Signature signature, BoundSignature boundSignature) {
        super(signature, boundSignature);
    }

    @Override
    @SafeVarargs
    public final Boolean evaluate(TransactionContext txnCtx, NodeContext nodeContext, Input<Object>... args) {
        Object value = args[0].value();
        if (value instanceof Map<?, ?> map) {
            return map.isEmpty();
        }
        if (value instanceof Collection<?> collection) {
            return collection.isEmpty();
        }
        return value == null;
    }

    @Override
    public Query toQuery(Function function, Context context) {
        assert function.arguments().size() == 1 : "Function has a single argument";
        Symbol arg = function.arguments().get(0);
        if (!(arg instanceof Reference ref)) {
            return null;
        }
        DataType<?> valueType = ref.valueType();
        if (valueType instanceof ObjectType objectType) {
            if (objectType.innerTypes().isEmpty()) {
                return null;
            }
            BooleanQuery.Builder booleanQuery = new BooleanQuery.Builder()
                .setMinimumNumberShouldMatch(1);
            for (var entry : objectType.innerTypes().entrySet()) {
                String childColumn = entry.getKey();
                Reference childRef = context.getRef(ref.column().getChild(childColumn));
                if (childRef == null) {
                    return null;
                }
                Query refExistsQuery = IsNullPredicate.refExistsQuery(childRef, context, true);
                if (refExistsQuery == null) {
                    return null;
                }
                booleanQuery.add(refExistsQuery, Occur.SHOULD);
            }
            return Queries.not(booleanQuery.build());
        } else if (valueType instanceof ArrayType<?> && ref.hasDocValues()) {
            return Queries.not(new FieldExistsQuery(ref.storageIdent()));
        }
        return null;
    }
}
