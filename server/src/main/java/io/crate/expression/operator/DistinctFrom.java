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

package io.crate.expression.operator;

import static io.crate.metadata.functions.TypeVariableConstraint.typeVariable;

import java.util.Collection;
import java.util.List;
import java.util.Map;

import org.apache.lucene.search.BooleanClause;
import org.apache.lucene.search.MatchAllDocsQuery;
import org.apache.lucene.search.Query;
import org.elasticsearch.common.lucene.search.Queries;

import io.crate.data.Input;
import io.crate.expression.predicate.IsNullPredicate;
import io.crate.expression.symbol.Function;
import io.crate.expression.symbol.Literal;
import io.crate.expression.symbol.Symbol;
import io.crate.lucene.LuceneQueryBuilder;
import io.crate.metadata.FunctionType;
import io.crate.metadata.Functions;
import io.crate.metadata.NodeContext;
import io.crate.metadata.Reference;
import io.crate.metadata.TransactionContext;
import io.crate.metadata.functions.BoundSignature;
import io.crate.metadata.functions.Signature;
import io.crate.types.ArrayType;
import io.crate.types.DataType;
import io.crate.types.ObjectType;
import io.crate.types.TypeSignature;

public class DistinctFrom extends Operator<Object> {

    public static final String NAME = "op_IS DISTINCT FROM";
    public static final Signature SIGNATURE = Signature.builder(NAME, FunctionType.SCALAR)
        .argumentTypes(TypeSignature.parse("E"), TypeSignature.parse("E"))
        .returnType(Operator.RETURN_TYPE.getTypeSignature())
        .features(Feature.DETERMINISTIC, Feature.NOTNULL)
        .typeVariableConstraints(typeVariable("E"))
        .build();

    public static void register(Functions.Builder builder) {
        builder.add(
            SIGNATURE,
            DistinctFrom::new
        );
    }

    private final DataType<Object> argType;

    @SuppressWarnings("unchecked")
    private DistinctFrom(Signature signature, BoundSignature boundSignature) {
        super(signature, boundSignature);
        this.argType = (DataType<Object>) boundSignature.argTypes().getFirst();
    }

    @Override
    public Symbol normalizeSymbol(Function function, TransactionContext txnCtx, NodeContext nodeCtx) {
        try {
            return evaluateIfLiterals(this, txnCtx, nodeCtx, function);
        } catch (Throwable t) {
            return function;
        }
    }

    @Override
    @SafeVarargs
    public final Boolean evaluate(TransactionContext txnCtx, NodeContext nodeCtx, Input<Object>... args) {
        // This operator does not evaluate to NULL if one argument is NULL! If both are NULL it evaluates to FALSE.
        assert args.length == 2 : "number of arguments must be 2";
        Object arg1 = args[0].value();
        Object arg2 = args[1].value();

        // two ``NULL`` values are not distinct from one other
        if (arg1 == null && arg2 == null) {
            return false;
        }
        // Any non-null Literal is distinct from null
        if (arg1 == null || arg2 == null) {
            return true;
        }
        return argType.compare(arg1, arg2) != 0;
    }

    @Override
    public Query toQuery(Function function, LuceneQueryBuilder.Context context) {
        List<Symbol> args = function.arguments();
        if (!(args.get(0) instanceof Reference ref && args.get(1) instanceof Literal<?> literal)) {
            return null;
        }
        String storageIdentifier = ref.storageIdent();
        Object value = literal.value();
        if (value == null) {
            if (!ref.isNullable()) {
                // If the column is NOT NULL, `x IS DISTINCT FROM NULL` is true for all documents
                return new MatchAllDocsQuery();
            }
            return IsNullPredicate.refExistsQuery(ref, context);
        }

        DataType<?> dataType = ref.valueType();
        return switch (dataType.id()) {
            case ObjectType.ID -> EqOperator.refEqObject(
                    function,
                    ref.column(),
                    (ObjectType) dataType,
                    (Map<String, Object>) value,
                    context,
                    BooleanClause.Occur.MUST_NOT
                );
            case ArrayType.ID -> EqOperator.termsAndGenericFilter(
                function,
                storageIdentifier,
                dataType,
                (Collection<?>) value,
                context,
                ref.hasDocValues(),
                ref.indexType(),
                BooleanClause.Occur.MUST_NOT
            );
            default -> {
                var query = EqOperator.fromPrimitive(dataType, storageIdentifier, value, ref.hasDocValues(), ref.indexType());
                yield query == null ? null : Queries.not(query);
            }
        };
    }

}
