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

import static io.crate.expression.scalar.SubscriptObjectFunction.tryToInferReturnTypeFromObjectTypeAndArguments;
import static io.crate.metadata.functions.TypeVariableConstraint.typeVariable;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.apache.lucene.search.BooleanClause;
import org.apache.lucene.search.BooleanQuery;
import org.apache.lucene.search.MatchNoDocsQuery;
import org.apache.lucene.search.Query;
import org.elasticsearch.common.TriFunction;
import org.elasticsearch.index.mapper.MappedFieldType;

import io.crate.data.Input;
import io.crate.exceptions.ColumnUnknownException;
import io.crate.expression.operator.EqOperator;
import io.crate.expression.operator.GtOperator;
import io.crate.expression.operator.GteOperator;
import io.crate.expression.operator.LtOperator;
import io.crate.expression.operator.LteOperator;
import io.crate.expression.symbol.Function;
import io.crate.expression.symbol.Literal;
import io.crate.expression.symbol.Symbol;
import io.crate.lucene.LuceneQueryBuilder;
import io.crate.lucene.LuceneQueryBuilder.Context;
import io.crate.metadata.IndexType;
import io.crate.metadata.NodeContext;
import io.crate.metadata.Reference;
import io.crate.metadata.Scalar;
import io.crate.metadata.TransactionContext;
import io.crate.metadata.functions.BoundSignature;
import io.crate.metadata.functions.Signature;
import io.crate.types.ArrayType;
import io.crate.types.DataType;
import io.crate.types.DataTypes;
import io.crate.types.EqQuery;
import io.crate.types.ObjectType;
import io.crate.types.StorageSupport;
import io.crate.types.TypeSignature;

/**
 * Supported subscript expressions:
 * <ul>
 *     <li>obj['x']</li>
 *     <li>arr[1]</li>
 *     <li>obj_array[1]</li>
 *     <li>obj_array['x']</li>
 * </ul>
 **/
public class SubscriptFunction extends Scalar<Object, Object[]> {

    public static final String NAME = "subscript";

    public static void register(ScalarFunctionModule module) {
        // All signatures but `array[int]` must forbid coercion.
        // Otherwise they would also match for non-int numeric indices like e.g. `array[long]`

        // subscript(array(object)), text) -> array(undefined)
        module.register(
            Signature.scalar(
                NAME,
                TypeSignature.parse("array(object)"),
                DataTypes.STRING.getTypeSignature(),
                TypeSignature.parse("array(undefined)")
            ).withForbiddenCoercion(),
            (signature, boundSignature) ->
                new SubscriptFunction(
                    signature,
                    boundSignature,
                    SubscriptFunction::lookupIntoListObjectsByName
                )
        );
        // subscript(array(any)), integer) -> any
        module.register(
            Signature
                .scalar(
                    NAME,
                    TypeSignature.parse("array(E)"),
                    DataTypes.INTEGER.getTypeSignature(),
                    TypeSignature.parse("E"))
                .withTypeVariableConstraints(typeVariable("E")),
            (signature, boundSignature) ->
                new SubscriptFunction(
                    signature,
                    boundSignature,
                    SubscriptFunction::lookupByNumericIndex
                )
        );
        // subscript(object(text, element), text) -> undefined
        module.register(
            Signature.scalar(
                NAME,
                DataTypes.UNTYPED_OBJECT.getTypeSignature(),
                DataTypes.STRING.getTypeSignature(),
                DataTypes.UNDEFINED.getTypeSignature()
            ).withForbiddenCoercion(),
            (signature, boundSignature) ->
                new SubscriptFunction(
                    signature,
                    boundSignature,
                    SubscriptFunction::lookupByName
                )
        );
        // subscript(undefined, text) -> undefined
        //
        // This signature is required for the cases when the look up
        // path is longer than 1, e.g. {\"x\" = { \"y\" = 'test'}}['x']['y'].
        // The first subscript function cannot infer the return type and always
        // returns the undefined type. Therefore, the second subscript function
        // must be resolved for the `subscript(undefined, text)` signature.
        module.register(
            Signature.scalar(
                NAME,
                DataTypes.UNDEFINED.getTypeSignature(),
                DataTypes.STRING.getTypeSignature(),
                DataTypes.UNDEFINED.getTypeSignature()
            ).withForbiddenCoercion(),
            (signature, boundSignature) ->
                new SubscriptFunction(
                    signature,
                    boundSignature,
                    SubscriptFunction::lookupByName
                )
        );
    }

    private final TriFunction<Object, Object, Boolean, Object> lookup;

    private SubscriptFunction(Signature signature,
                              BoundSignature boundSignature,
                              TriFunction<Object, Object, Boolean, Object> lookup) {
        super(signature, boundSignature);
        this.lookup = lookup;
    }

    @Override
    public Symbol normalizeSymbol(Function func, TransactionContext txnCtx, NodeContext nodeCtx) {
        Symbol result = evaluateIfLiterals(this, txnCtx, nodeCtx, func);
        if (result instanceof Literal) {
            return result;
        }
        if (func.arguments().get(0).valueType().id() == ObjectType.ID) {
            return tryToInferReturnTypeFromObjectTypeAndArguments(func);
        }
        return func;
    }

    @Override
    @SuppressWarnings("rawtypes")
    public Object evaluate(TransactionContext txnCtx, NodeContext nodeCtx, Input[] args) {
        assert args.length == 2 : "invalid number of arguments";
        Object element = args[0].value();
        Object index = args[1].value();
        if (element == null || index == null) {
            return null;
        }
        return lookup.apply(element, index, txnCtx.sessionSettings().errorOnUnknownObjectKey());
    }

    static Object lookupIntoListObjectsByName(Object base, Object name, boolean errorOnUnknownObjectKey) {
        List<?> values = (List<?>) base;
        List<Object> result = new ArrayList<>(values.size());
        for (int i = 0; i < values.size(); i++) {
            Map<?, ?> map = (Map<?, ?>) values.get(i);
            result.add(map.get(name));
        }
        return result;
    }

    static Object lookupByNumericIndex(Object base, Object index, boolean errorOnUnknownObjectKey) {
        List<?> values = (List<?>) base;
        int idx = ((Number) index).intValue();
        try {
            return values.get(idx - 1); // SQL array indices start with 1
        } catch (IndexOutOfBoundsException e) {
            return null;
        }
    }

    static Object lookupByName(Object base, Object name, boolean errorOnUnknownObjectKey) {
        if (!(base instanceof Map<?, ?> map)) {
            throw new IllegalArgumentException("Base argument to subscript must be an object, not " + base);
        }
        if (errorOnUnknownObjectKey && !map.containsKey(name)) {
            throw ColumnUnknownException.ofUnknownRelation("The object `" + base + "` does not contain the key `" + name + "`");
        }
        return map.get(name);
    }

    private interface PreFilterQueryBuilder {
        Query buildQuery(String field, EqQuery<Object> eqQuery, Object value, boolean hasDocValues, boolean isIndexed);
    }

    private static final Map<String, PreFilterQueryBuilder> PRE_FILTER_QUERY_BUILDER_BY_OP = Map.of(
        EqOperator.NAME, (field, eqQuery, value, haDocValues, isIndexed) ->
            eqQuery.termQuery(field, value, haDocValues, isIndexed),
        GteOperator.NAME, (field, eqQuery, value, hasDocValues, isIndexed) ->
                eqQuery.rangeQuery(field, value, null, true, false, hasDocValues, isIndexed),
        GtOperator.NAME, (field, eqQuery, value, hasDocValues, isIndexed) ->
                eqQuery.rangeQuery(field, value, null, false, false, hasDocValues, isIndexed),
        LteOperator.NAME, (field, eqQuery, value, hasDocValues, isIndexed) ->
                eqQuery.rangeQuery(field, null, value, false, true, hasDocValues, isIndexed),
        LtOperator.NAME, (field, eqQuery, value, hasDocValues, isIndexed) ->
                eqQuery.rangeQuery(field, null, value, false, false, hasDocValues, isIndexed)
    );

    @Override
    public Query toQuery(Function parent, Function inner, Context context) {
        // `subscript(ref, <keyLiteral>) [ = | > | >= | < | <= ] <cmpLiteral>`
        // ^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
        //       inner
        if (!(inner.arguments().get(0) instanceof Reference ref
              && inner.arguments().get(1) instanceof Literal<?>
              && parent.arguments().size() == 2 // parent could for example be `op_isnull`
              && parent.arguments().get(1) instanceof Literal<?> cmpLiteral)) {
            return null;
        }
        if (DataTypes.isArray(ref.valueType())) {
            PreFilterQueryBuilder preFilterQueryBuilder = PRE_FILTER_QUERY_BUILDER_BY_OP.get(parent.name());
            if (preFilterQueryBuilder == null) {
                return null;
            }
            MappedFieldType fieldType = context.getFieldTypeOrNull(ref.storageIdent());
            DataType<?> innerType = ArrayType.unnest(ref.valueType());
            if (fieldType == null) {
                if (innerType.id() == ObjectType.ID) {
                    return null; // fallback to generic query to enable objects[1] = {x=10}
                }
                return new MatchNoDocsQuery("column doesn't exist in this index");
            }
            StorageSupport<?> storageSupport = innerType.storageSupport();
            //noinspection unchecked
            EqQuery<Object> eqQuery = storageSupport == null ? null : (EqQuery<Object>) storageSupport.eqQuery();
            if (eqQuery == null) {
                return null;
            }
            BooleanQuery.Builder builder = new BooleanQuery.Builder();
            builder.add(
                preFilterQueryBuilder.buildQuery(ref.storageIdent(), eqQuery, cmpLiteral.value(), ref.hasDocValues(), ref.indexType() != IndexType.NONE),
                BooleanClause.Occur.MUST);
            builder.add(LuceneQueryBuilder.genericFunctionFilter(parent, context), BooleanClause.Occur.FILTER);
            return builder.build();
        }
        return null;
    }
}
