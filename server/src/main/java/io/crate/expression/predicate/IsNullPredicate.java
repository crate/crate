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

package io.crate.expression.predicate;

import static io.crate.lucene.LuceneQueryBuilder.genericFunctionFilter;
import static io.crate.metadata.functions.TypeVariableConstraint.typeVariable;

import java.util.Collections;
import java.util.List;

import org.apache.lucene.index.Term;
import org.apache.lucene.search.BooleanClause.Occur;
import org.apache.lucene.search.BooleanQuery;
import org.apache.lucene.search.ConstantScoreQuery;
import org.apache.lucene.search.FieldExistsQuery;
import org.apache.lucene.search.MatchNoDocsQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.TermQuery;
import org.elasticsearch.common.lucene.search.Queries;
import org.elasticsearch.index.mapper.FieldNamesFieldMapper;
import org.elasticsearch.index.mapper.MappedFieldType;
import org.elasticsearch.index.mapper.MapperService;
import org.jetbrains.annotations.Nullable;

import io.crate.data.Input;
import io.crate.expression.symbol.DynamicReference;
import io.crate.expression.symbol.Function;
import io.crate.expression.symbol.Literal;
import io.crate.expression.symbol.Symbol;
import io.crate.lucene.LuceneQueryBuilder.Context;
import io.crate.metadata.NodeContext;
import io.crate.metadata.Reference;
import io.crate.metadata.Scalar;
import io.crate.metadata.TransactionContext;
import io.crate.metadata.functions.BoundSignature;
import io.crate.metadata.functions.Signature;
import io.crate.sql.tree.ColumnPolicy;
import io.crate.types.ArrayType;
import io.crate.types.DataType;
import io.crate.types.DataTypes;
import io.crate.types.ObjectType;
import io.crate.types.StorageSupport;
import io.crate.types.TypeSignature;

public class IsNullPredicate<T> extends Scalar<Boolean, T> {

    public static final String NAME = "op_isnull";
    public static final Signature SIGNATURE = Signature.scalar(
            NAME,
            TypeSignature.parse("E"),
            DataTypes.BOOLEAN.getTypeSignature()
        ).withFeature(Feature.NON_NULLABLE)
        .withTypeVariableConstraints(typeVariable("E"));

    public static void register(PredicateModule module) {
        module.register(
            SIGNATURE,
            IsNullPredicate::new
        );
    }

    private IsNullPredicate(Signature signature, BoundSignature boundSignature) {
        super(signature, boundSignature);
    }

    @Override
    public Symbol normalizeSymbol(Function symbol, TransactionContext txnCtx, NodeContext nodeCtx) {
        assert symbol != null : "function must not be null";
        assert symbol.arguments().size() == 1 : "function's number of arguments must be 1";
        Symbol arg = symbol.arguments().get(0);
        if (arg instanceof Input<?> input) {
            return Literal.of(input.value() == null);
        }
        if (arg instanceof Reference ref && !ref.isNullable()) {
            return Literal.of(false);
        }
        return symbol;
    }

    @Override
    @SafeVarargs
    public final Boolean evaluate(TransactionContext txnCtx, NodeContext nodeCtx, Input<T> ... args) {
        assert args.length == 1 : "number of args must be 1";
        return args[0].value() == null;
    }

    @Override
    public Query toQuery(Function function, Context context) {
        List<Symbol> arguments = function.arguments();
        assert arguments.size() == 1 : "`<expression> IS NULL` function must have one argument";
        if (arguments.get(0) instanceof Reference ref) {
            Query refExistsQuery = refExistsQuery(ref, context, true);
            return refExistsQuery == null ? null : Queries.not(refExistsQuery);
        }
        return null;
    }


    @Nullable
    public static Query refExistsQuery(Reference ref, Context context, boolean countEmptyArrays) {
        String field = ref.storageIdent();
        MapperService mapperService = context.queryShardContext().getMapperService();
        MappedFieldType mappedFieldType = mapperService.fieldType(field);
        DataType<?> valueType = ref.valueType();
        boolean canUseFieldsExist = ref.hasDocValues() || (mappedFieldType != null && mappedFieldType.hasNorms());
        if (valueType instanceof ArrayType<?>) {
            if (countEmptyArrays) {
                if (canUseFieldsExist) {
                    return new BooleanQuery.Builder()
                        .setMinimumNumberShouldMatch(1)
                        .add(new FieldExistsQuery(field), Occur.SHOULD)
                        .add(Queries.not(isNullFuncToQuery(ref, context)), Occur.SHOULD)
                        .build();
                } else {
                    return null;
                }
            }
            // An empty array has no dimension, array_length([]) = NULL, thus we don't count [] as existing.
            valueType = ArrayType.unnest(valueType);
        }
        StorageSupport<?> storageSupport = valueType.storageSupport();
        if (ref instanceof DynamicReference) {
            if (ref.columnPolicy() == ColumnPolicy.IGNORED) {
                // Not indexed, need to use source lookup
                return null;
            }
            return new MatchNoDocsQuery("DynamicReference/type without storageSupport does not exist");
        } else if (canUseFieldsExist) {
            return new FieldExistsQuery(field);
        } else if (ref.columnPolicy() == ColumnPolicy.IGNORED) {
            // Not indexed, need to use source lookup
            return null;
        } else if (storageSupport != null) {
            if (valueType instanceof ObjectType objType) {
                if (objType.innerTypes().isEmpty()) {
                    return null;
                }
                BooleanQuery.Builder booleanQuery = new BooleanQuery.Builder()
                    .setMinimumNumberShouldMatch(1);
                for (var entry : objType.innerTypes().entrySet()) {
                    String childColumn = entry.getKey();
                    Reference childRef = context.getRef(ref.column().getChild(childColumn));
                    if (childRef == null) {
                        return null;
                    }
                    Query refExistsQuery = refExistsQuery(childRef, context, true);
                    if (refExistsQuery == null) {
                        return null;
                    }
                    booleanQuery.add(refExistsQuery, Occur.SHOULD);
                }
                return booleanQuery
                    // Even if a child columns exist, an object can have empty values. Example:
                    //  CREATE TABLE t (obj OBJECT as (x int));
                    //  INSERT INTO t (obj) VALUES ({});
                    .add(Queries.not(isNullFuncToQuery(ref, context)), Occur.SHOULD)
                    .build();
            }
            if (mappedFieldType == null || !mappedFieldType.isSearchable()) {
                return null;
            } else {
                return new ConstantScoreQuery(new TermQuery(new Term(FieldNamesFieldMapper.NAME, field)));
            }
        } else {
            return null;
        }
    }

    static Query isNullFuncToQuery(Symbol arg, Context context) {
        Function isNullFunction = new Function(
            IsNullPredicate.SIGNATURE,
            Collections.singletonList(arg),
            DataTypes.BOOLEAN
        );
        return genericFunctionFilter(isNullFunction, context);
    }

}
