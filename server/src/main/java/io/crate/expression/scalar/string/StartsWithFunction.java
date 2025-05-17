/*
 * Licensed to Crate.io GmbH ("Crate") under one or more contributor
 * license agreements.  See the NOTICE file distributed with this work for
 * additional information regarding copyright ownership.  Crate licenses
 * this file to you under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.  You may
 * obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
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

package io.crate.expression.scalar.string;

import io.crate.data.Input;
import io.crate.expression.symbol.Function;
import io.crate.expression.symbol.Literal;
import io.crate.expression.symbol.Symbol;
import io.crate.lucene.LuceneQueryBuilder;
import io.crate.metadata.FunctionType;
import io.crate.metadata.Functions;
import io.crate.metadata.IndexType;
import io.crate.metadata.NodeContext;
import io.crate.metadata.Reference;
import io.crate.metadata.Scalar;
import io.crate.metadata.TransactionContext;
import io.crate.metadata.functions.BoundSignature;
import io.crate.metadata.functions.Signature;
import io.crate.types.DataTypes;
import io.crate.types.EqQuery;
import io.crate.types.StorageSupport;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.PrefixQuery;
import org.apache.lucene.search.Query;

import java.util.List;

public final class StartsWithFunction extends Scalar<Boolean, String> {

    public static void register(Functions.Builder module) {
        module.add(
            Signature.builder("starts_with", FunctionType.SCALAR)
                .argumentTypes(DataTypes.STRING.getTypeSignature(), DataTypes.STRING.getTypeSignature())
                .returnType(DataTypes.BOOLEAN.getTypeSignature())
                .features(Feature.DETERMINISTIC, Feature.STRICTNULL)
                .build(),
            StartsWithFunction::new
        );
    }

    public StartsWithFunction(Signature signature, BoundSignature boundSignature) {
        super(signature, boundSignature);
    }

    @Override
    public Boolean evaluate(TransactionContext txnCtx, NodeContext nodeCtx, Input<String>[] args) {
        assert args.length == 2 : "starts_with takes exactly two arguments";
        var text = args[0].value();
        var prefix = args[1].value();
        if (text == null || prefix == null) {
            return null;
        }

        return text.startsWith(prefix);
    }

    @Override
    public Query toQuery(Function function, LuceneQueryBuilder.Context context) {
        List<Symbol> args = function.arguments();
        if (args.get(0) instanceof Reference ref
            && args.get(1) instanceof Literal<?> prefixLiteral
            && ref.indexType() != IndexType.NONE
        ) {
            Object value = prefixLiteral.value();
            assert value instanceof String
                : "StartsWithFunction is registered for string types. Value must be a string";
            if (((String) value).isEmpty()) {
                StorageSupport<?> storageSupport = ref.valueType().storageSupport();
                EqQuery<?> eqQuery = storageSupport == null ? null : storageSupport.eqQuery();
                if (eqQuery == null) {
                    return null;
                }
                return ((EqQuery<Object>) eqQuery).termQuery(
                    ref.storageIdent(), value, ref.hasDocValues(), ref.indexType() != IndexType.NONE);
            }
            return new PrefixQuery(new Term(ref.storageIdent(), (String) value));
        }
        return null;
    }
}
