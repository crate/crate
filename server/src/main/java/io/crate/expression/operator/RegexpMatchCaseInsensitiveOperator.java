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

import java.util.regex.Pattern;

import org.apache.lucene.index.Term;
import org.apache.lucene.search.Query;

import io.crate.data.Input;
import io.crate.expression.symbol.Literal;
import io.crate.lucene.match.CrateRegexQuery;
import io.crate.metadata.Functions;
import io.crate.metadata.NodeContext;
import io.crate.metadata.Reference;
import io.crate.metadata.TransactionContext;
import io.crate.metadata.functions.BoundSignature;
import io.crate.metadata.functions.Signature;
import io.crate.types.DataTypes;


public class RegexpMatchCaseInsensitiveOperator extends Operator<String> {

    public static final String NAME = "op_~*";

    public static void register(Functions.Builder builder) {
        builder.add(
            Signature.scalar(
                NAME,
                Feature.CONDITIONAL,
                DataTypes.STRING.getTypeSignature(),
                DataTypes.STRING.getTypeSignature(),
                Operator.RETURN_TYPE.getTypeSignature()
            ).withFeature(Feature.DETERMINISTIC),
            RegexpMatchCaseInsensitiveOperator::new
        );
    }

    public RegexpMatchCaseInsensitiveOperator(Signature signature, BoundSignature boundSignature) {
        super(signature, boundSignature);
    }

    @Override
    public Boolean evaluate(TransactionContext txnCtx, NodeContext nodeCtx, Input<String>[] args) {
        assert args.length == 2 : "invalid number of arguments";
        String source = args[0].value();
        if (source == null) {
            return null;
        }
        String pattern = args[1].value();
        if (pattern == null) {
            return null;
        }

        Pattern p = Pattern.compile(pattern, Pattern.CASE_INSENSITIVE | Pattern.UNICODE_CASE);
        return p.matcher(source).matches();
    }

    @Override
    public Query toQuery(Reference ref, Literal<?> literal) {
        String pattern = (String) literal.value();
        return new CrateRegexQuery(
            new Term(ref.storageIdent(), pattern),
            Pattern.CASE_INSENSITIVE | Pattern.UNICODE_CASE
        );
    }
}
