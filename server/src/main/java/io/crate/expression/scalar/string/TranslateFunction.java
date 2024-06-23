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

package io.crate.expression.scalar.string;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import io.crate.data.Input;
import io.crate.expression.symbol.Symbol;
import io.crate.metadata.Functions;
import io.crate.metadata.NodeContext;
import io.crate.metadata.Scalar;
import io.crate.metadata.TransactionContext;
import io.crate.metadata.functions.BoundSignature;
import io.crate.metadata.functions.Signature;
import io.crate.role.Roles;
import io.crate.types.DataTypes;

public class TranslateFunction extends Scalar<String, String> {

    private static final Character NULL = '\0';

    public static void register(Functions.Builder module) {
        module.add(
            Signature.scalar(
                    "translate",
                    Feature.CONDITIONAL,
                    DataTypes.STRING.getTypeSignature(),
                    DataTypes.STRING.getTypeSignature(),
                    DataTypes.STRING.getTypeSignature(),
                    DataTypes.STRING.getTypeSignature())
                .withFeature(Feature.DETERMINISTIC),
            TranslateFunction::new
        );
    }

    private final Map<Character, Character> tmap;

    private TranslateFunction(Signature signature, BoundSignature boundSignature) {
        this(signature, boundSignature, null);
    }

    public TranslateFunction(Signature signature, BoundSignature boundSignature, Map<Character, Character> tmap) {
        super(signature, boundSignature);
        this.tmap = tmap;
    }

    @Override
    public Scalar<String, String> compile(List<Symbol> args, String currentUser, Roles roles) {
        assert args.size() == 3 : "translate takes exactly three arguments";

        Symbol from = args.get(1);
        if (!Symbol.isLiteral(from, DataTypes.STRING)) {
            return this;
        }
        Symbol to = args.get(2);
        if (!Symbol.isLiteral(to, DataTypes.STRING)) {
            return this;
        }
        String fromStr = ((Input<String>) from).value();
        String toStr = ((Input<String>) to).value();
        return new TranslateFunction(signature, boundSignature, computeMap(fromStr, toStr));
    }

    @Override
    public String evaluate(TransactionContext txnCtx, NodeContext nodeCtx, Input<String>[] args) {
        assert args.length == 3 : "translate takes exactly three arguments";
        String source = args[0].value();
        String from = args[1].value();
        String to = args[2].value();
        if (source == null || from == null || to == null) {
            return null;
        }
        if (source.length() == 0 || from.length() == 0) {
            return source;
        }
        return translate(this.tmap != null ? this.tmap : computeMap(from, to), source);
    }

    private static String translate(Map<Character, Character> tmap, String source) {
        int sourceLen = source.length();
        char[] result = new char[sourceLen];
        int resultCount = 0;
        for (int i = 0; i < sourceLen; i++) {
            char c = source.charAt(i);
            var mc = tmap.get(c);
            if (mc == null) {
                result[resultCount++] = c;
            } else if (mc != NULL) {
                result[resultCount++] = mc;
            }
        }
        return String.valueOf(result, 0, resultCount);
    }

    private static Map<Character, Character> computeMap(String from, String to) {
        Map<Character, Character> tmap = new HashMap<>();
        for (int i = 0; i < from.length(); i++) {
            char c = from.charAt(i);
            tmap.putIfAbsent(c, i < to.length() ? to.charAt(i) : NULL);
        }
        return tmap;
    }
}
