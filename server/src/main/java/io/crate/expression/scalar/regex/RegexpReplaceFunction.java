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

package io.crate.expression.scalar.regex;

import static io.crate.expression.RegexpFlags.isGlobal;
import static io.crate.expression.RegexpFlags.parseFlags;

import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.jetbrains.annotations.Nullable;

import io.crate.data.Input;
import io.crate.expression.scalar.ScalarFunctions;
import io.crate.expression.symbol.Function;
import io.crate.expression.symbol.Symbol;
import io.crate.metadata.NodeContext;
import io.crate.metadata.Scalar;
import io.crate.metadata.TransactionContext;
import io.crate.metadata.functions.BoundSignature;
import io.crate.metadata.functions.Signature;
import io.crate.role.Roles;
import io.crate.types.DataTypes;

public final class RegexpReplaceFunction extends Scalar<String, String> {

    public static final String NAME = "regexp_replace";

    public static void register(ScalarFunctions module) {
        module.register(
            Signature.scalar(
                NAME,
                DataTypes.STRING.getTypeSignature(),
                DataTypes.STRING.getTypeSignature(),
                DataTypes.STRING.getTypeSignature(),
                DataTypes.STRING.getTypeSignature()
            ),
            RegexpReplaceFunction::new
        );
        module.register(
            Signature.scalar(
                NAME,
                DataTypes.STRING.getTypeSignature(),
                DataTypes.STRING.getTypeSignature(),
                DataTypes.STRING.getTypeSignature(),
                DataTypes.STRING.getTypeSignature(),
                DataTypes.STRING.getTypeSignature()
            ),
            RegexpReplaceFunction::new
        );
    }

    @Nullable
    private final Pattern pattern;

    private RegexpReplaceFunction(Signature signature, BoundSignature boundSignature) {
        this(signature, boundSignature, null);
    }

    private RegexpReplaceFunction(Signature signature, BoundSignature boundSignature, @Nullable Pattern pattern) {
        super(signature, boundSignature);
        this.pattern = pattern;
    }

    @Override
    public Symbol normalizeSymbol(Function symbol, TransactionContext txnCtx, NodeContext nodeCtx) {
        return evaluateIfLiterals(this, txnCtx, nodeCtx, symbol);
    }

    @Override
    public Scalar<String, String> compile(List<Symbol> arguments, String currentUser, Roles roles) {
        assert arguments.size() >= 3 : "number of arguments muts be >= 3";
        Symbol patternSymbol = arguments.get(1);
        if (patternSymbol instanceof Input) {
            String pattern = (String) ((Input) patternSymbol).value();
            if (pattern == null) {
                return this;
            }
            if (arguments.size() == 4) {
                Symbol flagsSymbol = arguments.get(3);
                if (flagsSymbol instanceof Input) {
                    String flags = (String) ((Input) flagsSymbol).value();
                    return new RegexpReplaceFunction(
                        signature, boundSignature, Pattern.compile(pattern, parseFlags(flags)));
                }
            }
        }
        return this;
    }

    @Override
    public String evaluate(TransactionContext txnCtx, NodeContext nodeCtx, Input<String>[] args) {
        assert args.length == 3 || args.length == 4 : "number of args must be 3 or 4";
        String value = args[0].value();
        String pattern = args[1].value();
        String replacement = args[2].value();
        if (value == null || pattern == null || replacement == null) {
            return null;
        }
        String flags = null;
        if (args.length == 4) {
            flags = args[3].value();
        }
        Pattern replacePattern;
        if (this.pattern == null) {
            replacePattern = Pattern.compile(pattern, parseFlags(flags));
        } else {
            replacePattern = this.pattern;
        }
        Matcher m = replacePattern.matcher(value);
        return isGlobal(flags) ?
            m.replaceAll(replacement)
            :
            m.replaceFirst(replacement);
    }
}
