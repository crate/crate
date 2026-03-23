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

package io.crate.expression.tablefunctions;

import static io.crate.expression.RegexpFlags.isGlobal;
import static io.crate.expression.RegexpFlags.parseFlags;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.elasticsearch.common.settings.Settings;

import io.crate.data.Input;
import io.crate.data.Row;
import io.crate.data.RowN;
import io.crate.expression.RegexpFlags;
import io.crate.expression.symbol.Symbol;
import io.crate.legacy.LegacySettings;
import io.crate.metadata.FunctionType;
import io.crate.metadata.Functions;
import io.crate.metadata.NodeContext;
import io.crate.metadata.Scalar;
import io.crate.metadata.TransactionContext;
import io.crate.metadata.functions.BoundSignature;
import io.crate.metadata.functions.Signature;
import io.crate.metadata.tablefunctions.TableFunctionImplementation;
import io.crate.role.Roles;
import io.crate.types.DataTypes;
import io.crate.types.RowType;

public final class MatchesFunction extends TableFunctionImplementation<String> {

    public static final String NAME = "regexp_matches";
    private static final RowType ROW_TYPE = new RowType(List.of(DataTypes.STRING_ARRAY), List.of(NAME));
    private static final RowType LEGACY_ROW_TYPE = new RowType(List.of(DataTypes.STRING_ARRAY), List.of("groups"));


    public static void register(Functions.Builder builder, Settings settings) {
        final RowType returnType =
            LegacySettings.LEGACY_TABLE_FUNCTION_COLUMN_NAMING.get(settings) ? LEGACY_ROW_TYPE : ROW_TYPE;

        builder.add(
            Signature.builder(NAME, FunctionType.TABLE)
                .argumentTypes(
                    DataTypes.STRING.getTypeSignature(),
                    DataTypes.STRING.getTypeSignature())
                .returnType(DataTypes.STRING_ARRAY.getTypeSignature())
                .features(Feature.DETERMINISTIC, Feature.NOTNULL)
                .build(),
            (signature, boundSignature) -> new MatchesFunction(
                signature,
                boundSignature,
                returnType
            )
        );
        builder.add(
            Signature.builder(NAME, FunctionType.TABLE)
                .argumentTypes(
                    DataTypes.STRING.getTypeSignature(),
                    DataTypes.STRING.getTypeSignature(),
                    DataTypes.STRING.getTypeSignature())
                .returnType(DataTypes.STRING_ARRAY.getTypeSignature())
                .features(Feature.DETERMINISTIC, Feature.NOTNULL)
                .build(),
            (signature, boundSignature) -> new MatchesFunction(
                signature,
                boundSignature,
                returnType
            )
        );
    }

    private final RowType returnType;

    private MatchesFunction(Signature signature, BoundSignature boundSignature, RowType returnType) {
        super(signature, boundSignature);
        this.returnType = returnType;
    }

    static class CompiledMatchesFunction extends TableFunctionImplementation<String> {

        private final Matcher matcher;
        private final RowType returnType;
        private final boolean isGlobal;

        private CompiledMatchesFunction(Signature signature,
                                        BoundSignature boundSignature,
                                        RowType returnType,
                                        String pattern,
                                        String flags) {
            super(signature, boundSignature);
            this.returnType = returnType;
            this.isGlobal = RegexpFlags.isGlobal(flags);
            this.matcher = Pattern.compile(pattern, RegexpFlags.parseFlags(flags)).matcher("");
        }

        @Override
        @SafeVarargs
        public final Iterable<Row> evaluate(TransactionContext txnCtx, NodeContext nodeContext, Input<String>... args) {
            String value = args[0].value();
            if (value == null) {
                return List.of();
            }
            matcher.reset(value);
            return matchesToRows(matcher, isGlobal);
        }

        @Override
        public RowType returnType() {
            return returnType;
        }

        @Override
        public boolean hasLazyResultSet() {
            return false;
        }
    }

    @Override
    public RowType returnType() {
        return returnType;
    }

    @Override
    public boolean hasLazyResultSet() {
        return false;
    }

    @Override
    public Scalar<Iterable<Row>, String> compile(List<Symbol> arguments, String currentUser, Roles roles) {
        assert arguments.size() > 1 : "number of arguments must be > 1";
        String pattern;
        Symbol arg1 = arguments.get(1);
        if (arg1 instanceof Input<?> patternInput) {
            pattern = (String) patternInput.value();
            if (pattern == null) {
                return this;
            }
        } else {
            return this;
        }
        String flags = null;
        if (arguments.size() == 3) {
            Symbol arg2 = arguments.get(2);
            if (arg2 instanceof Input<?> flagsInput) {
                flags = (String) flagsInput.value();
            } else {
                return this;
            }
        }
        return new CompiledMatchesFunction(
            signature,
            boundSignature,
            returnType,
            pattern,
            flags
        );
    }

    @Override
    @SafeVarargs
    public final Iterable<Row> evaluate(TransactionContext txnCtx, NodeContext nodeCtx, Input<String> ... args) {
        assert args.length == 2 || args.length == 3 : "number of args must be 2 or 3";

        String value = args[0].value();
        if (value == null) {
            return List.of();
        }
        String patternStr = args[1].value();
        if (patternStr == null) {
            return List.of();
        }
        String flags = null;
        if (args.length == 3) {
            flags = args[2].value();
        }

        Pattern pattern = Pattern.compile(patternStr, parseFlags(flags));
        Matcher matcher = pattern.matcher(value);
        return matchesToRows(matcher, isGlobal(flags));
    }

    private static Iterable<Row> matchesToRows(Matcher matcher, boolean global) {
        if (!matcher.find()) {
            return List.of();
        }
        return () -> new Iterator<>() {
            private final Object[] columns = new Object[]{null};
            private final RowN row = new RowN(columns);
            private boolean hasNext = true;

            @Override
            public boolean hasNext() {
                return hasNext;
            }

            @Override
            public Row next() {
                if (!hasNext) {
                    throw new NoSuchElementException("no more rows");
                }
                columns[0] = groups(matcher);
                hasNext = global && matcher.find();
                return row;
            }
        };
    }

    private static List<String> groups(Matcher matcher) {
        int groupCount = matcher.groupCount();
        if (groupCount == 0) {
            return List.of(matcher.group());
        }
        ArrayList<String> groups = new ArrayList<>(groupCount);
        for (int i = 1; i <= groupCount; i++) {
            groups.add(matcher.group(i));
        }
        return groups;
    }
}
