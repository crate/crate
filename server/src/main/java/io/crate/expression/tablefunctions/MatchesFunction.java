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

import io.crate.common.annotations.VisibleForTesting;
import io.crate.data.Input;
import io.crate.data.Row;
import io.crate.data.RowN;
import io.crate.expression.symbol.Literal;
import io.crate.expression.symbol.Symbol;
import io.crate.expression.symbol.SymbolType;
import io.crate.metadata.NodeContext;
import io.crate.metadata.Scalar;
import io.crate.metadata.TransactionContext;
import io.crate.metadata.functions.Signature;
import io.crate.metadata.tablefunctions.TableFunctionImplementation;
import io.crate.types.DataTypes;
import io.crate.types.RowType;

import javax.annotation.Nullable;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static io.crate.expression.RegexpFlags.parseFlags;
import static io.crate.expression.RegexpFlags.isGlobal;

public final class MatchesFunction extends TableFunctionImplementation<List<Object>> {

    public static final String NAME = "regexp_matches";
    private static final RowType ROW_TYPE = new RowType(
        List.of(DataTypes.STRING_ARRAY), List.of("groups"));

    public static void register(TableFunctionModule module) {
        module.register(
            Signature.table(
                NAME,
                DataTypes.STRING.getTypeSignature(),
                DataTypes.STRING.getTypeSignature(),
                DataTypes.STRING_ARRAY.getTypeSignature()
            ),
            MatchesFunction::new
        );
        module.register(
            Signature.table(
                NAME,
                DataTypes.STRING.getTypeSignature(),
                DataTypes.STRING.getTypeSignature(),
                DataTypes.STRING.getTypeSignature(),
                DataTypes.STRING_ARRAY.getTypeSignature()
            ),
            MatchesFunction::new
        );
    }

    @Nullable
    private final Pattern pattern;
    private final Signature signature;
    private final Signature boundSignature;

    private MatchesFunction(Signature signature, Signature boundSignature) {
        this(signature, boundSignature, null);
    }

    private MatchesFunction(Signature signature, Signature boundSignature, @Nullable Pattern pattern) {
        this.signature = signature;
        this.boundSignature = boundSignature;
        this.pattern = pattern;
    }

    @Override
    public Signature signature() {
        return signature;
    }

    @Override
    public Signature boundSignature() {
        return boundSignature;
    }

    @Override
    public RowType returnType() {
        return ROW_TYPE;
    }

    @Override
    public boolean hasLazyResultSet() {
        return false;
    }

    @VisibleForTesting
    Pattern pattern() {
        return pattern;
    }

    @Override
    public Scalar<Iterable<Row>, List<Object>> compile(List<Symbol> arguments) {
        assert arguments.size() > 1 : "number of arguments must be > 1";
        String pattern = null;
        if (arguments.get(1).symbolType() == SymbolType.LITERAL) {
            Literal<String> literal = (Literal<String>) arguments.get(1);
            pattern = literal.value();
            if (pattern == null) {
                return this;
            }
        }
        String flags = null;
        if (arguments.size() == 3) {
            assert arguments.get(2).symbolType() == SymbolType.LITERAL :
                "3rd argument must be a " + SymbolType.LITERAL;
            flags = ((Literal<String>) arguments.get(2)).value();
        }
        if (pattern != null) {
            return new MatchesFunction(
                signature, boundSignature, Pattern.compile(pattern, parseFlags(flags)));
        }
        return this;
    }

    @Override
    public Iterable<Row> evaluate(TransactionContext txnCtx, NodeContext nodeCtx, Input[] args) {
        assert args.length == 2 || args.length == 3 : "number of args must be 2 or 3";

        String value = (String) args[0].value();
        String pattern = (String) args[1].value();
        if (value == null || pattern == null) {
            return List.of();
        }
        String flags = null;
        if (args.length == 3) {
            flags = (String) args[2].value();
        }

        Pattern matchPattern;
        if (this.pattern == null) {
            matchPattern = Pattern.compile(pattern, parseFlags(flags));
        } else {
            matchPattern = this.pattern;
        }
        Matcher matcher = matchPattern.matcher(value);
        List<List<String>> rowGroups;
        if (isGlobal(flags)) {
            List<String> groups = groups(matcher);
            if (groups != null) {
                rowGroups = new ArrayList<>();
                while (groups != null) {
                    rowGroups.add(groups);
                    groups = groups(matcher);
                }
            } else {
                rowGroups = List.of();
            }
        } else {
            List<String> groups = groups(matcher);
            rowGroups = groups == null ? List.of() : List.of(groups);
        }
        return () -> new Iterator<>() {
            final Object[] columns = new Object[]{null};
            final RowN row = new RowN(columns);
            int idx = 0;

            @Override
            public boolean hasNext() {
                return idx < rowGroups.size();
            }

            @Override
            public Row next() {
                if (!hasNext()) {
                    throw new NoSuchElementException("no more rows");
                }
                columns[0] = rowGroups.get(idx++);
                return row;
            }
        };
    }

    private static List<String> groups(Matcher matcher) {
        if (!matcher.find()) {
            return null;
        }
        int groupCount = matcher.groupCount();
        if (groupCount == 0) {
            return List.of(matcher.group());
        }
        List<String> groups = new ArrayList<>(groupCount);
        for (int i = 1; i <= groupCount; i++) {
            groups.add(matcher.group(i));
        }
        return groups;
    }
}
