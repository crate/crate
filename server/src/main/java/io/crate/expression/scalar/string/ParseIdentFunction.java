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

import java.util.List;

import io.crate.common.annotations.VisibleForTesting;
import io.crate.data.Input;
import io.crate.metadata.FunctionName;
import io.crate.metadata.FunctionType;
import io.crate.metadata.Functions;
import io.crate.metadata.NodeContext;
import io.crate.metadata.Scalar;
import io.crate.metadata.TransactionContext;
import io.crate.metadata.functions.BoundSignature;
import io.crate.metadata.functions.Signature;
import io.crate.metadata.pgcatalog.PgCatalogSchemaInfo;
import io.crate.sql.parser.ParsingException;
import io.crate.sql.parser.SqlParser;
import io.crate.sql.tree.Expression;
import io.crate.sql.tree.FunctionCall;
import io.crate.sql.tree.QualifiedNameReference;
import io.crate.types.DataTypes;

/**
 * PostgreSQL-compatible {@code parse_ident} function.
 * <p>
 * Splits a qualified SQL identifier string into an array of identifiers,
 * removing any quoting of individual identifiers. Unquoted identifiers
 * are case-folded to lowercase.
 * <p>
 * Signature: {@code parse_ident(text [, boolean DEFAULT true]) → text[]}
 */
public class ParseIdentFunction extends Scalar<List<String>, Object> {

    private static final FunctionName FQNAME = new FunctionName(PgCatalogSchemaInfo.NAME, "parse_ident");

    public static void register(Functions.Builder module) {
        // parse_ident(text) → text[]
        module.add(
            Signature.builder(FQNAME, FunctionType.SCALAR)
                .argumentTypes(DataTypes.STRING.getTypeSignature())
                .returnType(DataTypes.STRING_ARRAY.getTypeSignature())
                .features(Feature.DETERMINISTIC, Feature.STRICTNULL)
                .build(),
            ParseIdentFunction::new
        );
        // parse_ident(text, boolean) → text[]
        module.add(
            Signature.builder(FQNAME, FunctionType.SCALAR)
                .argumentTypes(DataTypes.STRING.getTypeSignature(), DataTypes.BOOLEAN.getTypeSignature())
                .returnType(DataTypes.STRING_ARRAY.getTypeSignature())
                .features(Feature.DETERMINISTIC, Feature.STRICTNULL)
                .build(),
            ParseIdentFunction::new
        );
    }

    public ParseIdentFunction(Signature signature, BoundSignature boundSignature) {
        super(signature, boundSignature);
    }

    @Override
    @SafeVarargs
    public final List<String> evaluate(TransactionContext txnCtx, NodeContext nodeCtx, Input<Object>... args) {
        assert args.length == 1 || args.length == 2 : "number of args must be 1 or 2";

        String input = (String) args[0].value();
        if (input == null) {
            return null;
        }

        boolean strict = true;
        if (args.length == 2) {
            Boolean strictArg = (Boolean) args[1].value();
            if (strictArg == null) {
                return null;
            }
            strict = strictArg;
        }

        return parseIdent(input, strict);
    }

    @VisibleForTesting
    static List<String> parseIdent(String input, boolean strict) {
        Expression expression;
        try {
            expression = SqlParser.createExpression(input);
        } catch (ParsingException e) {
            if (!strict) {
                return tryParseLeadingIdent(input);
            }
            throw new IllegalArgumentException(
                "String is not a valid identifier: \"" + input + "\"", e);
        }

        if (expression instanceof QualifiedNameReference ref) {
            List<String> parts = ref.getName().getParts();
            for (String part : parts) {
                if (part.isEmpty()) {
                    throw new IllegalArgumentException(
                        "String is not a valid identifier: \"" + input + "\"");
                }
            }
            return parts;
        }

        if (!strict) {
            if (expression instanceof FunctionCall func) {
                return func.getName().getParts();
            }
            return tryParseLeadingIdent(input);
        }

        throw new IllegalArgumentException(
            "String is not a valid identifier: \"" + input + "\"");
    }

    private static List<String> tryParseLeadingIdent(String input) {
        try {
            return SqlParser.parseLeadingQualifiedName(input);
        } catch (ParsingException e) {
            throw new IllegalArgumentException(
                "String is not a valid identifier: \"" + input + "\"", e);
        }
    }
}
