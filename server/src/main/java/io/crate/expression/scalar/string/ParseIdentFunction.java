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

import io.crate.expression.scalar.UnaryScalar;
import io.crate.metadata.FunctionName;
import io.crate.metadata.FunctionType;
import io.crate.metadata.Functions;
import io.crate.metadata.Scalar;
import io.crate.metadata.functions.Signature;
import io.crate.metadata.pgcatalog.PgCatalogSchemaInfo;
import io.crate.sql.parser.ParsingException;
import io.crate.sql.parser.SqlParser;
import io.crate.sql.tree.Expression;
import io.crate.sql.tree.QualifiedNameReference;
import io.crate.types.DataTypes;

/**
 * PostgreSQL compatible parse_ident(text) function.
 *
 * parse_ident parses a possibly qualified identifier and returns its components
 * as a text[].
 *
 * Used by clients like Grafana to split "schema.table" inputs.
 */
public final class ParseIdentFunction {

    private static final FunctionName FQNAME = new FunctionName(PgCatalogSchemaInfo.NAME, "parse_ident");

    public static void register(Functions.Builder module) {
        module.add(
            Signature.builder(FQNAME, FunctionType.SCALAR)
                .argumentTypes(DataTypes.STRING.getTypeSignature())
                .returnType(DataTypes.STRING_ARRAY.getTypeSignature())
                .features(Scalar.Feature.DETERMINISTIC, Scalar.Feature.STRICTNULL)
                .build(),
            (signature, boundSignature) ->
                new UnaryScalar<>(
                    signature,
                    boundSignature,
                    DataTypes.STRING,
                    ParseIdentFunction::parseIdent
                )
        );
    }

    static List<String> parseIdent(String value) {
        Expression expression;
        try {
            expression = SqlParser.createExpression(value);
        } catch (ParsingException e) {
            throw new IllegalArgumentException("invalid name syntax: " + value, e);
        }
        if (expression instanceof QualifiedNameReference ref) {
            return ref.getName().getParts();
        }
        throw new IllegalArgumentException("invalid name syntax: " + value);
    }
}
