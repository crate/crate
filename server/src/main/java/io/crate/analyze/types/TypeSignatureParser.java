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

package io.crate.analyze.types;

import java.util.ArrayList;
import java.util.List;

import javax.annotation.Nullable;

import io.crate.sql.parser.SqlParser;
import io.crate.sql.tree.ArrayTypeSignature;
import io.crate.sql.tree.TypeParameter;
import io.crate.sql.tree.TypeSignatureType;
import io.crate.types.ArrayType;
import io.crate.types.IntegerLiteralTypeSignature;
import io.crate.types.ParameterTypeSignature;
import io.crate.types.TypeSignature;

public class TypeSignatureParser {

    private static final SqlParser SQL_PARSER = new SqlParser();

    private TypeSignatureParser() {}

    public static io.crate.types.TypeSignature apply(String signature) {
        return convert(SQL_PARSER.createTypeSignature(signature));
    }

    @Nullable
    static io.crate.types.TypeSignature convert(@Nullable io.crate.sql.tree.TypeSignature typeSignature) {
        if (typeSignature == null) {
            return null;
        }
        if (typeSignature instanceof ArrayTypeSignature a) {
                List<TypeSignature> parameter = new ArrayList<>();
                if (a.type() != null) {
                    parameter.add(convert(a.type()));
                }
                return new io.crate.types.TypeSignature(ArrayType.NAME, parameter);
            }
        if (typeSignature instanceof TypeSignatureType t) {
                String name = t.name();
                ArrayList<TypeSignature> fields = new ArrayList<>();
                for (io.crate.sql.tree.TypeSignature field : t.fields()) {
                    fields.add(convert(field));
                }
                // Ugly hack
                if (name.length() == 1) {
                    name = name.toUpperCase();
                }
                return new io.crate.types.TypeSignature(name, fields);
            }
        if (typeSignature instanceof TypeParameter t) {
                if (t.identifier() != null) {
                    return new ParameterTypeSignature(t.identifier(), convert(t.typeSignature()));
                }
                if (t.numericalValue() != null) {
                    return new IntegerLiteralTypeSignature(t.numericalValue());
                }
                if (t.typeSignature() != null) {
                    return convert(t.typeSignature());
                }
            }
        throw new IllegalArgumentException("Invalid Expression: " + typeSignature.toString());
    }
}
