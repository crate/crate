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

import io.crate.sql.parser.SqlParser;
import io.crate.sql.tree.ArrayDataTypeSignature;
import io.crate.sql.tree.DataTypeParameter;
import io.crate.sql.tree.DataTypeSignature;
import io.crate.sql.tree.DefaultTraversalVisitor;
import io.crate.sql.tree.GenericSignatureType;
import io.crate.types.ArrayType;
import io.crate.types.IntegerLiteralTypeSignature;
import io.crate.types.ParameterTypeSignature;
import io.crate.types.TypeSignature;

public final class TypeSignatureParser extends DefaultTraversalVisitor<TypeSignature, Void> {

    private static final SqlParser SQL_PARSER = new SqlParser();
    private static final TypeSignatureParser INSTANCE = new TypeSignatureParser();

    private TypeSignatureParser() {}

    public static TypeSignature parse(String signature) {
        var dataTypeSignature = SQL_PARSER.generateDataTypeSignature(signature);
        return dataTypeSignature.accept(INSTANCE, null);
    }

    public TypeSignature visitArrayDataTypeSignature(ArrayDataTypeSignature arrayDataTypeSignature, Void context) {
        var parameter = new ArrayList<TypeSignature>();
        if (arrayDataTypeSignature.type() != null) {
            parameter.add(arrayDataTypeSignature.type().accept(this, context));
        }
        return new TypeSignature(ArrayType.NAME, parameter);
    }

    public TypeSignature visitGenericSignatureType(GenericSignatureType typeSignature, Void context) {
        String name = typeSignature.name();
        var fields = new ArrayList<TypeSignature>();
        for (DataTypeSignature field : typeSignature.fields()) {
            fields.add(field.accept(this, context));
        }
        // Ugly hack
        if (name.length() == 1) {
            name = name.toUpperCase();
        }
        return new TypeSignature(name, fields);
    }

    public TypeSignature visitDataTypeParameter(DataTypeParameter dataTypeParameter, Void context) {
        if (dataTypeParameter.identifier() != null && dataTypeParameter.typeSignature() != null) {
            var typeSignature = dataTypeParameter.typeSignature().accept(this, context);
            return new ParameterTypeSignature(dataTypeParameter.identifier(), typeSignature);
        }
        if (dataTypeParameter.numericalValue() != null) {
            return new IntegerLiteralTypeSignature(dataTypeParameter.numericalValue());
        }
        if (dataTypeParameter.typeSignature() != null) {
            return dataTypeParameter.typeSignature().accept(this, context);
        }
        throw new IllegalArgumentException("Invalid DataTypeParameter: " + dataTypeParameter.toString());
    }

}
