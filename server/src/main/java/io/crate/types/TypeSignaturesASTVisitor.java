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

package io.crate.types;

import java.util.ArrayList;
import java.util.List;

import org.jetbrains.annotations.Nullable;

import io.crate.signatures.antlr.TypeSignaturesBaseVisitor;
import io.crate.signatures.antlr.TypeSignaturesParser;

class TypeSignaturesASTVisitor extends TypeSignaturesBaseVisitor<TypeSignature> {

    @Override
    public TypeSignature visitDoublePrecision(TypeSignaturesParser.DoublePrecisionContext context) {
        return new TypeSignature(DataTypes.DOUBLE.getName(), List.of());
    }

    @Override
    public TypeSignature visitTimeStampWithoutTimeZone(TypeSignaturesParser.TimeStampWithoutTimeZoneContext context) {
        return new TypeSignature(TimestampType.INSTANCE_WITHOUT_TZ.getName(), List.of());
    }

    @Override
    public TypeSignature visitTimeStampWithTimeZone(TypeSignaturesParser.TimeStampWithTimeZoneContext context) {
        return new TypeSignature(TimestampType.INSTANCE_WITH_TZ.getName(), List.of());
    }

    @Override
    public TypeSignature visitTimeWithTimeZone(TypeSignaturesParser.TimeWithTimeZoneContext context) {
        return new TypeSignature(TimeTZType.NAME, List.of());
    }

    @Override
    public TypeSignature visitArray(TypeSignaturesParser.ArrayContext context) {
        var parameter = visitOptionalContext(context.type());
        List<TypeSignature> parameters = parameter == null ? List.of() : List.of(parameter);
        return new TypeSignature(ArrayType.NAME, parameters);
    }

    @Override
    public TypeSignature visitObject(TypeSignaturesParser.ObjectContext context) {
        return new TypeSignature(ObjectType.NAME, getParameters(context.parameters()));
    }

    @Override
    public TypeSignature visitGeneric(TypeSignaturesParser.GenericContext context) {
        return new TypeSignature(getIdentifier(context.identifier()), getParameters(context.parameters()));
    }

    @Override
    public TypeSignature visitRow(TypeSignaturesParser.RowContext context) {
        return new TypeSignature(RowType.NAME, getParameters(context.parameters()));
    }

    @Override
    public TypeSignature visitParameter(TypeSignaturesParser.ParameterContext context) {
        if (context.INTEGER_VALUE() != null) {
            return new IntegerLiteralTypeSignature(Integer.parseInt(context.INTEGER_VALUE().getText()));
        } else if (context.identifier() != null) {
            return new ParameterTypeSignature(getIdentifier(context.identifier()), visitOptionalContext(context.type()));
        } else {
            return visit(context.type());
        }
    }

    @Nullable
    private String getIdentifier(@Nullable TypeSignaturesParser.IdentifierContext context) {
        if (context != null) {
            if (context.QUOTED_INDENTIFIER() != null) {
                var token = context.QUOTED_INDENTIFIER().getText();
                return token.substring(1, token.length() - 1);
            }

            if (context.UNQUOTED_INDENTIFIER() != null) {
                return context.UNQUOTED_INDENTIFIER().getText();
            }
        }
        return null;
    }

    @Nullable
    private TypeSignature visitOptionalContext(@Nullable TypeSignaturesParser.TypeContext context) {
        if (context != null) {
            return visit(context);
        }
        return null;
    }

    private List<TypeSignature> getParameters(@Nullable TypeSignaturesParser.ParametersContext context) {
        if (context == null || context.parameter().isEmpty()) {
            return List.of();
        }
        var result = new ArrayList<TypeSignature>(context.parameter().size());
        for (var p : context.parameter()) {
            result.add(p.accept(this));
        }
        return result;
    }

}
