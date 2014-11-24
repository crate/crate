/*
 * Licensed to CRATE Technology GmbH ("Crate") under one or more contributor
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

package io.crate.analyze;

import io.crate.types.DataType;
import io.crate.types.DataTypes;

import java.util.Locale;

import static io.crate.planner.symbol.Literal.newLiteral;

public class ParameterContext {

    final Object[] parameters;
    final Object[][] bulkParameters;
    DataType[] bulkTypes;

    private int currentIdx = 0;

    public ParameterContext(Object[] parameters, Object[][] bulkParameters) {
        this.parameters = parameters;
        if (bulkParameters.length > 0) {
            validateBulkParams(bulkParameters);
        }
        this.bulkParameters = bulkParameters;
    }

    private void validateBulkParams(Object[][] bulkParams) {
        for (Object[] bulkParam : bulkParams) {
            if (bulkTypes == null) {
                initializeBulkTypes(bulkParam);
                continue;
            } else if (bulkParam.length != bulkTypes.length) {
                throw new IllegalArgumentException("mixed number of arguments inside bulk arguments");
            }

            for (int i = 0; i < bulkParam.length; i++) {
                Object o = bulkParam[i];
                DataType expectedType = bulkTypes[i];
                DataType guessedType = guessTypeSafe(o);

                if (expectedType.isUndefinedType()) {
                    bulkTypes[i] = guessedType;
                } else if (o != null && !bulkTypes[i].equals(guessedType)) {
                    throw new IllegalArgumentException(String.format(Locale.ENGLISH,
                            "argument %d of bulk arguments contains mixed data types", i + 1));
                }
            }
        }
    }

    private static DataType guessTypeSafe(Object value) throws IllegalArgumentException {
        DataType guessedType = DataTypes.guessType(value, true);
        if (guessedType == null) {
            throw new IllegalArgumentException(String.format(
                    "Got an argument \"%s\" that couldn't be recognized", value));
        }
        return guessedType;
    }

    private void initializeBulkTypes(Object[] bulkParam) {
        bulkTypes = new DataType[bulkParam.length];
        for (int i = 0; i < bulkParam.length; i++) {
            bulkTypes[i] = guessTypeSafe(bulkParam[i]);
        }
    }

    public boolean hasBulkParams() {
        return bulkParameters.length > 0;
    }

    public void setBulkIdx(int i) {
        this.currentIdx = i;
    }

    public Object[] parameters() {
        if (hasBulkParams()) {
            return bulkParameters[currentIdx];
        }
        return parameters;
    }

    public io.crate.planner.symbol.Literal getAsSymbol(int index) {
        try {
            if (hasBulkParams()) {
                // already did a type guess so it is possible to create a literal directly
                return newLiteral(bulkTypes[index], bulkParameters[currentIdx][index]);
            }
            DataType type = guessTypeSafe(parameters[index]);
            // use type.value because some types need conversion (String to BytesRef, List to Array)
            return newLiteral(type, type.value(parameters[index]));
        } catch (ArrayIndexOutOfBoundsException e) {
            throw new IllegalArgumentException(String.format(Locale.ENGLISH,
                    "Tried to resolve a parameter but the arguments provided with the " +
                            "SQLRequest don't contain a parameter at position %d", index), e);
        }
    }
}
