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

import io.crate.action.sql.SQLOperations;
import io.crate.core.collections.Row;
import io.crate.types.DataType;
import io.crate.types.DataTypes;

import javax.annotation.Nullable;
import java.util.Collections;
import java.util.List;
import java.util.Locale;
import java.util.Set;

import static io.crate.analyze.symbol.Literal.newLiteral;


public class ParameterContext {

    public static final ParameterContext EMPTY = new ParameterContext(
        Row.EMPTY, Collections.<Row>emptyList(), null, SQLOperations.Option.NONE);

    @Nullable
    private final String defaultSchema;

    private final Set<SQLOperations.Option> options;
    private final Row parameters;
    private final List<Row> bulkParameters;

    private int currentIdx = 0;


    public ParameterContext(Row parameters, List<Row> bulkParameters,
                            @Nullable String defaultSchema, Set<SQLOperations.Option> options) {
        this.parameters = parameters;
        this.defaultSchema = defaultSchema;
        this.options = options;
        if (bulkParameters.size() > 0) {
            validateBulkParams(bulkParameters);
        }
        this.bulkParameters = bulkParameters;
    }

    public ParameterContext(Row parameters, List<Row> bulkParameters, @Nullable String defaultSchema) {
        this(parameters, bulkParameters, defaultSchema, SQLOperations.Option.NONE);
    }

    public Set<SQLOperations.Option> options() {
        return options;
    }

    @Nullable
    public String defaultSchema() {
        return defaultSchema;
    }

    private void validateBulkParams(List<Row> bulkParams) {
        int length = bulkParams.get(0).size();
        for (Row bulkParam : bulkParams) {
            if (bulkParam.size() != length) {
                throw new IllegalArgumentException("mixed number of arguments inside bulk arguments");
            }
        }
    }

    private static DataType guessTypeSafe(Object value) throws IllegalArgumentException {
        DataType guessedType = DataTypes.guessType(value);
        if (guessedType == null) {
            throw new IllegalArgumentException(String.format(Locale.ENGLISH,
                    "Got an argument \"%s\" that couldn't be recognized", value));
        }
        return guessedType;
    }

    public boolean hasBulkParams() {
        return bulkParameters.size() > 0;
    }

    public int numBulkParams() {
        return bulkParameters.size();
    }

    public void setBulkIdx(int i) {
        this.currentIdx = i;
    }

    public Row parameters() {
        if (hasBulkParams()) {
            return bulkParameters.get(currentIdx);
        }
        return parameters;
    }

    public io.crate.analyze.symbol.Literal getAsSymbol(int index) {
        try {
            Object value = parameters().get(index);
            DataType type = guessTypeSafe(value);
            // use type.value because some types need conversion (String to BytesRef, List to Array)
            return newLiteral(type, type.value(value));
        } catch (ArrayIndexOutOfBoundsException e) {
            throw new IllegalArgumentException(String.format(Locale.ENGLISH,
                    "Tried to resolve a parameter but the arguments provided with the " +
                            "SQLRequest don't contain a parameter at position %d", index), e);
        }
    }
}
