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

import static io.crate.metadata.functions.TypeVariableConstraint.typeVariable;
import static io.crate.types.DataTypes.INTEGER;

import java.util.List;
import java.util.Locale;

import io.crate.data.Input;
import io.crate.data.Row;
import io.crate.metadata.FunctionName;
import io.crate.metadata.NodeContext;
import io.crate.metadata.TransactionContext;
import io.crate.metadata.functions.BoundSignature;
import io.crate.metadata.functions.Signature;
import io.crate.metadata.pgcatalog.PgCatalogSchemaInfo;
import io.crate.metadata.tablefunctions.TableFunctionImplementation;
import io.crate.types.DataTypes;
import io.crate.types.RowType;
import io.crate.types.TypeSignature;

public final class GenerateSubscripts<T> extends TableFunctionImplementation<T> {

    public static final FunctionName NAME = new FunctionName(PgCatalogSchemaInfo.NAME, "generate_subscripts");
    private static final RowType RETURN_TYPE = new RowType(List.of(INTEGER), List.of(NAME.name()));


    public static void register(TableFunctionModule module) {
        module.register(
            Signature.table(
                    NAME,
                    TypeSignature.parse("array(E)"),
                    DataTypes.INTEGER.getTypeSignature(),
                    DataTypes.INTEGER.getTypeSignature()
                ).withTypeVariableConstraints(typeVariable("E"))
                .withFeature(Feature.NON_NULLABLE),
            GenerateSubscripts::new
        );
        module.register(
            Signature.table(
                    NAME,
                    TypeSignature.parse("array(E)"),
                    DataTypes.INTEGER.getTypeSignature(),
                    DataTypes.BOOLEAN.getTypeSignature(),
                    DataTypes.INTEGER.getTypeSignature()
                ).withTypeVariableConstraints(typeVariable("E"))
                .withFeature(Feature.NON_NULLABLE),
            GenerateSubscripts::new
        );
    }

    private GenerateSubscripts(Signature signature, BoundSignature boundSignature) {
        super(signature, boundSignature);
    }

    private static int getNumRows(List<?> array, int depthLevel) {
        if (depthLevel <= 0) {
            throw new IllegalArgumentException("target level must be greater than zero");
        }
        List<?> targetArray = array;
        for (int level = 2; level <= depthLevel; level++) {
            if (targetArray == null || targetArray.isEmpty()) {
                return 0;
            }
            int size = -1;
            List<?> firstNonNullElement = null;
            for (int i = 0; i < targetArray.size(); i++) {
                Object oi = targetArray.get(i);
                if (oi == null) {
                    // null is a valid value within an array
                    continue;
                }
                if (!(oi instanceof List)) {
                    return 0;
                }
                List<?> element = (List<?>) oi;
                if (size == -1) {
                    size = element.size();
                } else {
                    if (size != element.size()) {
                        throw new IllegalArgumentException(String.format(
                            Locale.ENGLISH,
                            "nested arrays must have the same dimension within a level, offending level %d, position %d",
                            level, i + 1));
                    }
                }
                if (firstNonNullElement == null) {
                    firstNonNullElement = element;
                }
            }
            targetArray = firstNonNullElement;
        }
        return targetArray != null ? targetArray.size() : 0;
    }

    @SafeVarargs
    @Override
    public final Iterable<Row> evaluate(TransactionContext txnCtx, NodeContext nodeCtx, Input<T>... args) {
        assert args.length == 2 || args.length == 3 :
            "Signature must ensure that there are either two or three arguments";

        List<?> array = (List<?>) args[0].value();
        Integer dim = (Integer) args[1].value();
        if (array == null || array.isEmpty() || dim == null) {
            return List.of();
        }
        int numRows = getNumRows(array, dim);
        if (numRows == 0) {
            return List.of();
        }
        Boolean rev = null;
        if (args.length == 3) {
            rev = (Boolean) args[2].value();
        }
        boolean reversed = rev != null && rev.booleanValue();
        int startInclusive = reversed ? numRows : 1;
        int stopInclusive = reversed ? 1 : numRows;
        int step = reversed ? -1 : 1;
        return new RangeIterable<>(
            startInclusive,
            stopInclusive,
            value -> value + step,
            Integer::compareTo,
            i -> i
        );
    }

    @Override
    public RowType returnType() {
        return RETURN_TYPE;
    }

    @Override
    public boolean hasLazyResultSet() {
        return true;
    }
}
