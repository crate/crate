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

package io.crate.planner.node;

import com.google.common.base.Objects;
import com.google.common.collect.ImmutableList;
import io.crate.planner.DataTypeVisitor;
import io.crate.planner.projection.Projection;
import io.crate.planner.symbol.InputColumn;
import io.crate.planner.symbol.Symbol;
import io.crate.planner.symbol.SymbolType;
import io.crate.types.DataType;
import io.crate.types.DataTypes;

import javax.annotation.Nullable;
import java.util.ArrayList;
import java.util.List;

/**
 * Helper class for extracting and resolving types of symbols used in plannodes
 */
public class PlanNodeTypeResolver {

    /**
     * Extract the types of the given symbols as is.
     *
     * Returns UNDEFINED for {@linkplain io.crate.planner.symbol.InputColumn}
     * and {@linkplain io.crate.planner.symbol.Aggregation} symbols.
     *
     * @return a list of DataTypes
     */
    public static List<DataType> extractDataTypes(List<Symbol> symbols) {
        return extractSymbolDataTypes(symbols, null);
    }

    /**
     * Extract the datatypes of <code>symbols</code>.
     * If a symbol is an {@linkplain io.crate.planner.symbol.InputColumn}
     * and <code>inputTypes</code> is not null, use the corresponding type from
     * this list.
     *
     * @param symbols the symbols to extract the types of
     * @param inputTypes the types to use as a fallback
     * @return a list of DataTypes
     */
    public static List<DataType> extractSymbolDataTypes(List<Symbol> symbols, @Nullable List<DataType> inputTypes) {
        List<DataType> types = new ArrayList<>(symbols.size());
        for (Symbol symbol : symbols) {
            if (symbol.symbolType() == SymbolType.INPUT_COLUMN) {
                if (inputTypes != null) {
                    int columnIdx = ((InputColumn) symbol).index();
                    types.add(inputTypes.get(columnIdx));

                } else {
                    types.add(DataTypes.UNDEFINED); // TODO: what to do here?
                }
            } else {
                types.add(DataTypeVisitor.fromSymbol(symbol));
            }
        }
        return types;
    }

    /**
     * Extract the DataTypes from the list of projections.
     * If the symbol is an {@linkplain io.crate.planner.symbol.InputColumn} or it
     * cannot be determined, consider the output from the former projection if any.
     * If no type could be found this way and <code>inputTypes</code>, is not null,
     * use the corresponding type from this list as a fallback.
     *
     * @param projections the list of projections to extract the output types of.
     * @param inputTypes the types to use as a fallback
     * @return a list of DataTypes
     */
    public static List<DataType> extractDataTypes(List<Projection> projections, @Nullable List<DataType> inputTypes) {
        if (projections.size() == 0){
            return inputTypes;
        }
        int projectionIdx = projections.size() - 1;
        Projection lastProjection = projections.get(projectionIdx);
        List<DataType> types = new ArrayList<>(lastProjection.outputs().size());

        List<DataType> dataTypes = Objects.firstNonNull(inputTypes, ImmutableList.<DataType>of());

        for (int c = 0; c < lastProjection.outputs().size(); c++) {
            types.add(resolveType(projections, projectionIdx, c, dataTypes));
        }

        return types;
    }

    /**
     * resolve the type of the column at <code>columnIdx</code> of the projection
     * in <code>projections</code> at index <code>projectionIdx</code>.
     * As a fallback, return the corresponding input type
     */
    public static DataType resolveType(List<Projection> projections, int projectionIdx, int columnIdx, List<DataType> inputTypes) {
        Projection projection = projections.get(projectionIdx);
        Symbol symbol = projection.outputs().get(columnIdx);
        DataType type = DataTypeVisitor.fromSymbol(symbol);
        if (type == null) {
            if (symbol.symbolType() == SymbolType.INPUT_COLUMN) {
                columnIdx = ((InputColumn)symbol).index();
            }
            if (projectionIdx > 0) {
                return resolveType(projections, projectionIdx - 1, columnIdx, inputTypes);
            } else {
                assert symbol instanceof InputColumn; // otherwise type shouldn't be null
                return inputTypes.get(((InputColumn) symbol).index());
            }
        }

        return type;
    }

}
