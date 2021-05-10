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

package io.crate.analyze;

import io.crate.expression.symbol.Symbol;
import io.crate.expression.symbol.Symbols;
import io.crate.metadata.ColumnIdent;
import io.crate.sql.tree.ColumnDefinition;
import io.crate.sql.tree.ColumnPolicy;
import io.crate.sql.tree.Expression;
import io.crate.types.DataType;

import java.util.List;

public class SymbolToColumnDefinitionConverter {

    private static final ColumnPolicy OBJECT_TYPE_DEFAULT_COLUMN_POLICY = ColumnPolicy.STRICT;

    public static ColumnDefinition<Expression> symbolToColumnDefinition(Symbol symbol) {

        final ColumnIdent column = Symbols.pathFromSymbol(symbol);
        final DataType<?> dataType = symbol.valueType();
        var columnType = MetadataToASTNodeResolver.dataTypeToColumnType(
            null,
            dataType,
            OBJECT_TYPE_DEFAULT_COLUMN_POLICY,
            null
        );

        return new ColumnDefinition<>(
            column.sqlFqn(),        // allow ObjectTypes to return col name in subscript notation
            null,
            null,
            columnType,
            List.of());
    }
}
