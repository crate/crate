/*
 * Licensed to CRATE Technology GmbH ("Crate") under one or more contributor
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

import io.crate.DataType;
import io.crate.metadata.TableIdent;
import io.crate.planner.symbol.Literal;
import io.crate.planner.symbol.Parameter;
import io.crate.planner.symbol.Symbol;
import io.crate.planner.symbol.SymbolType;
import io.crate.sql.tree.CopyFromStatement;
import io.crate.sql.tree.Table;

public class CopyStatementAnalyzer extends DataStatementAnalyzer<CopyAnalysis> {

    @Override
    public Symbol visitCopyFromStatement(CopyFromStatement node, CopyAnalysis context) {
        context.mode(CopyAnalysis.Mode.FROM);
        process(node.table(), context);
        Symbol pathSymbol = process(node.path(), context);
        if (pathSymbol.symbolType() != SymbolType.STRING_LITERAL && pathSymbol.symbolType() != SymbolType.PARAMETER) {
            throw new IllegalArgumentException("Invalid COPY FROM statement");
        }
        if (pathSymbol.symbolType() == SymbolType.PARAMETER) {
            if (((Parameter)pathSymbol).guessedValueType() != DataType.STRING) {
                throw new IllegalArgumentException("Invalid COPY FROM statement");
            }
            pathSymbol = ((Parameter)pathSymbol).toLiteral(DataType.STRING);
        }
        context.path(((Literal) pathSymbol).valueAsString());
        return null;
    }

    @Override
    protected Symbol visitTable(Table node, CopyAnalysis context) {
        context.editableTable(TableIdent.of(node));
        return null;
    }
}
