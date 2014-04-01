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
import io.crate.exceptions.UnsupportedFeatureException;
import io.crate.metadata.TableIdent;
import io.crate.planner.symbol.Parameter;
import io.crate.planner.symbol.Symbol;
import io.crate.planner.symbol.SymbolType;
import io.crate.sql.tree.*;
import org.elasticsearch.common.settings.ImmutableSettings;
import org.elasticsearch.common.settings.Settings;

import java.util.List;
import java.util.Map;

public class CopyStatementAnalyzer extends DataStatementAnalyzer<CopyAnalysis> {

    @Override
    public Symbol visitCopyFromStatement(CopyFromStatement node, CopyAnalysis context) {
        if (node.genericProperties().isPresent()) {
            context.settings(settingsFromProperties(node.genericProperties().get(), context));
        }
        context.mode(CopyAnalysis.Mode.FROM);
        process(node.table(), context);
        Symbol pathSymbol = process(node.path(), context);

        if (pathSymbol.symbolType() == SymbolType.PARAMETER) {
            if (((Parameter) pathSymbol).guessedValueType() != DataType.STRING) {
                throw new IllegalArgumentException("Invalid COPY FROM statement");
            }
            pathSymbol = ((Parameter) pathSymbol).toLiteral(DataType.STRING);
        } else if (pathSymbol.symbolType() != SymbolType.STRING_LITERAL) {
            throw new IllegalArgumentException("Invalid COPY FROM statement");
        }
        context.uri(pathSymbol);
        return null;
    }

    @Override
    public Symbol visitCopyTo(CopyTo node, CopyAnalysis context) {

        context.mode(CopyAnalysis.Mode.TO);
        if (node.genericProperties().isPresent()) {
            context.settings(settingsFromProperties(node.genericProperties().get(), context));
        }
        process(node.table(), context);
        context.uri(process(node.targetUri(), context));

        if (node.directoryUri()) {
            // TODO: add format symbols to generate shard specific uris
            throw new UnsupportedFeatureException("directory URI not supprted");
        }
        return null;
    }

    private Settings settingsFromProperties(GenericProperties properties, CopyAnalysis context) {
        ImmutableSettings.Builder builder = ImmutableSettings.builder();
        for (Map.Entry<String, List<Expression>> entry : properties.properties().entrySet()) {
            if (entry.getValue().size() != 1) {
                throw new IllegalArgumentException("Invalid argument(s) passed to parameter");
            }
            Symbol v = process(entry.getValue().get(0), context);
            if (!v.symbolType().isLiteral()) {
                throw new UnsupportedFeatureException("Only literals are allowed as parameter values");
            }
            builder.put(entry.getKey(),
                    ((io.crate.planner.symbol.Literal) v).valueAsString());
        }
        return builder.build();
    }

    @Override
    protected Symbol visitTable(Table node, CopyAnalysis context) {
        context.editableTable(TableIdent.of(node));
        return null;
    }
}
