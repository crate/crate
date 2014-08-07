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

import io.crate.exceptions.UnsupportedFeatureException;
import io.crate.metadata.TableIdent;
import io.crate.planner.symbol.StringValueSymbolVisitor;
import io.crate.planner.symbol.Symbol;
import io.crate.sql.tree.*;
import org.elasticsearch.common.settings.ImmutableSettings;
import org.elasticsearch.common.settings.Settings;

import java.util.ArrayList;
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

        if (!node.table().partitionProperties().isEmpty()) {
            context.partitionIdent(PartitionPropertiesAnalyzer.toPartitionIdent(
                            context.table(),
                            node.table().partitionProperties(),
                            context.parameters()));
        }

        Symbol pathSymbol = process(node.path(), context);
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

        if (!node.table().partitionProperties().isEmpty()) {
            context.partitionIdent(PartitionPropertiesAnalyzer.toPartitionIdent(
                    context.table(),
                    node.table().partitionProperties(),
                    context.parameters()));
        }
        context.uri(process(node.targetUri(), context));
        context.directoryUri(node.directoryUri());

        List<Symbol> columns = new ArrayList<>(node.columns().size());
        for (Expression expression : node.columns()) {
            columns.add(process(expression, context));
        }
        context.outputSymbols(columns);
        return null;
    }

    private Settings settingsFromProperties(GenericProperties properties, CopyAnalysis context) {
        ImmutableSettings.Builder builder = ImmutableSettings.builder();
        for (Map.Entry<String, Expression> entry : properties.properties().entrySet()) {
            String key = entry.getKey();
            Expression expression = entry.getValue();
            if (expression instanceof ArrayLiteral) {
                throw new IllegalArgumentException("Invalid argument(s) passed to parameter");
            }
            if (expression instanceof QualifiedNameReference) {
                throw new IllegalArgumentException(String.format(
                        "Can't use column reference in property assignment \"%s = %s\". Use literals instead.",
                        key,
                        ((QualifiedNameReference) expression).getName().toString()));
            }

            Symbol v = process(expression, context);
            if (!v.symbolType().isValueSymbol()) {
                throw new UnsupportedFeatureException("Only literals are allowed as parameter values");
            }
            builder.put(key, StringValueSymbolVisitor.INSTANCE.process(v));
        }
        return builder.build();
    }

    @Override
    protected Symbol visitTable(Table node, CopyAnalysis context) {
        context.editableTable(TableIdent.of(node));
        return null;
    }
}
