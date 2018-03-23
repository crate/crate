/*
 * Licensed to Crate under one or more contributor license agreements.
 * See the NOTICE file distributed with this work for additional
 * information regarding copyright ownership.  Crate licenses this file
 * to you under the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.  You may
 * obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
 * implied.  See the License for the specific language governing
 * permissions and limitations under the License.
 *
 * However, if you have executed another commercial license agreement
 * with Crate these terms will supersede the license and you may use the
 * software solely pursuant to the terms of the relevant commercial
 * agreement.
 */

package io.crate.analyze;

import io.crate.analyze.expressions.ExpressionAnalysisContext;
import io.crate.analyze.expressions.ExpressionAnalyzer;
import io.crate.analyze.relations.FieldProvider;
import io.crate.expression.symbol.Symbol;
import io.crate.metadata.Functions;
import io.crate.metadata.TransactionContext;
import io.crate.metadata.table.Operation;
import io.crate.sql.tree.AlterUser;
import io.crate.sql.tree.Expression;
import io.crate.sql.tree.GenericProperties;
import io.crate.sql.tree.QualifiedName;
import org.elasticsearch.common.Nullable;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class AlterUserAnalyzer {
    private final Functions functions;

    private static final FieldProvider fieldProvider = new FieldProvider() {
        @Override
        public Symbol resolveField(QualifiedName qualifiedName, @Nullable List path, Operation operation) {
            throw new UnsupportedOperationException("Cannot resolve field references");
        }
    };

    public AlterUserAnalyzer(Functions functions) {
        this.functions = functions;
    }


    public AlterUserAnalyzedStatement analyze(AlterUser node, ParamTypeHints typeHints, TransactionContext txnContext) {
        ExpressionAnalysisContext exprContext = new ExpressionAnalysisContext();
        ExpressionAnalyzer expressionAnalyzer = new ExpressionAnalyzer(
            functions,
            txnContext,
            typeHints,
            fieldProvider,
            null
        );

        Map<String, Symbol> rows = new HashMap<>();
        GenericProperties genericProperties = node.genericProperties();

        for (Map.Entry<String, Expression> expr : genericProperties.properties().entrySet()) {
            Symbol valueSymbol = expressionAnalyzer.convert(expr.getValue(), exprContext);
            rows.put(expr.getKey(), valueSymbol);
        }

        return new AlterUserAnalyzedStatement(node.name(), rows);
    }
}
