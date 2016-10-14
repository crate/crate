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

import com.google.common.base.Function;
import io.crate.action.sql.SessionContext;
import io.crate.analyze.expressions.ExpressionAnalysisContext;
import io.crate.analyze.expressions.ExpressionAnalyzer;
import io.crate.analyze.relations.FieldProvider;
import io.crate.analyze.symbol.Field;
import io.crate.analyze.symbol.Symbol;
import io.crate.analyze.symbol.format.SymbolFormatter;
import io.crate.metadata.Functions;
import io.crate.sql.tree.Expression;
import io.crate.sql.tree.FunctionCall;
import io.crate.sql.tree.ParameterExpression;
import io.crate.types.DataTypes;

import java.util.List;

/**
 * ExpressionAnalyzer that supports the VALUES ( columnRef ) expression
 * <p>
 * e.g. in
 * <p>
 * -------------------------------------------------------------------
 * insert into t_new (id, name) (select id_old, name_old from t_old)
 * on duplicate key update
 * set id = values(id) + 1000
 * <p>
 * will return the following for values (id) + 1000
 * <p>
 * add(id_old, 10000)
 * <p>
 * -------------------------------------------------------------------
 * insert into t_new (id, name) values (1, 'foo')
 * on duplicate key update
 * set id = values (id) + 1
 * <p>
 * will return the following for values (id) + 1
 * <p>
 * 2      (normalized add(1, 1))
 */
public class ValuesAwareExpressionAnalyzer extends ExpressionAnalyzer {

    private final ValuesResolver valuesResolver;

    /**
     * used to resolve the argument column in VALUES (&lt;argumentColumn&gt;) to the literal or reference
     */
    interface ValuesResolver {

        Symbol allocateAndResolve(Field argumentColumn);
    }

    ValuesAwareExpressionAnalyzer(Functions functions,
                                  SessionContext sessionContext,
                                  Function<ParameterExpression, Symbol> convertParamFunction,
                                  FieldProvider fieldProvider,
                                  ValuesResolver valuesResolver) {
        super(functions, sessionContext, convertParamFunction, fieldProvider, null);
        this.valuesResolver = valuesResolver;
    }

    @Override
    protected Symbol convertFunctionCall(FunctionCall node, ExpressionAnalysisContext context) {
        List<String> parts = node.getName().getParts();
        if (parts.get(0).equals("values")) {
            Expression expression = node.getArguments().get(0);
            Symbol argumentColumn = super.convert(expression, context);
            if (argumentColumn.valueType().equals(DataTypes.UNDEFINED)) {
                throw new IllegalArgumentException(
                    SymbolFormatter.format("Referenced column '%s' in VALUES expression not found", argumentColumn));
            }
            if (!(argumentColumn instanceof Field)) {
                throw new IllegalArgumentException(SymbolFormatter.format(
                    "Argument to VALUES expression must reference a column that " +
                    "is part of the INSERT statement. %s is invalid", argumentColumn));
            }
            return valuesResolver.allocateAndResolve((Field) argumentColumn);
        }
        return super.convertFunctionCall(node, context);
    }
}
