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

import io.crate.analyze.expressions.ExpressionAnalysisContext;
import io.crate.analyze.expressions.ExpressionAnalyzer;
import io.crate.analyze.relations.FieldProvider;
import io.crate.expression.symbol.Field;
import io.crate.expression.symbol.Symbol;
import io.crate.expression.symbol.format.SymbolFormatter;
import io.crate.metadata.Functions;
import io.crate.metadata.TransactionContext;
import io.crate.sql.tree.Expression;
import io.crate.sql.tree.FunctionCall;
import io.crate.sql.tree.Insert;
import io.crate.sql.tree.ParameterExpression;
import io.crate.types.DataTypes;

import java.util.List;
import java.util.function.Function;

/**
 * ExpressionAnalyzer that supports the VALUES ( columnRef ) expression
 * <p>
 * e.g. in
 * <p>
 * -------------------------------------------------------------------
 * insert into t_new (id, name) (select id_old, name_old from t_old)
 * on duplicate key update id = values(id) + 1000
 * <p>
 * will return the following for values (id) + 1000
 * <p>
 * add(id_old, 10000)
 * <p>
 * -------------------------------------------------------------------
 * insert into t_new (id, name) values (1, 'foo')
 * on duplicate key update id = values (id) + 1
 * <p>
 * will return the following for values (id) + 1
 * <p>
 * 2      (normalized add(1, 1))
 *
 * ON DUPLICATE KEY UPDATE / ON CONFLICT DO UPDATE SET
 * ===================================================
 *
 * The behavior is identical for ON CONFLICT DO UPDATE SET with the only difference
 * that VALUES(col) is not allowed and has to be represented as EXCLUDED.col instead.
 *
 * The following two statements are semantically identical:
 *
 * insert into t_new (id, name) values (1, 'foo')
 *   on conflict do update set name = excluded.name
 *
 * insert into t_new (id, name) values (1, 'foo')
 *   on duplicate key update name = values(name)
 *
 * @deprecated in favor of ON CONFLICT DO UPDATE clause. ExpressionAnalyzer should not be subclassed in the future.
 */
@Deprecated
public class ValuesAwareExpressionAnalyzer extends ExpressionAnalyzer {

    private final ValuesResolver valuesResolver;
    private final Insert.DuplicateKeyType duplicateKeyType;

    /**
     * used to resolve the argument column in VALUES (&lt;argumentColumn&gt;) to the literal or reference
     */
    public interface ValuesResolver {

        Symbol allocateAndResolve(Field argumentColumn);
    }

    ValuesAwareExpressionAnalyzer(Functions functions,
                                  TransactionContext transactionContext,
                                  Function<ParameterExpression, Symbol> convertParamFunction,
                                  FieldProvider fieldProvider,
                                  ValuesResolver valuesResolver,
                                  Insert.DuplicateKeyType duplicateKeyType) {
        super(functions, transactionContext, convertParamFunction, fieldProvider, null);
        this.valuesResolver = valuesResolver;
        this.duplicateKeyType = duplicateKeyType;
    }

    @Override
    protected Symbol convertFunctionCall(FunctionCall node, ExpressionAnalysisContext context) {
        List<String> parts = node.getName().getParts();
        if (parts.get(0).equals("values")) {
            if (duplicateKeyType != Insert.DuplicateKeyType.ON_DUPLICATE_KEY_UPDATE) {
                throw new UnsupportedOperationException("Can't use VALUES outside ON DUPLICATE KEY UPDATE col = VALUES(..)");
            }
            Expression expression = node.getArguments().get(0);

            Symbol argumentColumn = convert(expression, context);
            if (argumentColumn.valueType().equals(DataTypes.UNDEFINED)) {
                throw new IllegalArgumentException(
                    SymbolFormatter.format("Referenced column '%s' in VALUES not found", argumentColumn));
            }
            if (!(argumentColumn instanceof Field)) {
                throw new IllegalArgumentException(SymbolFormatter.format(
                    "Argument to VALUES must reference a column that " +
                    "is part of the INSERT statement. %s is invalid", argumentColumn));
            }
            return valuesResolver.allocateAndResolve((Field) argumentColumn);
        }
        return super.convertFunctionCall(node, context);
    }
}
