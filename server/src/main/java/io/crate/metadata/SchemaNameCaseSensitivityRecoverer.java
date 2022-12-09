/*
 * Licensed to Crate.io GmbH ("Crate") under one or more contributor
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

package io.crate.metadata;

import io.crate.sql.tree.AstVisitor;
import io.crate.sql.tree.Expression;
import io.crate.sql.tree.FunctionCall;

public class SchemaNameCaseSensitivityRecoverer extends AstVisitor<Expression, String> {

    private static final SchemaNameCaseSensitivityRecoverer INSTANCE = new SchemaNameCaseSensitivityRecoverer();

    public static Expression recover(Expression expression, String caseSensitiveSchemaName) {
        return expression.accept(INSTANCE, caseSensitiveSchemaName);
    }

    @Override
    protected Expression visitFunctionCall(FunctionCall node, String caseSensitiveSchemaName) {
        var functionName = node.getName().getParts();
        if (functionName.get(0).equals(caseSensitiveSchemaName.toLowerCase())) {
            functionName.remove(0);
            functionName.add(0, caseSensitiveSchemaName);
        }
        return node;
    }
}
