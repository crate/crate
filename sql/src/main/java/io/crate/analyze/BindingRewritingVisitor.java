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

import io.crate.analyze.tree.BoundFunctionCall;
import io.crate.analyze.tree.BoundReference;
import io.crate.metadata.ReferenceInfo;
import io.crate.sql.tree.*;

public class BindingRewritingVisitor extends AstVisitor<Expression, Analysis> {

    @Override
    protected Expression visitSingleColumn(SingleColumn node, Analysis context) {
        if (!node.getAlias().isPresent()) {
            // TODO: toString() repr is not always correct
            node.setAlias(node.getExpression().toString());
        }
        node.setExpression(node.getExpression().accept(this, context));
        return null;
    }

    @Override
    protected BoundReference visitSubscriptExpression(SubscriptExpression node, Analysis context) {
        ReferenceInfo info = context.getReferenceInfo(node);
        return new BoundReference(info);
    }

    @Override
    protected BoundFunctionCall visitFunctionCall(FunctionCall node, Analysis context) {
        // TODO: need to replace expressions from node.getArguments()
        return new BoundFunctionCall(context.getFunctionInfo(node), node.getArguments());
    }
}
