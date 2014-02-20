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

import io.crate.metadata.ReferenceIdent;
import io.crate.planner.symbol.Literal;
import io.crate.planner.symbol.Reference;
import io.crate.planner.symbol.Symbol;
import io.crate.sql.tree.Assignment;
import io.crate.sql.tree.QualifiedNameReference;
import io.crate.sql.tree.SubscriptExpression;
import io.crate.sql.tree.Update;

import java.util.List;

public class UpdateStatementAnalyzer extends StatementAnalyzer<UpdateAnalysis> {

    @Override
    public Symbol visitUpdate(Update node, UpdateAnalysis context) {
        context.updateStatement(node);
        process(node.table(), context);

        for (Assignment assignment : node.assignements()) {
            process(assignment, context);
        }
        if (node.whereClause().isPresent()) {
            processWhereClause(node.whereClause().get(), context);
        }
        return null;
    }

    @Override
    protected Symbol visitSubscriptExpression(SubscriptExpression node, UpdateAnalysis context) {
        SubscriptContext subscriptContext = new SubscriptContext();
        node.accept(visitor, subscriptContext);
        ReferenceIdent ident = new ReferenceIdent(
                context.table().ident(), subscriptContext.column(), subscriptContext.parts());
        return context.allocateReference(ident);
    }

    @Override
    protected Symbol visitQualifiedNameReference(QualifiedNameReference node, UpdateAnalysis context) {
        ReferenceIdent ident;
        List<String> parts = node.getName().getParts();
        switch (parts.size()) {
            case 1:
                ident = new ReferenceIdent(context.table().ident(), parts.get(0));
                break;
            case 2:
                if (!context.table().ident().name().equals(parts.get(0))) {
                    throw new UnsupportedOperationException("unsupported name reference: " + node);
                }
                ident = new ReferenceIdent(context.table().ident(), parts.get(1));
                break;
            default:
                throw new UnsupportedOperationException("unsupported name reference: " + node);
        }
        return context.allocateReference(ident);
    }

    @Override
    public Symbol visitAssignment(Assignment node, UpdateAnalysis context) {
        // unknown columns in strict objects handled in here
        Reference reference = (Reference)process(node.columnName(), context);
        Symbol value = process(node.expression(), context);

        // it's something that we can normalize to a literal
        Literal updateValue = context.normalizeInputValue(value, reference);
        context.addAssignement(reference, updateValue);
        return null;
    }
}
