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

import com.google.common.base.Preconditions;
import io.crate.metadata.ReferenceIdent;
import io.crate.metadata.TableIdent;
import io.crate.planner.symbol.Symbol;
import io.crate.sql.tree.Delete;
import io.crate.sql.tree.QualifiedNameReference;
import io.crate.sql.tree.Table;

import java.util.List;

public class DeleteStatementAnalyzer extends StatementAnalyzer<DeleteAnalysis> {

    @Override
    public Symbol visitDelete(Delete node, DeleteAnalysis context) {
        process(node.getTable(), context);

        if (context.table().isAlias()) {
            throw new IllegalArgumentException("Table alias not allowed in DELETE statement.");
        }

        if (node.getWhere().isPresent()) {
            processWhereClause(node.getWhere().get(), context);
        }

        return null;
    }

    @Override
    protected Symbol visitTable(Table node, DeleteAnalysis context) {
        Preconditions.checkState(context.table() == null, "deleting multiple tables is not supported");
        context.editableTable(TableIdent.of(node));
        return null;
    }

    protected Symbol visitQualifiedNameReference(QualifiedNameReference node, DeleteAnalysis context) {
        ReferenceIdent ident;
        List<String> parts = node.getName().getParts();
        switch (parts.size()) {
            case 1:
                ident = new ReferenceIdent(context.table().ident(), parts.get(0));
                break;
            case 2:
                // make sure tableName matches the tableInfo
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
}
