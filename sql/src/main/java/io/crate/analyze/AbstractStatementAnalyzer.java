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

import com.google.common.base.Joiner;
import com.google.common.base.Preconditions;
import io.crate.exceptions.UnsupportedFeatureException;
import io.crate.metadata.TableIdent;
import io.crate.metadata.blob.BlobSchemaInfo;
import io.crate.planner.DataTypeVisitor;
import io.crate.sql.ExpressionFormatter;
import io.crate.sql.tree.*;

import java.util.ArrayList;
import java.util.List;

public abstract class AbstractStatementAnalyzer<R extends Object, T extends Analysis> extends DefaultTraversalVisitor<R, T> {
    protected static OutputNameFormatter outputNameFormatter = new OutputNameFormatter();
    protected static PrimaryKeyVisitor primaryKeyVisitor = new PrimaryKeyVisitor();

    protected static SubscriptVisitor visitor = new SubscriptVisitor();
    protected static DataTypeVisitor symbolDataTypeVisitor = new DataTypeVisitor();
    protected static NegativeLiteralVisitor negativeLiteralVisitor = new NegativeLiteralVisitor();

    static class OutputNameFormatter extends ExpressionFormatter.Formatter {
        @Override
        protected String visitQualifiedNameReference(QualifiedNameReference node, Void context) {

            List<String> parts = new ArrayList<>();
            for (String part : node.getName().getParts()) {
                parts.add(part);
            }
            return Joiner.on('.').join(parts);
        }

        @Override
        protected String visitSubscriptExpression(SubscriptExpression node, Void context) {
            return String.format("%s[%s]", process(node.name(), null), process(node.index(), null));
        }
    }

    @Override
    protected R visitNode(Node node, T context) {
        System.out.println("Not analyzed node: " + node);
        return super.visitNode(node, context);
    }

    @Override
    protected R visitTable(Table node, T context) {
        Preconditions.checkState(context.table() == null, "selecting from multiple tables is not supported");
        TableIdent tableIdent = TableIdent.of(node);
        if (tableIdent.schema() != null && tableIdent.schema().equalsIgnoreCase(BlobSchemaInfo.NAME)) {
            throw new UnsupportedFeatureException("Querying on BLOB tables is not supported yet");
        }
        context.table(tableIdent);
        return null;
    }
}
