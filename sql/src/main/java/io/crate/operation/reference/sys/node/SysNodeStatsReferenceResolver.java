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

package io.crate.operation.reference.sys.node;

import io.crate.metadata.ColumnIdent;
import io.crate.metadata.Reference;
import io.crate.metadata.RowCollectExpression;
import io.crate.metadata.expressions.RowCollectExpressionFactory;
import io.crate.operation.reference.ReferenceResolver;

import java.util.HashMap;
import java.util.Map;

public class SysNodeStatsReferenceResolver implements ReferenceResolver<RowCollectExpression<?, ?>> {

    private static Map<ColumnIdent, RowCollectExpressionFactory> sysNodeStatsExpressions
        = SysNodesExpressionFactories.getSysNodesTableInfoFactories();

    private final Map<ColumnIdent, RowCollectExpression> cachedRowCollectExpressions;

    public static SysNodeStatsReferenceResolver newInstance() {
        return new SysNodeStatsReferenceResolver();
    }

    private SysNodeStatsReferenceResolver() {
        this.cachedRowCollectExpressions = new HashMap<>();
    }

    @Override
    public RowCollectExpression<?, ?> getImplementation(Reference refInfo) {
        ColumnIdent columnIdent = refInfo.ident().columnIdent();

        RowCollectExpression cachedRowCollectExpression = cachedRowCollectExpressions.get(columnIdent);
        if (cachedRowCollectExpression != null) {
            return cachedRowCollectExpression;
        }

        RowCollectExpressionFactory rowCollectExpressionFactory = sysNodeStatsExpressions.get(columnIdent);
        if (rowCollectExpressionFactory != null) {
            RowCollectExpression collectExpression = rowCollectExpressionFactory.create();
            cachedRowCollectExpressions.put(columnIdent, collectExpression);
            return collectExpression;
        }

        if (columnIdent.isColumn()) {
            return null;
        }
        return getImplementationByRootTraversal(columnIdent);
    }

    private RowCollectExpression<?, ?> getImplementationByRootTraversal(ColumnIdent columnIdent) {
        ColumnIdent root = columnIdent.getRoot();

        RowCollectExpression rowCollectExpression = cachedRowCollectExpressions.get(root);
        if (rowCollectExpression == null) {
            RowCollectExpressionFactory collectExpressionFactory = sysNodeStatsExpressions.get(root);
            if (collectExpressionFactory == null) {
                return null;
            }
            rowCollectExpression = collectExpressionFactory.create();
            cachedRowCollectExpressions.put(root, rowCollectExpression);
        }

        for (String part : columnIdent.path()) {
            rowCollectExpression = (RowCollectExpression) rowCollectExpression.getChildImplementation(part);
            if (rowCollectExpression == null) {
                return null;
            }
        }
        return rowCollectExpression;
    }
}
