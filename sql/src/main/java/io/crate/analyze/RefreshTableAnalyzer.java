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
import io.crate.PartitionName;
import io.crate.metadata.ReferenceInfo;
import io.crate.metadata.TableIdent;
import io.crate.sql.tree.Expression;
import io.crate.sql.tree.GenericProperties;
import io.crate.sql.tree.RefreshStatement;

import java.util.Arrays;
import java.util.List;
import java.util.Map;

public class RefreshTableAnalyzer extends AbstractStatementAnalyzer<Void, RefreshTableAnalysis> {

    @Override
    public Void visitRefreshStatement(RefreshStatement node, RefreshTableAnalysis context) {
        context.table(TableIdent.of(node.table()));
        if (node.table().partitionProperties().isPresent()) {
            setParitionIdent(node, context);
        }

        return null;
    }

    private void setParitionIdent(RefreshStatement node, RefreshTableAnalysis context) {
        GenericProperties properties = node.table().partitionProperties().get();
        Preconditions.checkArgument(properties.properties().size() == context.table().partitionedBy().size(),
                "The Table \"%s\" is partitioned by %s columns but the refresh statement included %s partition columns",
                context.table().ident().name(), context.table().partitionedBy().size(), properties.properties().size());

        String[] values = new String[properties.properties().size()];
        for (Map.Entry<String, List<Expression>> stringListEntry : properties.properties().entrySet()) {
            Object value = ExpressionToObjectVisitor.convert(
                    stringListEntry.getValue().get(0), context.parameters());
            int idx = context.table().partitionedBy().indexOf(stringListEntry.getKey());
            try {
                ReferenceInfo referenceInfo = context.table().partitionedByColumns().get(idx);
                values[idx] = referenceInfo.type().value(value).toString();
            } catch (IndexOutOfBoundsException ex) {
                throw new IllegalArgumentException(
                        String.format("\"%s\" is no known partition column", stringListEntry.getKey()));
            }
        }
        context.partitionIdent(new PartitionName(context.table().ident().name(), Arrays.asList(values)).ident());
    }
}
