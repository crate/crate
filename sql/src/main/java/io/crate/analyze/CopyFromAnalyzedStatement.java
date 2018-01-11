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

import io.crate.execution.dsl.phases.FileUriCollectPhase;
import io.crate.expression.symbol.Symbol;
import org.elasticsearch.cluster.node.DiscoveryNode;
import io.crate.metadata.doc.DocTableInfo;
import io.crate.types.DataType;
import org.elasticsearch.common.settings.Settings;

import javax.annotation.Nullable;
import java.util.function.Predicate;

public class CopyFromAnalyzedStatement extends AbstractCopyAnalyzedStatement {

    private final DocTableInfo table;
    @Nullable
    private final String partitionIdent;
    private final Predicate<DiscoveryNode> nodePredicate;
    @Nullable
    private final FileUriCollectPhase.InputFormat inputFormat;

    public CopyFromAnalyzedStatement(DocTableInfo table,
                                     Settings settings,
                                     Symbol uri,
                                     @Nullable String partitionIdent,
                                     Predicate<DiscoveryNode> nodePredicate,
                                     FileUriCollectPhase.InputFormat inputFormat) {
        super(settings, uri);
        this.table = table;
        this.partitionIdent = partitionIdent;
        this.nodePredicate = nodePredicate;
        this.inputFormat = inputFormat;
    }

    public FileUriCollectPhase.InputFormat inputFormat() {
        return inputFormat;
    }

    public DocTableInfo table() {
        return table;
    }

    @Nullable
    public String partitionIdent() {
        return this.partitionIdent;
    }

    @Override
    public <C, R> R accept(AnalyzedStatementVisitor<C, R> analyzedStatementVisitor, C context) {
        return analyzedStatementVisitor.visitCopyFromStatement(this, context);
    }

    public static IllegalArgumentException raiseInvalidType(DataType dataType) {
        throw new IllegalArgumentException("fileUri must be of type STRING or STRING ARRAY. Got " + dataType);
    }

    public Predicate<DiscoveryNode> nodePredicate() {
        return nodePredicate;
    }

    @Override
    public boolean isWriteOperation() {
        return true;
    }
}
