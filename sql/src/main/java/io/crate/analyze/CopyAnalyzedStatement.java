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

import com.google.common.collect.ImmutableList;
import io.crate.metadata.PartitionName;
import io.crate.metadata.Functions;
import io.crate.metadata.ReferenceInfos;
import io.crate.metadata.ReferenceResolver;
import io.crate.planner.symbol.Symbol;
import io.crate.types.DataType;
import org.elasticsearch.common.settings.ImmutableSettings;
import org.elasticsearch.common.settings.Settings;

import javax.annotation.Nullable;
import java.util.List;

public class CopyAnalyzedStatement extends AbstractDataAnalyzedStatement {

    private Settings settings = ImmutableSettings.EMPTY;

    public static enum Mode {
        FROM,
        TO
    }

    private Symbol uri;
    private Mode mode;
    private boolean directoryUri;
    private String partitionIdent = null;

    public CopyAnalyzedStatement(ReferenceInfos referenceInfos,
                                 Functions functions,
                                 ParameterContext parameterContext,
                                 ReferenceResolver referenceResolver) {
        super(referenceInfos, functions, parameterContext, referenceResolver);
    }

    @Override
    public boolean expectsAffectedRows() {
        return true;
    }

    public Symbol uri() {
        return uri;
    }

    @Override
    public List<DataType> outputTypes() {
        return ImmutableList.of();
    }

    @Nullable
    public String partitionIdent() {
        return this.partitionIdent;
    }

    public void partitionIdent(String partitionIdent) {
        this.partitionIdent = partitionIdent;
    }

    public void directoryUri(boolean directoryUri) {
        this.directoryUri = directoryUri;
    }

    public boolean directoryUri() {
        return this.directoryUri;
    }

    public void uri(Symbol uri) {
        this.uri = uri;
    }

    public Mode mode() {
        return mode;
    }

    public void mode(Mode mode) {
        this.mode = mode;
    }

    public void settings(Settings settings){
        this.settings = settings;
    }

    public Settings settings(){
        return settings;
    }

    public boolean partitionExists(String partitionIdent){
        if (table.isPartitioned() && partitionIdent != null ){
            return table.partitions().contains(PartitionName.fromPartitionIdent(table.ident().schema(), table.ident().name(), partitionIdent));
        }
        return false;
    }

    @Override
    public <C, R> R accept(AnalyzedStatementVisitor<C, R> analyzedStatementVisitor, C context) {
        return analyzedStatementVisitor.visitCopyStatement(this, context);
    }

}
