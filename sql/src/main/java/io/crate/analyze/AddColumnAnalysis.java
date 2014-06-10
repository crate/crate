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

import com.google.common.base.Optional;
import io.crate.PartitionName;
import io.crate.exceptions.InvalidTableNameException;
import io.crate.metadata.FulltextAnalyzerResolver;
import io.crate.metadata.ReferenceInfos;
import io.crate.metadata.TableIdent;
import io.crate.metadata.table.TableInfo;

import javax.annotation.Nullable;

public class AddColumnAnalysis extends CreateTableAnalysis {

    private Optional<PartitionName> partitionName;
    private TableInfo tableInfo;

    public AddColumnAnalysis(ReferenceInfos referenceInfos,
                             FulltextAnalyzerResolver fulltextAnalyzerResolver,
                             Object[] parameters) {
        super(referenceInfos, fulltextAnalyzerResolver, parameters);
    }

    public void partitionName(@Nullable PartitionName partitionName) {
        this.partitionName = Optional.fromNullable(partitionName);
    }

    public Optional<PartitionName> partitionName() {
        return partitionName;
    }

    @Override
    public void table(TableIdent tableIdent) {
        if (!isValidTableName(tableIdent.name())) {
            throw new InvalidTableNameException(tableIdent.name());
        }

        this.tableInfo = referenceInfos.getTableInfoUnsafe(tableIdent);
        this.tableIdent = tableIdent;
    }

    @Override
    public TableInfo table() {
        return this.tableInfo;
    }

    @Override
    public boolean isData() {
        return false;
    }

    @Override
    public <C, R> R accept(AnalysisVisitor<C, R> visitor, C context) {
        return visitor.visitAddColumnAnalysis(this, context);
    }
}
