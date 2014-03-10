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

import io.crate.core.NumberOfReplicas;
import io.crate.metadata.TableIdent;
import io.crate.metadata.table.SchemaInfo;
import io.crate.metadata.table.TableInfo;

import javax.annotation.Nullable;

public class CreateBlobTableAnalysis extends AbstractDDLAnalysis {

    private NumberOfReplicas numberOfReplicas;
    private Integer numberOfShards;

    public CreateBlobTableAnalysis(Object[] parameters) {
        super(parameters);
    }

    @Override
    public TableInfo table() {
        return null;
    }

    @Override
    public SchemaInfo schema() {
        return null;
    }

    @Override
    public void normalize() {
    }

    public String tableName() {
        return tableIdent.name();
    }

    public void numberOfReplicas(String numberOfReplicas) {
        this.numberOfReplicas = new NumberOfReplicas(numberOfReplicas);
    }

    @Nullable
    public NumberOfReplicas numberOfReplicas() {
        return numberOfReplicas;
    }

    public void numberOfShards(Integer numberOfShards) {
        this.numberOfShards = numberOfShards;
    }

    @Nullable
    public Integer numberOfShards() {
        return numberOfShards;
    }

    @Override
    public <C, R> R accept(AnalysisVisitor<C, R> analysisVisitor, C context) {
        return analysisVisitor.visitCreateBlobTableAnalysis(this, context);
    }

    public TableIdent tableIdent() {
        return tableIdent;
    }
}
