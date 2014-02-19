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

import com.carrotsearch.hppc.IntOpenHashSet;
import com.carrotsearch.hppc.IntSet;
import com.google.common.base.Preconditions;
import io.crate.metadata.Functions;
import io.crate.metadata.ReferenceInfos;
import io.crate.metadata.ReferenceResolver;
import io.crate.metadata.TableIdent;
import io.crate.metadata.table.TableInfo;
import io.crate.planner.RowGranularity;
import io.crate.planner.symbol.Reference;
import io.crate.planner.symbol.Symbol;
import io.crate.sql.tree.Insert;

import java.util.ArrayList;
import java.util.List;

public class InsertAnalysis extends Analysis {

    private Insert insertStatement;
    private List<List<Symbol>> values;
    private List<Reference> columns;
    private boolean visitingValues = false;
    private IntSet primaryKeyColumnIndices = new IntOpenHashSet(); // optional

    public InsertAnalysis(ReferenceInfos referenceInfos,
                          Functions functions,
                          Object[] parameters,
                          ReferenceResolver referenceResolver) {
        super(referenceInfos, functions, parameters, referenceResolver);
    }

    @Override
    public Type type() {
        return Type.INSERT;
    }

    @Override
    public void table(TableIdent tableIdent) {
        TableInfo t = referenceInfos.getTableInfo(tableIdent);
        Preconditions.checkNotNull(t, "Table not found", tableIdent);
        if (t.rowGranularity() != RowGranularity.DOC) {
            throw new UnsupportedOperationException("cannot insert into system tables");
        }
        table = t;
        updateRowGranularity(table.rowGranularity());
        super.table(tableIdent);
    }

    public void visitValues() {
        this.visitingValues = true;
    }

    public boolean isVisitingValues() {
        return this.visitingValues;
    }

    public Insert insertStatement() {
        return insertStatement;
    }

    public void insertStatement(Insert insertStatement) {
        this.insertStatement = insertStatement;
    }

    public void allocateValues() {
        this.values = new ArrayList<>();
    }

    public void allocateValues(int numValues) {
        this.values = new ArrayList<>(numValues);
    }

    public void addValues(List<Symbol> values) {
        if (this.values == null) {
            allocateValues();
        }
        for (Symbol s : values) {
            s = normalizer.process(s, null);
        }
        this.values.add(values);

    }

    public List<List<Symbol>> values() {
        return values;
    }

    public List<Reference> columns() {
        return columns;
    }

    public void columns(List<Reference> columns) {
        this.columns = columns;
    }

    public IntSet primaryKeyColumnIndices() {
        return primaryKeyColumnIndices;
    }

    public void addPrimaryKeyColumnIdx(int primaryKeyColumnIdx) {
        this.primaryKeyColumnIndices.add(primaryKeyColumnIdx);
    }

}
