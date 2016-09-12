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

import com.carrotsearch.hppc.IntHashSet;
import com.carrotsearch.hppc.IntSet;
import io.crate.analyze.symbol.DynamicReference;
import io.crate.exceptions.ColumnUnknownException;
import io.crate.metadata.ColumnIdent;
import io.crate.metadata.Reference;
import io.crate.metadata.doc.DocTableInfo;

import java.util.HashSet;
import java.util.List;
import java.util.Locale;
import java.util.Set;

/**
 * holding analysis results for any insert statement variant
 */
public abstract class AbstractInsertAnalyzedStatement implements AnalyzedStatement {

    protected List<Reference> columns;
    private IntSet primaryKeyColumnIndices = new IntHashSet();
    private IntSet partitionedByColumnsIndices = new IntHashSet();
    private int routingColumnIndex = -1;
    protected DocTableInfo tableInfo;

    private final Set<Reference> allocatedReferences = new HashSet<>();

    public List<Reference> columns() {
        return columns;
    }

    public IntSet partitionedByIndices() {
        return partitionedByColumnsIndices;
    }

    public void addPartitionedByIndex(int i) {
        this.partitionedByColumnsIndices.add(i);
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


    public void routingColumnIndex(int routingColumnIndex) {
        this.routingColumnIndex = routingColumnIndex;
    }

    public int routingColumnIndex() {
        return routingColumnIndex;
    }

    public DocTableInfo tableInfo() {
        return tableInfo;
    }

    public void tableInfo(DocTableInfo tableInfo) {
        this.tableInfo = tableInfo;
    }

    Reference allocateUniqueReference(ColumnIdent column) {
        Reference ref = tableInfo.getReference(column);
        if (ref == null) {
            ref = tableInfo.indexColumn(column);
            if (ref == null) {
                DynamicReference reference = tableInfo.getDynamic(column, true);
                if (reference == null) {
                    throw new ColumnUnknownException(column.sqlFqn());
                }
                if (!allocatedReferences.add(reference)) {
                    throw new IllegalArgumentException(String.format(Locale.ENGLISH, "reference '%s' repeated", column.sqlFqn()));
                }
                return reference;
            }
        }
        if (!allocatedReferences.add(ref)) {
            throw new IllegalArgumentException(String.format(Locale.ENGLISH, "reference '%s' repeated", column.sqlFqn()));
        }
        return ref;
    }

    @Override
    public boolean isWriteOperation() {
        return true;
    }
}
