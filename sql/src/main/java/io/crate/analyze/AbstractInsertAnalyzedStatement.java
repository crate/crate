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
import io.crate.analyze.symbol.Reference;
import io.crate.exceptions.ColumnUnknownException;
import io.crate.metadata.ColumnIdent;
import io.crate.metadata.ReferenceIdent;
import io.crate.metadata.ReferenceInfo;
import io.crate.metadata.doc.DocTableInfo;

import javax.annotation.Nullable;
import java.util.*;

/**
 * holding analysis results for any insert statement variant
 */
public abstract class AbstractInsertAnalyzedStatement implements AnalyzedStatement {

    private List<Reference> columns;
    private IntSet primaryKeyColumnIndices = new IntHashSet();
    private IntSet partitionedByColumnsIndices = new IntHashSet();
    private int routingColumnIndex = -1;
    private DocTableInfo tableInfo;

    private final Set<ReferenceInfo> allocatedReferences = new HashSet<>();

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

    public boolean containsReferenceInfo(ReferenceInfo referenceInfo) {
        for (Reference reference : columns) {
            if (reference.info().equals(referenceInfo)) {
                return true;
            }
        }
        return false;
    }

    public IntSet primaryKeyColumnIndices() {
        return primaryKeyColumnIndices;
    }

    public void addPrimaryKeyColumnIdx(int primaryKeyColumnIdx) {
        this.primaryKeyColumnIndices.add(primaryKeyColumnIdx);
    }

    protected List<String> partitionedByColumnNames() {
        assert tableInfo != null;
        List<String> names = new ArrayList<>(tableInfo.partitionedByColumns().size());
        for (ReferenceInfo info : tableInfo.partitionedByColumns()) {
            names.add(info.ident().columnIdent().fqn());
        }
        return names;
    }

    public void routingColumnIndex(int routingColumnIndex) {
        this.routingColumnIndex = routingColumnIndex;
    }

    public int routingColumnIndex() {
        return routingColumnIndex;
    }

    /**
     *
     * @return routing column if it is used in insert statement
     */
    public @Nullable ColumnIdent routingColumn() {
        if (routingColumnIndex < 0) { return null; }
        else {
            return tableInfo.clusteredBy();
        }
    }

    public DocTableInfo tableInfo() {
        return tableInfo;
    }

    public void tableInfo(DocTableInfo tableInfo) {
        this.tableInfo = tableInfo;
    }

    public Reference allocateUniqueReference(ReferenceIdent ident) {
        ColumnIdent column = ident.columnIdent();
        ReferenceInfo referenceInfo = tableInfo.getReferenceInfo(column);
        if (referenceInfo == null) {
            referenceInfo = tableInfo.indexColumn(column);
            if (referenceInfo == null) {
                DynamicReference reference = tableInfo.getDynamic(column, true);
                if (reference == null) {
                    throw new ColumnUnknownException(column.sqlFqn());
                }
                referenceInfo = reference.info();
                if (!allocatedReferences.add(referenceInfo)) {
                    throw new IllegalArgumentException(String.format(Locale.ENGLISH, "reference '%s' repeated", ident.columnIdent().sqlFqn()));
                }
                return reference;
            }
        }
        if (!allocatedReferences.add(referenceInfo)) {
            throw new IllegalArgumentException(String.format(Locale.ENGLISH, "reference '%s' repeated", ident.columnIdent().sqlFqn()));
        }
        return new Reference(referenceInfo);
    }
}
