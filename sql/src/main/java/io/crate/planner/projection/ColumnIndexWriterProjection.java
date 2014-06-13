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

package io.crate.planner.projection;

import com.carrotsearch.hppc.IntSet;
import com.google.common.collect.Lists;
import io.crate.metadata.ColumnIdent;
import io.crate.planner.symbol.InputColumn;
import io.crate.planner.symbol.Symbol;
import org.elasticsearch.common.settings.Settings;

import javax.annotation.Nullable;
import java.util.ArrayList;
import java.util.List;

public class ColumnIndexWriterProjection extends AbstractIndexWriterProjection {

    private List<Symbol> columnSymbols;
    private List<ColumnIdent> columnIdents;

    public static final ProjectionFactory<ColumnIndexWriterProjection> FACTORY =
            new ProjectionFactory<ColumnIndexWriterProjection>() {
                @Override
                public ColumnIndexWriterProjection newInstance() {
                    return new ColumnIndexWriterProjection();
                }
            };

    protected ColumnIndexWriterProjection() {}

    /**
     *
     * @param tableName
     * @param primaryKeys
     * @param columns the columnIdents of all the columns to be written in order of appearance
     * @param primaryKeyIndices
     * @param partitionedByIndices
     * @param clusteredByIndex
     * @param settings
     */
    public ColumnIndexWriterProjection(String tableName,
                                       List<ColumnIdent> primaryKeys,
                                       List<ColumnIdent>  columns,
                                       IntSet primaryKeyIndices,
                                       IntSet partitionedByIndices,
                                       @Nullable ColumnIdent clusteredByColumn,
                                       int clusteredByIndex,
                                       Settings settings) {
        super(tableName, primaryKeys, clusteredByColumn, settings);
        generateSymbols(primaryKeyIndices.toArray(), partitionedByIndices.toArray(), clusteredByIndex);

        this.columnIdents = Lists.newArrayList(columns);
        this.columnSymbols = new ArrayList<>(columns.size()-partitionedByIndices.size());

        for (int i = 0; i < columns.size(); i++) {
            if (!partitionedByIndices.contains(i)) {
                this.columnSymbols.add(new InputColumn(i));
            } else {
                columnIdents.remove(i);
            }
        }

    }

    public List<Symbol> columnSymbols() {
        return columnSymbols;
    }

    public List<ColumnIdent> columnIdents() {
        return columnIdents;
    }



    @Override
    public <C, R> R accept(ProjectionVisitor<C, R> visitor, C context) {
        return visitor.visitColumnIndexWriterProjection(this, context);
    }
}
