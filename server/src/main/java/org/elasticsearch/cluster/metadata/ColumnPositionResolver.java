/*
 * Licensed to Crate.io GmbH ("Crate") under one or more contributor
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

package org.elasticsearch.cluster.metadata;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.function.BiConsumer;

public class ColumnPositionResolver<T> {
    // Depths of the columns are used as keys such that deeper columns take higher column positions. (parent's position < children's positions)
    private Map<Integer, List<Column<T>>> columnsToReposition = new TreeMap<>(Comparator.naturalOrder());
    private Map<Integer, Column<T>> takenPositions = new HashMap<>();

    public void updatePositions(int startingColumnPosition) {
        // by depths first
        for (var o : this.columnsToReposition.values()) {
            // then by columnOrder (nulls-first, descending) then by name
            Collections.sort(o);
            for (Column<T> column : o) {
                column.updatePosition(++startingColumnPosition);
            }
        }
        this.clear();
    }

    public void updatePositions() {
        updatePositions(takenPositions.isEmpty() ? 0 : Collections.max(takenPositions.keySet()));
    }

    public void addColumnToReposition(String[] fullPathName,
                                      @Nullable Integer columnPosition,
                                      T column,
                                      BiConsumer<T, Integer> setPosition,
                                      int depth) {
        /*
         * Possible values of column positions:
         *      null: due to a bug (related: https://github.com/crate/crate/issues/12630)
         *      negative: actually column orders borrowing column positions' place
         *      positive: actual column positions
         *      NOTE: 0 should not happen
         *
         * Possible values of column orders:
         *      null: 1) propagation of the bug through column position 2) when column positions are positive
         *      negative: when column positions are negative
         *
         * column order: specifies order of columns ex) a before b, b before c
         * column position: exact positions, ex) a = 4, b = 5, c = 6
         */

        assert columnPosition == null || columnPosition != 0 : "columns should not be positioned to 0";
        Column<T> duplicateCol = takenPositions.get(columnPosition);
        boolean isDuplicate = duplicateCol != null;
        Column<T> c = new Column<>(
            fullPathName,
            columnPosition == null || columnPosition > 0 ? null : columnPosition,
            depth,
            setPosition,
            column);
        if (columnPosition == null || columnPosition < 0) { // equivalent to if column order is null or negative
            addToColumnsToReposition(depth, c);
        } else if (isDuplicate) {
            int depthOfDuplicateColumn = duplicateCol.depth();
            if (depth < depthOfDuplicateColumn ||
                depth == depthOfDuplicateColumn && c.compareTo(duplicateCol) < 0) {
                takenPositions.put(columnPosition, c);
                addToColumnsToReposition(depthOfDuplicateColumn, duplicateCol);
            } else {
                addToColumnsToReposition(depth, c);
            }
        } else { // when column positions are positive
            takenPositions.put(columnPosition, c);
        }
    }

    public int numberOfColumnsToReposition() {
        return this.columnsToReposition.size();
    }

    private void addToColumnsToReposition(int depth, Column<T> c) {
        List<Column<T>> columnsPerDepths = columnsToReposition.get(depth);
        if (columnsPerDepths == null) {
            List<Column<T>> columns = new ArrayList<>();
            columns.add(c);
            columnsToReposition.put(depth, columns);
        } else {
            columnsPerDepths.add(c);
        }
    }

    private void clear() {
        this.columnsToReposition = new TreeMap<>(Comparator.naturalOrder());
        this.takenPositions = new HashMap<>();
    }

    private record Column<T>(String[] fullPathName,
                             Integer columnOrdering,
                             int depth,
                             BiConsumer<T, Integer> setPosition,
                             T column) implements Comparable<Column<T>> {

        public void updatePosition(Integer position) {
            this.setPosition.accept(column, position);
        }

        @Override
        public int compareTo(@Nonnull Column<T> o) {
            // by columnOrder (nulls-first, descending) then by name
            if (this.columnOrdering == null && o.columnOrdering == null) {
                assert this.fullPathName.length == o.fullPathName.length :
                    "depth should be equal, implying the sizes of the full path names to be equal";
                return compareFullPathName(o);
            } else if (this.columnOrdering == null) {
                return -1;
            } else if (o.columnOrdering == null) {
                return 1;
            } else {
                int comparison = o.columnOrdering.compareTo(this.columnOrdering);
                if (comparison != 0) {
                    return comparison;
                } else {
                    return compareFullPathName(o);
                }
            }
        }

        private int compareFullPathName(Column<T> o) {
            for (int i = 0; i < this.fullPathName.length; i++) {
                int comparison = this.fullPathName[i].compareTo(o.fullPathName[i]);
                if (comparison != 0) {
                    return comparison;
                }
            }
            throw new IllegalArgumentException("Cannot resolve column positions due to identical column names");
        }
    }
}
