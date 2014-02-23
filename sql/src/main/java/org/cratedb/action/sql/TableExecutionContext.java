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

package org.cratedb.action.sql;

import org.cratedb.index.ColumnDefinition;
import org.cratedb.index.IndexMetaDataExtractor;
import org.elasticsearch.common.logging.ESLogger;
import org.elasticsearch.common.logging.Loggers;

import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;

@Deprecated
public class TableExecutionContext implements ITableExecutionContext {

    private final ESLogger logger = Loggers.getLogger(getClass());
    private final IndexMetaDataExtractor indexMetaDataExtractor;
    public final String tableName;
    private boolean tableIsAlias = false;



    public TableExecutionContext(String name, IndexMetaDataExtractor indexMetaDataExtractor,
                                 boolean tableIsAlias) {
        this.indexMetaDataExtractor = indexMetaDataExtractor;
        this.tableName = name;
        this.tableIsAlias = tableIsAlias;
    }


    protected Map<String, Object> mapping() {
        return indexMetaDataExtractor.getDefaultMappingMap();
    }

    /**
     * Returns the ``primary key`` column names defined at index creation under the ``_meta``
     * key. If not defined, return empty list.
     *
     * @return a list of primary key column names
     */
    @SuppressWarnings("unchecked")
    public List<String> primaryKeys() {
        return indexMetaDataExtractor.getPrimaryKeys();
    }

    /**
     * Returns the ``primary key`` column names defined at index creation under the ``_meta``
     * key. If none defined, add ``_id`` as primary key(Default).
     *
     * @return a list of primary key column names
     */
    public List<String> primaryKeysIncludingDefault() {
        List<String> primaryKeys = primaryKeys();
        if (primaryKeys.isEmpty()) {
            primaryKeys.add("_id"); // Default Primary Key (only for optimization, not for consistency checks)
        }
        return primaryKeys;
    }


    /**
     * returns all columns defined in the mapping as a sorted sequence
     * to be used in "*" selects.
     *
     * @return a sequence of column names
     */
    @SuppressWarnings("unchecked")
    public Iterable<String> allCols() {
        Set<String> res = new TreeSet<>();
        if (mapping().size() > 0 && mapping().containsKey("properties")) {
            String columnName;
            for (ColumnDefinition columnDefinition : indexMetaDataExtractor.getColumnDefinitions()) {
                // don't add internal or sub object field names
                columnName = columnDefinition.columnName;
                if (columnName.startsWith("_") || columnName.contains(".") || !columnDefinition.isSupported()) {
                    continue;
                }

                res.add(columnName);
            }
        }
        return res;
    }

    /**
     * Check if given name is equal to defined routing name.
     *
     * @param name
     * @return
     */
    public Boolean isRouting(String name) {
        String routingPath = indexMetaDataExtractor.getRoutingColumn();
        if (routingPath == null) {
            // the primary key(s) values are saved under _id, so they are used as default
            // routing values
            if (primaryKeys().contains(name)) {
                return true;
            }
            routingPath = "_id";
        }
        return routingPath.equals(name);
    }

    public boolean tableIsAlias() {
        return tableIsAlias;
    }
}
