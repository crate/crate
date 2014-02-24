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

import com.google.common.collect.ImmutableMap;
import org.apache.lucene.search.Query;
import org.cratedb.service.SQLParseService;
import org.cratedb.sql.parser.parser.NodeType;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.collect.Tuple;
import org.elasticsearch.common.settings.ImmutableSettings;
import org.elasticsearch.common.settings.Settings;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

@Deprecated
public class ParsedStatement {

    public final ArrayList<Tuple<String, String>> outputFields = new ArrayList<>();

    private String schemaName;
    private String[] indices = null;

    private ActionType type;
    private NodeType nodeType;

    public Long versionFilter;
    public String stmt;
    public Query query;

    public String importPath;

    public boolean tableNameIsAlias = false;

    /**
     * used for create analyzer statements
     */
    public Settings createAnalyzerSettings = ImmutableSettings.EMPTY;

    /**
     * set if the where clause contains a single pk column.
     * E.g.:
     *      pk_col = 1
     */
    public String primaryKeyLookupValue;

    /**
     * set if the where clause contains multiple pk columns.
     * E.g.:
     *      pk_col = 1 or pk_col = 2
     */
    public Set<String> primaryKeyValues = new HashSet<>();

    public Set<String> routingValues = new HashSet<>();

    public Set<String> columnsWithFilter = new HashSet<>();
    public int orClauses = 0;
    public List<OrderByColumnName> orderByColumns = new ArrayList<>();

    public ParsedStatement(String stmt) {
        this.stmt = stmt;
    }

    public ImmutableMap<String, Object> indexSettings;
    public ImmutableMap<String, Object> indexMapping;

    public static enum ActionType {
        SEARCH_ACTION,
        INSERT_ACTION,
        DELETE_BY_QUERY_ACTION,
        BULK_ACTION, GET_ACTION,
        DELETE_ACTION,
        UPDATE_ACTION,
        CREATE_INDEX_ACTION,
        DELETE_INDEX_ACTION,
        MULTI_GET_ACTION,
        INFORMATION_SCHEMA,
        CREATE_ANALYZER_ACTION,
        COPY_IMPORT_ACTION,
        STATS
    }

    public static final int UPDATE_RETRY_ON_CONFLICT = 3;

    public BytesReference xcontent;

    private Integer limit = null;
    private Integer offset = null;

    public void offset(Integer offset) {
        this.offset = offset;
    }

    public int offset() {
        if (offset == null) {
            return 0;
        }
        return offset;
    }

    public void limit(Integer limit) {
        this.limit = limit;
    }

    public int limit() {
        if (limit == null) {
            return SQLParseService.DEFAULT_SELECT_LIMIT;
        }
        return limit;
    }

    public void schemaName(String schemaName) {
        this.schemaName = schemaName;
    }

    public String schemaName() {
        return schemaName;
    }


    public void tableName(String tableName) {
        if (indices == null) {
            indices = new String[] { tableName };
        } else {
            indices[0] = tableName;
        }
    }

    public String tableName() {
        return indices == null ? null : indices[0];
    }

    public String[] indices() {
        return indices;
    }

    public void type(ActionType type) {
        this.type = type;
    }

    public ActionType type() {
        return type;
    }

    public void nodeType(NodeType nodeType) {
        this.nodeType = nodeType;
    }

    public NodeType nodeType() {
        return nodeType;
    }

    /**
     * Get the result column-names as listed in the SELECT Statement,
     * eventually including aliases, not real column-names
     * @return Array of Column-Name or -Alias Strings
     */
    public String[] cols() {
        String[] cols = new String[outputFields.size()];
        for (int i = 0; i < outputFields.size(); i++) {
            cols[i] = outputFields.get(i).v1();
        }
        return cols;
    }
}
