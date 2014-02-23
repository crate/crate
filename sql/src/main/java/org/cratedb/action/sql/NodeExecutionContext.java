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

import org.cratedb.action.sql.analyzer.AnalyzerService;
import org.cratedb.index.ColumnDefinition;
import org.cratedb.index.IndexMetaDataExtractor;
import org.cratedb.sql.CrateException;
import org.cratedb.sql.TableAliasSchemaException;
import org.cratedb.sql.TableUnknownException;
import org.elasticsearch.action.support.IgnoreIndices;
import org.elasticsearch.cluster.ClusterService;
import org.elasticsearch.cluster.metadata.IndexMetaData;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.indices.IndexMissingException;

import java.io.IOException;
import java.util.List;

@Deprecated
public class NodeExecutionContext {

    private final ClusterService clusterService;
    private final AnalyzerService analyzerService;
    private final Settings settings;

    @Inject
    public NodeExecutionContext(ClusterService clusterService,
                                AnalyzerService analyzerService,
                                Settings settings) {

        this.clusterService = clusterService;
        this.analyzerService = analyzerService;
        this.settings = settings;
    }

    /**
     * create a new TableExecutionContext for the table described by schema and table
     * @param schema the name of the schema this table is in
     * @param table the name of the table
     * @return an implementation of ITableExecutionContext or null if no context could be created
     */
    public ITableExecutionContext tableContext(String schema, String table) {
        // resolve aliases to the concreteIndices
        String[] indices = {table};
        String[] concreteIndices;
        boolean tableIsAlias = false;
        try {
            concreteIndices = clusterService.state().metaData().concreteIndices(
                    indices, IgnoreIndices.NONE, true
            );
        } catch (IndexMissingException ex) {
            throw new TableUnknownException(table, ex);
        }

        if (concreteIndices.length == 1 ) {
            tableIsAlias = !concreteIndices[0].equals(indices[0]);
        } else if (concreteIndices.length > 1) {
            tableIsAlias = true;
            try {
                if (!compareIndicesMetaData(concreteIndices)) {
                    throw new TableAliasSchemaException(table);
                }
            } catch (IOException e) {
                throw new CrateException("Unknown error while comparing table meta data", e);
            }
        }
        IndexMetaData indexMetaData = clusterService.state().metaData().index(concreteIndices[0]);
        if (indexMetaData != null) {
            IndexMetaDataExtractor metaDataExtractor = new IndexMetaDataExtractor(indexMetaData);

            return new TableExecutionContext(
                    table,
                    metaDataExtractor,
                    tableIsAlias);
        } else {
            return null;
        }
    }

    public AnalyzerService analyzerService() {
        return analyzerService;
    }

    private boolean compareIndicesMetaData(String[] indices) throws IOException {
        if (!settings.getAsBoolean("crate.table_alias.schema_check", true)) {
            return true;
        }

        IndexMetaData indexMetaData = clusterService.state().metaData().index(indices[0]);
        IndexMetaDataExtractor extractor = new IndexMetaDataExtractor(indexMetaData);
        String routingColumn = extractor.getRoutingColumn();
        List<ColumnDefinition> columnDefinitionList = extractor
                .getColumnDefinitions();
        List<IndexMetaDataExtractor.Index> indexList = extractor.getIndices();

        for (int i = 1; i < indices.length; i++) {
            IndexMetaData indexMetaDataOther = clusterService.state().metaData()
                    .index(indices[i]);
            IndexMetaDataExtractor extractorOther = new IndexMetaDataExtractor(indexMetaDataOther);
            String routingColumnOther = extractorOther.getRoutingColumn();
            if ((routingColumn != null && !routingColumn.equals(routingColumnOther))
                    || (routingColumn == null && routingColumnOther != null)) {
                return false;
            }
            List<ColumnDefinition> columnDefinitionListOther =
                    extractorOther.getColumnDefinitions();
            if (columnDefinitionList.size() != columnDefinitionListOther.size()) {
                return false;
            }
            for (int j = 0; j < columnDefinitionList.size(); j++) {
                if (!columnDefinitionList.get(j).columnName.equals(columnDefinitionListOther.get(j).columnName)
                        || !columnDefinitionList.get(j).dataType.equals(columnDefinitionListOther.get(j).dataType)) {
                    return false;
                }
            }
            List<IndexMetaDataExtractor.Index> indexListOther = extractorOther.getIndices();
            if (indexList.size() != indexListOther.size()) {
                return false;
            }
            for (int j = 0; j < indexList.size(); j++) {
                if (!indexList.get(j).columnName.equals(indexListOther.get(j).columnName)
                        || !indexList.get(j).indexName.equals(indexListOther.get(j).indexName)
                        || !indexList.get(j).method.equals(indexListOther.get(j).method)
                        ) {
                    return false;
                }
            }
        }

        return true;
    }
}
