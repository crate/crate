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

import com.google.common.base.Strings;
import com.google.common.collect.ImmutableMap;
import com.google.common.util.concurrent.UncheckedExecutionException;
import io.crate.metadata.ReferenceInfos;
import io.crate.metadata.TableIdent;
import io.crate.metadata.table.SchemaInfo;
import io.crate.metadata.table.TableInfo;
import org.cratedb.action.sql.analyzer.AnalyzerService;
import org.cratedb.sql.TableAlreadyExistsException;
import org.cratedb.sql.TableUnknownException;
import org.elasticsearch.common.settings.ImmutableSettings;
import org.elasticsearch.common.settings.Settings;

import java.util.*;

public class CreateTableAnalysis extends AbstractDDLAnalysis {

    private final ImmutableSettings.Builder indexSettingsBuilder = ImmutableSettings.builder();
    private final Map<String, Object> mappingProperties = new HashMap<>();
    private final Map<String, Object> metaColumns = new HashMap<>();
    private final Map<String, Object> metaIndices = new HashMap<>();
    private final List<String> primaryKeys = new ArrayList<>();

    private final Stack<Map<String, Object>> propertiesStack = new Stack<>();
    private final Map<String, Object> mapping = new HashMap<>();
    private final Map<String, Set<String>> copyTo = new HashMap<>();


    /**
     *  _meta : {
     *      columns: {
     *          "someColumn": {
     *              "collection_type": [array | set | null]
     *          }
     *      },
     *      indices: {
     *          "someColumn_ft: {}
     *      }
     *      primary_keys: [ ... ]
     * }
     */
    private final Map<String, Object> crateMeta;
    private final ReferenceInfos referenceInfos;
    private final AnalyzerService analyzerService;

    private String currentColumnName;
    private Map<String, Object> currentMetaColumnDefinition = new HashMap<>();

    public CreateTableAnalysis(ReferenceInfos referenceInfos,
                               AnalyzerService analyzerService,
                               Object[] parameters) {
        super(parameters);
        this.referenceInfos = referenceInfos;
        this.analyzerService = analyzerService;

        crateMeta = new HashMap<>();
        crateMeta.put("primary_keys", primaryKeys);
        crateMeta.put("columns", metaColumns);
        crateMeta.put("indices", metaIndices);

        mapping.put("_meta", crateMeta);
        mapping.put("properties", mappingProperties);
        mapping.put("_all", ImmutableMap.of("enabled", false));

        propertiesStack.push(mappingProperties);
    }

    @Override
    public void table(TableIdent tableIdent) {
        if (!Strings.isNullOrEmpty(tableIdent.schema())) {
            throw new UnsupportedOperationException("Custom schemas are currently not supported");
        }

        // TODO: add a getTableInfoUnsafe() to make this a bit cleaner
        try {
            if (referenceInfos.getTableInfo(tableIdent) != null) {
                throw new TableAlreadyExistsException(tableIdent.name());
            }
        } catch (UncheckedExecutionException e) {
            if (e.getCause() instanceof TableUnknownException) {
                super.table(tableIdent);
            } else {
                throw new TableAlreadyExistsException(tableIdent.name());
            }
        } catch (TableUnknownException e) {
            super.table(tableIdent);
        }
    }


    public String tableName() {
        return tableIdent.name();
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

    @Override
    public <C, R> R accept(AnalysisVisitor<C, R> analysisVisitor, C context) {
        return analysisVisitor.visitCreateTableAnalysis(this, context);
    }

    public ImmutableSettings.Builder indexSettingsBuilder() {
        return indexSettingsBuilder;
    }

    public Settings indexSettings() {
        return indexSettingsBuilder.build();
    }

    public void addColumnDefinition(String columnName,
                                    Map<String, Object> columnDefinition) {
        currentMetaColumnDefinition = new HashMap<>();
        metaColumns.put(columnName, currentMetaColumnDefinition);
        currentColumnName = columnName;
        currentColumnDefinition().put(columnName, columnDefinition);
        propertiesStack.push(columnDefinition);
    }

    public void addIndexDefinition(String name,
                                   Map<String, Object> columnDefinition) {
        currentColumnName = name;
        currentColumnDefinition().put(name, columnDefinition);
        propertiesStack.push(columnDefinition);

        if (metaIndices.containsKey(name)) {
            throw new IllegalArgumentException(
                    String.format("the index name \"%s\" is already in use!", name));
        }
        metaIndices.put(name, new HashMap<>());
    }

    public Map<String, Object> currentMetaColumnDefinition() {
        return currentMetaColumnDefinition;
    }

    public Map<String, Object> currentColumnDefinition() {
        return propertiesStack.peek();
    }

    public String currentColumnName() {
        return currentColumnName;
    }

    public Map<String, Object> mappingProperties() {
        return mappingProperties;
    }

    public void addPrimaryKey(String columnName) {
        if (primaryKeys.size() > 0) {
            throw new UnsupportedOperationException("Multiple primary keys are currently not supported");
        }
        primaryKeys.add(columnName);
    }

    public List<String> primaryKeys() {
        return primaryKeys;
    }

    public Map<String, Object> mapping() {
        return mapping;
    }

    public Map<String, Object> metaMapping() {
        return crateMeta;
    }

    public Stack<Map<String, Object>> propertiesStack() {
        return propertiesStack;
    }

    public void addCopyTo(String sourceColumn, String targetColumn) {
        Set<String> targetColumns = copyTo.get(sourceColumn);
        if (targetColumns == null) {
            targetColumns = new HashSet<>();
            copyTo.put(sourceColumn, targetColumns);
        }
        targetColumns.add(targetColumn);
    }

    public Map<String, Set<String>> copyTo() {
        return copyTo;
    }

    public AnalyzerService analyzerService() {
        return analyzerService;
    }
}
