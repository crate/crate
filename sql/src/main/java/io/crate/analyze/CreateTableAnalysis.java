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

import com.google.common.base.Function;
import com.google.common.base.Predicate;
import com.google.common.base.Splitter;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Iterables;
import io.crate.PartitionName;
import io.crate.core.StringUtils;
import io.crate.exceptions.TableAlreadyExistsException;
import io.crate.exceptions.TableUnknownException;
import io.crate.metadata.FulltextAnalyzerResolver;
import io.crate.metadata.ReferenceInfos;
import io.crate.metadata.TableIdent;
import io.crate.metadata.table.SchemaInfo;
import io.crate.metadata.table.TableInfo;
import org.elasticsearch.common.settings.ImmutableSettings;
import org.elasticsearch.common.settings.Settings;

import javax.annotation.Nullable;
import java.util.*;

public class CreateTableAnalysis extends AbstractDDLAnalysis {

    private final ImmutableSettings.Builder indexSettingsBuilder = ImmutableSettings.builder();
    private final Map<String, Object> mappingProperties = new HashMap<>();
    private final Map<String, Object> metaIndices = new HashMap<>();
    private final Map<String, Object> metaColumns = new HashMap<>();
    private final List<String> primaryKeys = new ArrayList<>();
    private final List<List<String>> partitionedBy = new ArrayList<>();

    private final Map<String, Object> mapping = new HashMap<>();
    private final Map<String, Set<String>> copyTo = new HashMap<>();

    private final Stack<ColumnSchema> schemaStack = new Stack<>();


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
    protected final Map<String, Object> crateMeta;
    protected final ReferenceInfos referenceInfos;
    protected final FulltextAnalyzerResolver fulltextAnalyzerResolver;

    public CreateTableAnalysis(ReferenceInfos referenceInfos,
                               FulltextAnalyzerResolver fulltextAnalyzerResolver,
                               Object[] parameters) {
        super(parameters);
        this.referenceInfos = referenceInfos;
        this.fulltextAnalyzerResolver = fulltextAnalyzerResolver;

        crateMeta = new HashMap<>();
        crateMeta.put("primary_keys", primaryKeys);
        crateMeta.put("columns", metaColumns);
        crateMeta.put("indices", metaIndices);
        crateMeta.put("partitioned_by", partitionedBy);

        mapping.put("_meta", crateMeta);
        mapping.put("properties", mappingProperties);
        mapping.put("_all", ImmutableMap.of("enabled", false));

        schemaStack.push(new ColumnSchema(null, mappingProperties, metaColumns));
    }

    @Override
    public void table(TableIdent tableIdent) {
        try {
            referenceInfos.getTableInfoUnsafe(tableIdent);
            // no exception thrown, table exists
            throw new TableAlreadyExistsException(tableIdent.name());
        } catch (TableUnknownException e) {
            super.table(tableIdent); // name validated here
        }
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

    public List<List<String>> partitionedBy() {
        return partitionedBy;
    }

    public boolean isPartitioned() {
        return partitionedBy.size() > 0;
    }

    public void addPartitionedByColumn(String column, String type) {
        this.partitionedBy.add(Arrays.asList(column, type));
    }

    /**
     * name of the template to create
     * @return the name of the template to create or <code>null</code>
     *         if no template is created
     */
    public @Nullable String templateName() {
        if (isPartitioned()) {
            return PartitionName.templateName(tableIdent().name());
        }
        return null;
    }

    /**
     * template prefix to match against index names to which
     * this template should be applied
     * @return a template prefix for matching index names or null
     *         if no template is created
     */
    public @Nullable String templatePrefix() {
        if (isPartitioned()) {
            return templateName() + "*";
        }
        return null;
    }

    public ImmutableSettings.Builder indexSettingsBuilder() {
        return indexSettingsBuilder;
    }

    public Settings indexSettings() {
        return indexSettingsBuilder.build();
    }

    public Map<String, Object> mappingProperties() {
        return mappingProperties;
    }

    public void addPrimaryKey(String columnName) {
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

    public FulltextAnalyzerResolver analyzerService() {
        return fulltextAnalyzerResolver;
    }

    public TableIdent tableIdent() {
        return tableIdent;
    }

    public void routing(String routingPath) {
        if (routingPath.equalsIgnoreCase("_id")) {
            return;
        }
        crateMeta.put("routing", routingPath);
    }

    public @Nullable String routing() {
        return (String)crateMeta.get("routing");
    }

    /**
     * return true if a columnDefinition with name <code>columnName</code> exists
     * as top level column or nested (if <code>columnName</code> contains a dot)
     *
     * @param columnName (dotted) column name
     */
    public boolean hasColumnDefinition(String columnName) {
        if (columnName.equalsIgnoreCase("_id")) { return true; }
        return getColumnDefinition(columnName) != null;
    }

    @SuppressWarnings("unchecked")
    public @Nullable
    Map<String, Object> getColumnDefinition(String columnName) {
        return getColumnDefinition(columnName, false);
    }

    /**
     * like Map.remove(key)
     * @param columnName
     * @return
     */
    public @Nullable Map<String, Object> popColumnDefinition(String columnName) {
        return getColumnDefinition(columnName, true);
    }

    private @Nullable Map<String, Object> getColumnDefinition(String columnName, boolean remove) {
        if (metaIndices.containsKey(columnName)) {
            return null;  // ignore fulltext index columns
        }

        Map<String, Object> parentProperties = null;
        Map<String, Object> columnsMeta = this.metaColumns;
        Map<String, Object> properties = mappingProperties;
        List<String> columPath = Splitter.on('.').splitToList(columnName);
        for (String namePart : columPath) {
            Map<String, Object> fieldMapping = (Map<String, Object>)properties.get(namePart);
            if (fieldMapping == null) {
                return null;
            } else if (fieldMapping.get("type") != null) {
                parentProperties = properties;
                if (fieldMapping.get("type").equals("object")
                        && fieldMapping.get("properties") != null) {
                    properties = (Map<String, Object>)fieldMapping.get("properties");
                    if (columnsMeta != null) {
                        columnsMeta = (Map<String, Object>) columnsMeta.get("properties");
                    }
                } else {
                    properties = fieldMapping;
                }
            }
        }
        if (parentProperties != null && remove) {
            String lastPath = columPath.get(columPath.size()-1);
            parentProperties.remove(lastPath);
            if (columnsMeta != null) {
                columnsMeta.remove(lastPath);
            }
        }
        return properties;
    }



    /**
     * return true if column with name <code>columnName</code> is an array
     * or is a nested column inside an array, false otherwise
     */
    @SuppressWarnings("unchecked")
    public boolean isInArray(String columnName) {
        Map<String, Object> metaColumnInfo = metaColumns;
        for (String namePart : Splitter.on('.').split(columnName)) {
            Map<String, Object> columnInfo = (Map<String, Object>) metaColumnInfo.get(namePart);
            if (columnInfo != null) {
                if (columnInfo.get("collection_type") != null
                        && columnInfo.get("collection_type").equals("array")) {
                    return true;
                } else if (columnInfo.get("properties") != null) {
                    metaColumnInfo = (Map<String, Object>) columnInfo.get("properties");
                }
            }
        }
        return false;
    }


    @Override
    public boolean isData() {
        // TODO: remove CreateTableAnalysis from Planner and extend DDLVisitor in the Transport
        return true;
    }

    public ColumnSchema pushColumn(String ident) {
        ColumnSchema columnSchema = schemaStack.peek();

        if (columnSchema.crateMeta.containsKey(ident)
                || columnSchema.esMapping.containsKey(ident)) {
            throw new IllegalArgumentException(
                    String.format(Locale.ENGLISH, "column '%s' specified more than once", ident));
        }

        Map<String, Object> esMapping = new HashMap<>();
        Map<String, Object> crateMeta = new HashMap<>();
        columnSchema.crateMeta.put(ident, crateMeta);
        columnSchema.esMapping.put(ident, esMapping);

        return schemaStack.push(new ColumnSchema(ident, esMapping, crateMeta));
    }

    public ColumnSchema pushIndex(String ident) {
        if (metaIndices.containsKey(ident)) {
            throw new IllegalArgumentException(
                    String.format(Locale.ENGLISH, "the index name \"%s\" is already in use!", ident));
        }
        metaIndices.put(ident, ImmutableMap.of());
        ColumnSchema columnSchema = schemaStack.peek();
        Map<String, Object> esMapping = new HashMap<>();
        columnSchema.esMapping.put(ident, esMapping);

        return schemaStack.push(new ColumnSchema(ident, esMapping, null));
    }

    public Map<String, Object> currentColumnDefinition() {
        return schemaStack.peek().esMapping;
    }

    public Map<String, Object> currentMetaColumnDefinition() {
        return schemaStack.peek().crateMeta;
    }

    public String currentColumnName() {
        return schemaStack.peek().name;
    }

    public String currentFullQualifiedColumnName() {
        return StringUtils.PATH_JOINER.join(Iterables.filter(Iterables.transform(schemaStack, new Function<ColumnSchema, String>() {
            @Nullable
            @Override
            public String apply(@Nullable ColumnSchema input) {
                if (input == null) {
                    return null;
                } else {
                    return input.name;
                }
            }
        }), new Predicate<String>() {
            @Override
            public boolean apply(@Nullable String input) {
                return input != null;
            }
        }));
    }


    public ColumnSchema pop() {
        return schemaStack.pop();
    }

    public ColumnSchema pushNestedProperties() {
        ColumnSchema currentSchema = schemaStack.peek();
        Map<String, Object> nestedProperties = new HashMap<>();
        Map<String, Object> nestedMetaProperties = new HashMap<>();
        currentSchema.esMapping.put("properties", nestedProperties);
        currentSchema.crateMeta.put("properties", nestedMetaProperties);
        return schemaStack.push(new ColumnSchema(null, nestedProperties, nestedMetaProperties));
    }

    static class ColumnSchema {
        final String name;
        final Map<String, Object> crateMeta;
        final Map<String, Object> esMapping;

        public ColumnSchema(String name, Map<String, Object> esMapping, Map<String, Object> crateMeta) {
            this.name = name;
            this.esMapping = esMapping;
            this.crateMeta = crateMeta;
        }
    }
}
