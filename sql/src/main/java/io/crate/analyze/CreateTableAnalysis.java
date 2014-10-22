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

import io.crate.PartitionName;
import io.crate.exceptions.TableAlreadyExistsException;
import io.crate.exceptions.TableUnknownException;
import io.crate.metadata.ColumnIdent;
import io.crate.metadata.FulltextAnalyzerResolver;
import io.crate.metadata.ReferenceInfos;
import io.crate.metadata.TableIdent;
import io.crate.metadata.table.ColumnPolicy;
import io.crate.metadata.table.SchemaInfo;
import io.crate.metadata.table.TableInfo;
import org.elasticsearch.common.settings.ImmutableSettings;
import org.elasticsearch.common.settings.Settings;

import javax.annotation.Nullable;
import java.util.List;
import java.util.Map;

public class CreateTableAnalysis extends AbstractDDLAnalysis {

    private final ImmutableSettings.Builder indexSettingsBuilder = ImmutableSettings.builder();

    protected final ReferenceInfos referenceInfos;
    protected final FulltextAnalyzerResolver fulltextAnalyzerResolver;
    private AnalyzedTableElements analyzedTableElements;
    private Settings builtSettings;
    private Map<String, Object> mapping;
    private ColumnIdent routingColumn;
    private ColumnPolicy columnPolicy = ColumnPolicy.DYNAMIC; // Default for doc tables

    public CreateTableAnalysis(ReferenceInfos referenceInfos,
                               FulltextAnalyzerResolver fulltextAnalyzerResolver,
                               Analyzer.ParameterContext parameterContext) {
        super(parameterContext);
        this.referenceInfos = referenceInfos;
        this.fulltextAnalyzerResolver = fulltextAnalyzerResolver;
    }

    @Override
    public void table(TableIdent tableIdent) {
        try {
            TableInfo existingTable = referenceInfos.getTableInfoUnsafe(tableIdent);
            // no exception thrown, table exists
            // is it an orphaned alias? allow it,
            // as it will be deleted before the actual table creation
            if (!isOrphanedAlias(existingTable)) {
                throw new TableAlreadyExistsException(existingTable.ident().name());
            }
        } catch (TableUnknownException e) {
            // ok, that is expected
        }
        super.table(tableIdent); // name validated here
    }

    @Override
    public TableInfo table() {
        return null;
    }

    @Override
    public SchemaInfo schema() {
        return null;
    }

    /**
     * checks if the given TableInfo has been created from an orphaned alias left from
     * an incomplete drop table on a partitioned table
     */
    private boolean isOrphanedAlias(TableInfo table) {
        if (!table.isPartitioned() && table.isAlias()
                && table.concreteIndices().length >= 1) {

            boolean isPartitionAlias = true;
            for (String index : table.concreteIndices()) {
                if (!PartitionName.isPartition(index, table.ident().name())) {
                    isPartitionAlias = false;
                    break;
                }
            }
            return isPartitionAlias;
        }
        return false;

    }

    @Override
    public void normalize() {

    }

    @Override
    public <C, R> R accept(AnalysisVisitor<C, R> analysisVisitor, C context) {
        return analysisVisitor.visitCreateTableAnalysis(this, context);
    }

    public List<List<String>> partitionedBy() {
        return analyzedTableElements().partitionedBy();
    }

    public boolean isPartitioned() {
        return !analyzedTableElements().partitionedByColumns.isEmpty();
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
        if (builtSettings == null) {
            indexSettingsBuilder.put(analyzedTableElements.settings());
            builtSettings = indexSettingsBuilder.build();
        }
        return builtSettings;
    }

    @SuppressWarnings("unchecked")
    public Map<String, Object> mappingProperties() {
        return (Map) mapping().get("properties");
    }

    public List<String> primaryKeys() {
        return analyzedTableElements.primaryKeys();
    }

    public Map<String, Object> mapping() {
        if (mapping == null) {
            mapping = analyzedTableElements.toMapping();
            if (routingColumn != null) {
                ((Map) mapping.get("_meta")).put("routing", routingColumn.fqn());
            }
            if (columnPolicy != null) {
                mapping.put("dynamic", columnPolicy.mappingValue());
            }
        }
        return mapping;
    }

    public FulltextAnalyzerResolver fulltextAnalyzerResolver() {
        return fulltextAnalyzerResolver;
    }

    public TableIdent tableIdent() {
        return tableIdent;
    }

    public void routing(ColumnIdent routingColumn) {
        if (routingColumn.name().equalsIgnoreCase("_id")) {
            return;
        }
        this.routingColumn = routingColumn;
    }

    public @Nullable ColumnIdent routing() {
        return routingColumn;
    }

    /**
     * return true if a columnDefinition with name <code>columnName</code> exists
     */
    public boolean hasColumnDefinition(ColumnIdent columnIdent) {
        return (analyzedTableElements().columnIdents().contains(columnIdent) ||
                columnIdent.name().equalsIgnoreCase("_id"));
    }

    @Override
    public boolean isData() {
        // TODO: remove CreateTableAnalysis from Planner and extend DDLVisitor in the Transport
        return true;
    }

    public void analyzedTableElements(AnalyzedTableElements analyze) {
        this.analyzedTableElements = analyze;
    }

    public AnalyzedTableElements analyzedTableElements() {
        return analyzedTableElements;
    }

    public void columnPolicy(ColumnPolicy columnPolicy) {
        this.columnPolicy = columnPolicy;
    }

    public ColumnPolicy columnPolicy() {
        return columnPolicy;
    }
}
