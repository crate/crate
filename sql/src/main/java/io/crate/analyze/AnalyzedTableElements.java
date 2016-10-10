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

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import io.crate.action.sql.SessionContext;
import io.crate.analyze.expressions.ExpressionAnalysisContext;
import io.crate.analyze.expressions.ExpressionAnalyzer;
import io.crate.analyze.expressions.TableReferenceResolver;
import io.crate.analyze.symbol.Symbol;
import io.crate.analyze.symbol.format.SymbolPrinter;
import io.crate.exceptions.ColumnUnknownException;
import io.crate.metadata.*;
import io.crate.metadata.table.TableInfo;
import io.crate.operation.scalar.cast.CastFunctionResolver;
import io.crate.types.DataType;
import io.crate.types.DataTypes;
import org.elasticsearch.common.settings.Settings;

import javax.annotation.Nullable;
import java.util.*;

public class AnalyzedTableElements {

    List<AnalyzedColumnDefinition> partitionedByColumns = new ArrayList<>();
    private List<AnalyzedColumnDefinition> columns = new ArrayList<>();
    private Set<ColumnIdent> columnIdents = new HashSet<>();
    private Map<ColumnIdent, String> columnTypes = new HashMap<>();
    private Set<String> primaryKeys;
    private Set<String> notNullColumns;
    private List<List<String>> partitionedBy;
    private int numGeneratedColumns = 0;


    /**
     * additional primary keys that are not inline with a column definition
     */
    private Set<String> additionalPrimaryKeys = new LinkedHashSet<>();
    private Map<String, Set<String>> copyToMap = new HashMap<>();


    public Map<String, Object> toMapping() {
        Map<String, Object> mapping = new HashMap<>();
        Map<String, Object> meta = new HashMap<>();
        Map<String, Object> properties = new HashMap<>(columns.size());

        Map<String, String> generatedColumns = new HashMap<>();
        Map<String, Object> indicesMap = new HashMap<>();
        for (AnalyzedColumnDefinition column : columns) {
            properties.put(column.name(), column.toMapping());
            if (column.isIndex()) {
                indicesMap.put(column.name(), column.toMetaIndicesMapping());
            }
            if (column.formattedGeneratedExpression() != null) {
                generatedColumns.put(column.name(), column.formattedGeneratedExpression());
            }
        }

        if (!partitionedByColumns.isEmpty()) {
            meta.put("partitioned_by", partitionedBy());
        }
        if (!indicesMap.isEmpty()) {
            meta.put("indices", indicesMap);
        }
        if (!primaryKeys().isEmpty()) {
            meta.put("primary_keys", primaryKeys());
        }
        if (!generatedColumns.isEmpty()) {
            meta.put("generated_columns", generatedColumns);
        }
        if (!notNullColumns().isEmpty()) {
            Map<String, Object> constraints = new HashMap<>();
            constraints.put("not_null", notNullColumns());
            meta.put("constraints", constraints);
        }

        mapping.put("_meta", meta);
        mapping.put("properties", properties);
        mapping.put("_all", ImmutableMap.of("enabled", false));

        return mapping;
    }

    public List<List<String>> partitionedBy() {
        if (partitionedBy == null) {
            partitionedBy = new ArrayList<>(partitionedByColumns.size());
            for (AnalyzedColumnDefinition partitionedByColumn : partitionedByColumns) {
                partitionedBy.add(ImmutableList.of(
                    partitionedByColumn.ident().fqn(),
                    partitionedByColumn.dataType())
                );
            }
        }

        return partitionedBy;
    }

    private void expandColumnIdents() {
        for (AnalyzedColumnDefinition column : columns) {
            expandColumn(column);
        }
    }

    private void expandColumn(AnalyzedColumnDefinition column) {
        if (column.isIndex()) {
            columnIdents.remove(column.ident());
            return;
        }

        columnIdents.add(column.ident());
        columnTypes.put(column.ident(), column.dataType());
        for (AnalyzedColumnDefinition child : column.children()) {
            expandColumn(child);
        }
    }

    Set<String> notNullColumns() {
        if (notNullColumns == null) {
            notNullColumns = new HashSet<>();
            for (AnalyzedColumnDefinition column : columns) {
                String fqn = column.ident().fqn();
                if (column.isNotNull() && !primaryKeys().contains(fqn)) { // Columns part of pk are implicitly not null
                    notNullColumns.add(fqn);
                }
            }
        }
        return notNullColumns;
    }

    public Set<String> primaryKeys() {
        if (primaryKeys == null) {
            primaryKeys = new LinkedHashSet<>(); // To preserve order
            primaryKeys.addAll(additionalPrimaryKeys);
            for (AnalyzedColumnDefinition column : columns) {
                addPrimaryKeys(primaryKeys, column);
            }
        }
        return primaryKeys;
    }

    private static void addPrimaryKeys(Set<String> primaryKeys, AnalyzedColumnDefinition column) {
        if (column.isPrimaryKey()) {
            String fqn = column.ident().fqn();
            checkPrimaryKeyAlreadyDefined(primaryKeys, fqn);
            primaryKeys.add(fqn);
        }
        for (AnalyzedColumnDefinition analyzedColumnDefinition : column.children()) {
            addPrimaryKeys(primaryKeys, analyzedColumnDefinition);
        }
    }

    private static void checkPrimaryKeyAlreadyDefined(Set<String> primaryKeys, String columnName) {
        if (primaryKeys.contains(columnName)) {
            throw new IllegalArgumentException(String.format(Locale.ENGLISH,
                "Column \"%s\" appears twice in primary key constraint", columnName));
        }
    }

    public void addPrimaryKey(String fqColumnName) {
        checkPrimaryKeyAlreadyDefined(additionalPrimaryKeys, fqColumnName);
        additionalPrimaryKeys.add(fqColumnName);
    }

    public void add(AnalyzedColumnDefinition analyzedColumnDefinition) {
        if (columnIdents.contains(analyzedColumnDefinition.ident())) {
            throw new IllegalArgumentException(String.format(Locale.ENGLISH,
                "column \"%s\" specified more than once", analyzedColumnDefinition.ident().sqlFqn()));
        }
        columnIdents.add(analyzedColumnDefinition.ident());
        columns.add(analyzedColumnDefinition);
        columnTypes.put(analyzedColumnDefinition.ident(), analyzedColumnDefinition.dataType());
        if (analyzedColumnDefinition.generatedExpression() != null) {
            numGeneratedColumns++;
        }
    }

    public Settings settings() {
        Settings.Builder builder = Settings.builder();
        for (AnalyzedColumnDefinition column : columns) {
            builder.put(column.analyzerSettings());
        }
        return builder.build();
    }

    void finalizeAndValidate(TableIdent tableIdent,
                             @Nullable TableInfo tableInfo,
                             Functions functions,
                             ParameterContext parameterContext,
                             SessionContext sessionContext) {
        expandColumnIdents();
        validateGeneratedColumns(tableIdent, tableInfo, functions, parameterContext, sessionContext);
        for (AnalyzedColumnDefinition column : columns) {
            column.validate();
            addCopyToInfo(column);
        }
        validateIndexDefinitions();
        validatePrimaryKeys();
    }

    private void validateGeneratedColumns(TableIdent tableIdent,
                                          @Nullable TableInfo tableInfo,
                                          Functions functions,
                                          ParameterContext parameterContext,
                                          SessionContext sessionContext) {
        List<Reference> tableReferences = new ArrayList<>();
        for (AnalyzedColumnDefinition columnDefinition : columns) {
            buildReference(tableIdent, columnDefinition, tableReferences);
        }
        if (tableInfo != null) {
            // add existing references
            tableReferences.addAll(tableInfo.columns());
        }

        TableReferenceResolver tableReferenceResolver = new TableReferenceResolver(tableReferences);
        ExpressionAnalyzer expressionAnalyzer = new ExpressionAnalyzer(
            functions, sessionContext, parameterContext, tableReferenceResolver, null);
        SymbolPrinter printer = new SymbolPrinter(functions);
        ExpressionAnalysisContext expressionAnalysisContext = new ExpressionAnalysisContext();
        for (AnalyzedColumnDefinition columnDefinition : columns) {
            if (columnDefinition.generatedExpression() != null) {
                processGeneratedExpression(expressionAnalyzer, printer, columnDefinition, expressionAnalysisContext);
            }
        }
    }

    private void processGeneratedExpression(ExpressionAnalyzer expressionAnalyzer,
                                            SymbolPrinter symbolPrinter,
                                            AnalyzedColumnDefinition columnDefinition,
                                            ExpressionAnalysisContext expressionAnalysisContext) {
        // validate expression
        Symbol function = expressionAnalyzer.convert(columnDefinition.generatedExpression(), expressionAnalysisContext);

        String formattedExpression;
        DataType valueType = function.valueType();
        DataType definedType =
            columnDefinition.dataType() == null ? null : DataTypes.ofMappingNameSafe(columnDefinition.dataType());

        // check for optional defined type and add `cast` to expression if possible
        if (definedType != null && !definedType.equals(valueType)) {
            Preconditions.checkArgument(valueType.isConvertableTo(definedType),
                "generated expression value type '%s' not supported for conversion to '%s'", valueType, definedType.getName());

            Symbol castFunction = CastFunctionResolver.generateCastFunction(function, definedType, false);
            formattedExpression = symbolPrinter.print(castFunction, SymbolPrinter.Style.PARSEABLE_NOT_QUALIFIED); // no full qualified references here
        } else {
            columnDefinition.dataType(function.valueType().getName());
            formattedExpression = symbolPrinter.print(function, SymbolPrinter.Style.PARSEABLE_NOT_QUALIFIED); // no full qualified references here
        }

        columnDefinition.formattedGeneratedExpression(formattedExpression);
    }

    private void buildReference(TableIdent tableIdent, AnalyzedColumnDefinition columnDefinition, List<Reference> references) {
        Reference reference;
        if (columnDefinition.generatedExpression() == null) {
            reference = new Reference(
                new ReferenceIdent(tableIdent, columnDefinition.ident()),
                RowGranularity.DOC,
                DataTypes.ofMappingNameSafe(columnDefinition.dataType()));
        } else {
            reference = new GeneratedReference(
                new ReferenceIdent(tableIdent, columnDefinition.ident()),
                RowGranularity.DOC,
                columnDefinition.dataType() ==
                null ? DataTypes.UNDEFINED : DataTypes.ofMappingNameSafe(columnDefinition.dataType()),
                "dummy expression, real one not needed here");
        }
        references.add(reference);
        for (AnalyzedColumnDefinition childDefinition : columnDefinition.children()) {
            buildReference(tableIdent, childDefinition, references);
        }
    }

    private void addCopyToInfo(AnalyzedColumnDefinition column) {
        if (!column.isIndex()) {
            Set<String> targets = copyToMap.get(column.ident().fqn());
            if (targets != null) {
                column.addCopyTo(targets);
            }
        }
        for (AnalyzedColumnDefinition child : column.children()) {
            addCopyToInfo(child);
        }
    }

    private void validatePrimaryKeys() {
        for (String additionalPrimaryKey : additionalPrimaryKeys) {
            ColumnIdent columnIdent = ColumnIdent.fromPath(additionalPrimaryKey);
            if (!columnIdents.contains(columnIdent)) {
                throw new ColumnUnknownException(columnIdent.sqlFqn());
            }
        }
        // will collect both column constraint and additional defined once and check for duplicates
        primaryKeys();
    }

    private void validateIndexDefinitions() {
        for (Map.Entry<String, Set<String>> entry : copyToMap.entrySet()) {
            ColumnIdent columnIdent = ColumnIdent.fromPath(entry.getKey());
            if (!columnIdents.contains(columnIdent)) {
                throw new ColumnUnknownException(columnIdent.sqlFqn());
            }
            if (!columnTypes.get(columnIdent).equalsIgnoreCase("string")) {
                throw new IllegalArgumentException("INDEX definition only support 'string' typed source columns");
            }
        }
    }

    void addCopyTo(String sourceColumn, String targetIndex) {
        Set<String> targetColumns = copyToMap.get(sourceColumn);
        if (targetColumns == null) {
            targetColumns = new HashSet<>();
            copyToMap.put(sourceColumn, targetColumns);
        }
        targetColumns.add(targetIndex);
    }

    Set<ColumnIdent> columnIdents() {
        return columnIdents;
    }

    @Nullable
    private AnalyzedColumnDefinition columnDefinitionByIdent(ColumnIdent ident) {
        AnalyzedColumnDefinition result = null;
        ColumnIdent root = ident.getRoot();
        for (AnalyzedColumnDefinition column : columns) {
            if (column.ident().equals(root)) {
                result = column;
                break;
            }
        }
        if (result == null) {
            return null;
        }

        if (result.ident().equals(ident)) {
            return result;
        }

        return findInChildren(result, ident);
    }

    private AnalyzedColumnDefinition findInChildren(AnalyzedColumnDefinition column,
                                                    ColumnIdent ident) {
        AnalyzedColumnDefinition result = null;
        for (AnalyzedColumnDefinition child : column.children()) {
            if (child.ident().equals(ident)) {
                result = child;
                break;
            }
            AnalyzedColumnDefinition inChildren = findInChildren(child, ident);
            if (inChildren != null) {
                return inChildren;
            }
        }
        return result;
    }

    void changeToPartitionedByColumn(ColumnIdent partitionedByIdent, boolean skipIfNotFound) {
        Preconditions.checkArgument(!partitionedByIdent.name().startsWith("_"),
            "Cannot use system columns in PARTITIONED BY clause");

        // need to call primaryKeys() before the partition column is removed from the columns list
        if (!primaryKeys().isEmpty() && !primaryKeys().contains(partitionedByIdent.fqn())) {
            throw new IllegalArgumentException(String.format(Locale.ENGLISH,
                "Cannot use non primary key column '%s' in PARTITIONED BY clause if primary key is set on table",
                partitionedByIdent.sqlFqn()));
        }

        AnalyzedColumnDefinition columnDefinition = columnDefinitionByIdent(partitionedByIdent);
        if (columnDefinition == null) {
            if (skipIfNotFound) {
                return;
            }
            throw new ColumnUnknownException(partitionedByIdent.sqlFqn());
        }
        DataType columnType = DataTypes.ofMappingNameSafe(columnDefinition.dataType());
        if (!DataTypes.isPrimitive(columnType)) {
            throw new IllegalArgumentException(String.format(Locale.ENGLISH,
                "Cannot use column %s of type %s in PARTITIONED BY clause",
                columnDefinition.ident().sqlFqn(), columnDefinition.dataType()));
        }
        if (columnDefinition.isArrayOrInArray()) {
            throw new IllegalArgumentException(String.format(Locale.ENGLISH,
                "Cannot use array column %s in PARTITIONED BY clause", columnDefinition.ident().sqlFqn()));


        }
        if (columnDefinition.index().equals("analyzed")) {
            throw new IllegalArgumentException(String.format(Locale.ENGLISH,
                "Cannot use column %s with fulltext index in PARTITIONED BY clause",
                columnDefinition.ident().sqlFqn()));
        }
        columnIdents.remove(columnDefinition.ident());
        columnDefinition.index(Reference.IndexType.NO.toString());
        partitionedByColumns.add(columnDefinition);
    }

    public List<AnalyzedColumnDefinition> columns() {
        return columns;
    }

    boolean hasGeneratedColumns() {
        return numGeneratedColumns > 0;
    }
}
