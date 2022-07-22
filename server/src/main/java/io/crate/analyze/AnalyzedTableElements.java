/*
 * Licensed to Crate.io GmbH ("Crate") under one or more contributor
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

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.function.Consumer;
import java.util.function.Function;

import javax.annotation.Nullable;

import org.elasticsearch.common.UUIDs;
import org.elasticsearch.common.settings.Settings;

import io.crate.analyze.expressions.TableReferenceResolver;
import io.crate.common.annotations.VisibleForTesting;
import io.crate.exceptions.ColumnUnknownException;
import io.crate.expression.scalar.cast.CastFunctionResolver;
import io.crate.expression.symbol.RefVisitor;
import io.crate.expression.symbol.Symbol;
import io.crate.expression.symbol.SymbolVisitors;
import io.crate.expression.symbol.Symbols;
import io.crate.expression.symbol.format.Style;
import io.crate.metadata.ColumnIdent;
import io.crate.metadata.FulltextAnalyzerResolver;
import io.crate.metadata.GeneratedReference;
import io.crate.metadata.IndexType;
import io.crate.metadata.Reference;
import io.crate.metadata.ReferenceIdent;
import io.crate.metadata.RelationName;
import io.crate.metadata.RowGranularity;
import io.crate.metadata.SimpleReference;
import io.crate.sql.tree.CheckColumnConstraint;
import io.crate.sql.tree.CheckConstraint;
import io.crate.types.ArrayType;
import io.crate.types.DataType;
import io.crate.types.DataTypes;

public class AnalyzedTableElements<T> {

    public List<AnalyzedColumnDefinition<T>> partitionedByColumns = new ArrayList<>();
    private List<AnalyzedColumnDefinition<T>> columns = new ArrayList<>();
    private Set<ColumnIdent> columnIdents = new HashSet<>();
    private Map<ColumnIdent, DataType> columnTypes = new HashMap<>();
    private Set<String> primaryKeys;
    private Set<String> notNullColumns;
    private Map<String, String> checkConstraints = new LinkedHashMap<>();
    private List<List<String>> partitionedBy;
    private int numGeneratedColumns = 0;


    /**
     * additional primary keys that are not inline with a column definition
     */
    private List<T> additionalPrimaryKeys = new ArrayList<>();
    private Map<T, Set<String>> copyToMap = new HashMap<>();

    public AnalyzedTableElements() {
    }

    private AnalyzedTableElements(List<AnalyzedColumnDefinition<T>> partitionedByColumns,
                                  List<AnalyzedColumnDefinition<T>> columns,
                                  Set<ColumnIdent> columnIdents,
                                  Map<ColumnIdent, DataType> columnTypes,
                                  Set<String> primaryKeys,
                                  Set<String> notNullColumns,
                                  Map<String, String> checkConstraints,
                                  List<List<String>> partitionedBy,
                                  int numGeneratedColumns,
                                  List<T> additionalPrimaryKeys,
                                  Map<T, Set<String>> copyToMap) {
        this.partitionedByColumns = partitionedByColumns;
        this.columns = columns;
        this.columnIdents = columnIdents;
        this.columnTypes = columnTypes;
        this.primaryKeys = primaryKeys;
        this.notNullColumns = notNullColumns;
        this.checkConstraints = checkConstraints;
        this.partitionedBy = partitionedBy;
        this.numGeneratedColumns = numGeneratedColumns;
        this.additionalPrimaryKeys = additionalPrimaryKeys;
        this.copyToMap = copyToMap;
    }

    public static Map<String, Object> toMapping(AnalyzedTableElements<Object> elements) {
        final Map<String, Object> mapping = new HashMap<>();
        final Map<String, Object> meta = new HashMap<>();
        final Map<String, Object> properties = new HashMap<>(elements.columns.size());

        Map<String, String> generatedColumns = new HashMap<>();
        Map<String, Object> indicesMap = new HashMap<>();
        for (AnalyzedColumnDefinition<Object> column : elements.columns) {
            properties.put(column.name(), AnalyzedColumnDefinition.toMapping(column));
            if (column.isIndexColumn()) {
                indicesMap.put(column.name(), column.toMetaIndicesMapping());
            }
            addToGeneratedColumns("", column, generatedColumns);
        }

        if (!elements.partitionedByColumns.isEmpty()) {
            meta.put("partitioned_by", elements.partitionedBy());
        }
        if (!indicesMap.isEmpty()) {
            meta.put("indices", indicesMap);
        }
        if (!primaryKeys(elements).isEmpty()) {
            meta.put("primary_keys", primaryKeys(elements));
        }
        if (!generatedColumns.isEmpty()) {
            meta.put("generated_columns", generatedColumns);
        }
        if (!notNullColumns(elements).isEmpty()) {
            Map<String, Object> constraints = new HashMap<>();
            constraints.put("not_null", notNullColumns(elements));
            meta.put("constraints", constraints);
        }
        if (!elements.checkConstraints.isEmpty()) {
            meta.put("check_constraints", elements.checkConstraints);
        }

        mapping.put("_meta", meta);
        mapping.put("properties", properties);

        return mapping;
    }

    private static void addToGeneratedColumns(String columnPrefix,
                                              AnalyzedColumnDefinition<Object> column,
                                              Map<String, String> generatedColumns) {
        String generatedExpression = column.formattedGeneratedExpression();
        if (generatedExpression != null) {
            generatedColumns.put(columnPrefix + column.name(), generatedExpression);
        }
        for (AnalyzedColumnDefinition<Object> child : column.children()) {
            addToGeneratedColumns(columnPrefix + column.name() + '.', child, generatedColumns);
        }
    }

    public <U> AnalyzedTableElements<U> map(Function<? super T, ? extends U> mapper) {
        List<U> additionalPrimaryKeys = new ArrayList<>(this.additionalPrimaryKeys.size());
        for (T p : this.additionalPrimaryKeys) {
            additionalPrimaryKeys.add(mapper.apply(p));
        }
        Map<U, Set<String>> copyToMap = new HashMap<>(this.copyToMap.size());
        for (Map.Entry<T, Set<String>> entry : this.copyToMap.entrySet()) {
            copyToMap.put(mapper.apply(entry.getKey()), entry.getValue());
        }
        List<AnalyzedColumnDefinition<U>> partitionedByColumns = new ArrayList<>(this.partitionedByColumns.size());
        for (AnalyzedColumnDefinition<T> d : this.partitionedByColumns) {
            partitionedByColumns.add(d.map(mapper));
        }
        List<AnalyzedColumnDefinition<U>> columns = new ArrayList<>(this.columns.size());
        for (AnalyzedColumnDefinition<T> d : this.columns) {
            columns.add(d.map(mapper));
        }
        return new AnalyzedTableElements<>(
            partitionedByColumns,
            columns,
            columnIdents,
            columnTypes,
            primaryKeys,
            notNullColumns,
            checkConstraints,
            partitionedBy,
            numGeneratedColumns,
            additionalPrimaryKeys,
            copyToMap
        );
    }


    public List<List<String>> partitionedBy() {
        if (partitionedBy == null) {
            partitionedBy = new ArrayList<>(partitionedByColumns.size());
            for (AnalyzedColumnDefinition<T> partitionedByColumn : partitionedByColumns) {
                partitionedBy.add(List.of(
                    partitionedByColumn.ident().fqn(), partitionedByColumn.typeNameForESMapping()));
            }
        }

        return partitionedBy;
    }

    private void expandColumnIdents() {
        for (AnalyzedColumnDefinition<T> column : columns) {
            expandColumn(column);
        }
    }

    private void expandColumn(AnalyzedColumnDefinition<T> column) {
        if (column.isIndexColumn()) {
            columnIdents.remove(column.ident());
            return;
        }

        columnIdents.add(column.ident());
        columnTypes.put(column.ident(), column.dataType());
        for (AnalyzedColumnDefinition<T> child : column.children()) {
            expandColumn(child);
        }
    }

    static Set<String> notNullColumns(AnalyzedTableElements<Object> elements) {
        if (elements.notNullColumns == null) {
            elements.notNullColumns = new HashSet<>();
            for (AnalyzedColumnDefinition<Object> column : elements.columns) {
                addNotNullFromChildren(column, elements);
            }
        }
        return elements.notNullColumns;
    }

    /**
     * Recursively add all not null constraints from child columns (object columns)
     */
    private static void addNotNullFromChildren(AnalyzedColumnDefinition<Object> parentColumn, AnalyzedTableElements<Object> elements) {
        LinkedList<AnalyzedColumnDefinition<Object>> childColumns = new LinkedList<>();
        childColumns.add(parentColumn);

        while (!childColumns.isEmpty()) {
            AnalyzedColumnDefinition<Object> column = childColumns.remove();
            String fqn = column.ident().fqn();
            if (column.hasNotNullConstraint() && !primaryKeys(elements).contains(fqn)) { // Columns part of pk are implicitly not null
                elements.notNullColumns.add(fqn);
            }
            childColumns.addAll(column.children());
        }
    }

    public static Set<String> primaryKeys(AnalyzedTableElements<Object> elements) {
        if (elements.primaryKeys == null) {
            elements.primaryKeys = new LinkedHashSet<>(); // To preserve order
            for (Object pk : elements.additionalPrimaryKeys) {
                String pkAsString = pk.toString();
                checkPrimaryKeyAlreadyDefined(elements.primaryKeys, pkAsString);
                elements.primaryKeys.add(pkAsString);
            }
            for (AnalyzedColumnDefinition<Object> column : elements.columns) {
                elements.addPrimaryKeys(elements.primaryKeys, column);
            }
        }
        return elements.primaryKeys;
    }

    private void addPrimaryKeys(Set<String> primaryKeys, AnalyzedColumnDefinition<T> column) {
        if (column.hasPrimaryKeyConstraint()) {
            String fqn = column.ident().fqn();
            checkPrimaryKeyAlreadyDefined(primaryKeys, fqn);
            primaryKeys.add(fqn);
        }
        for (AnalyzedColumnDefinition<T> analyzedColumnDefinition : column.children()) {
            addPrimaryKeys(primaryKeys, analyzedColumnDefinition);
        }
    }

    private static void checkPrimaryKeyAlreadyDefined(Set<String> primaryKeys, String columnName) {
        if (primaryKeys.contains(columnName)) {
            throw new IllegalArgumentException(String.format(Locale.ENGLISH,
                                                             "Column \"%s\" appears twice in primary key constraint", columnName));
        }
    }

    void addPrimaryKey(T fqColumnName) {
        additionalPrimaryKeys.add(fqColumnName);
    }

    public void add(AnalyzedColumnDefinition<T> analyzedColumnDefinition) {
        if (columnIdents.contains(analyzedColumnDefinition.ident())) {
            throw new IllegalArgumentException(String.format(Locale.ENGLISH,
                                                             "column \"%s\" specified more than once", analyzedColumnDefinition.ident().sqlFqn()));
        }
        columnIdents.add(analyzedColumnDefinition.ident());
        columns.add(analyzedColumnDefinition);
        columnTypes.put(analyzedColumnDefinition.ident(), analyzedColumnDefinition.dataType());
        if (analyzedColumnDefinition.isGenerated()) {
            numGeneratedColumns++;
        }
    }

    public static Settings validateAndBuildSettings(AnalyzedTableElements<Object> tableElementsEvaluated,
                                                    FulltextAnalyzerResolver fulltextAnalyzerResolver) {
        Settings.Builder builder = Settings.builder();
        for (AnalyzedColumnDefinition<Object> column : tableElementsEvaluated.columns) {
            AnalyzedColumnDefinition.applyAndValidateAnalyzerSettings(column, fulltextAnalyzerResolver);
            builder.put(column.builtAnalyzerSettings());
        }
        return builder.build();
    }

    public static void finalizeAndValidate(RelationName relationName,
                                           AnalyzedTableElements<Symbol> tableElementsWithExpressionSymbols,
                                           AnalyzedTableElements<Object> tableElementsEvaluated) {
        tableElementsEvaluated.expandColumnIdents();
        validateExpressions(tableElementsWithExpressionSymbols, tableElementsEvaluated);
        for (AnalyzedColumnDefinition<Object> column : tableElementsEvaluated.columns) {
            column.validate();
            tableElementsEvaluated.addCopyToInfo(column);
        }
        validateIndexDefinitions(relationName, tableElementsEvaluated);
        validatePrimaryKeys(relationName, tableElementsEvaluated);
    }

    private static void validateExpressions(AnalyzedTableElements<Symbol> tableElementsWithExpressionSymbols,
                                            AnalyzedTableElements<Object> tableElementsEvaluated) {
        for (int i = 0; i < tableElementsWithExpressionSymbols.columns.size(); i++) {
            processExpressions(
                tableElementsWithExpressionSymbols.columns.get(i),
                tableElementsEvaluated.columns.get(i)
            );
        }
    }

    public TableReferenceResolver referenceResolver(RelationName relationName) {
        List<Reference> tableReferences = new ArrayList<>();
        for (AnalyzedColumnDefinition<T> columnDefinition : columns) {
            buildReference(relationName, columnDefinition, tableReferences);
        }
        return new TableReferenceResolver(tableReferences, relationName);
    }

    private static void processExpressions(AnalyzedColumnDefinition<Symbol> columnDefinitionWithExpressionSymbols,
                                           AnalyzedColumnDefinition<Object> columnDefinitionEvaluated) {
        Symbol generatedExpression = columnDefinitionWithExpressionSymbols.generatedExpression();
        if (generatedExpression != null) {
            validateAndFormatExpression(
                generatedExpression,
                columnDefinitionWithExpressionSymbols,
                columnDefinitionEvaluated,
                columnDefinitionEvaluated::formattedGeneratedExpression);
        }
        Symbol defaultExpression = columnDefinitionWithExpressionSymbols.defaultExpression();
        if (defaultExpression != null) {
            RefVisitor.visitRefs(defaultExpression, r -> {
                throw new UnsupportedOperationException(
                    "Columns cannot be used in this context. " +
                    "Maybe you wanted to use a string literal which requires single quotes: '" + r.column().sqlFqn() + "'");
            });
            validateAndFormatExpression(
                defaultExpression,
                columnDefinitionWithExpressionSymbols,
                columnDefinitionEvaluated,
                columnDefinitionEvaluated::formattedDefaultExpression);
        }
        for (int i = 0; i < columnDefinitionWithExpressionSymbols.children().size(); i++) {
            processExpressions(
                columnDefinitionWithExpressionSymbols.children().get(i),
                columnDefinitionEvaluated.children().get(i)
            );
        }
    }

    private static void validateAndFormatExpression(Symbol function,
                                                    AnalyzedColumnDefinition<Symbol> columnDefinitionWithExpressionSymbols,
                                                    AnalyzedColumnDefinition<Object> columnDefinitionEvaluated,
                                                    Consumer<String> formattedExpressionConsumer) {
        String formattedExpression;
        DataType<?> valueType = function.valueType();
        DataType<?> definedType = columnDefinitionWithExpressionSymbols.dataType();

        if (SymbolVisitors.any(Symbols::isAggregate, function)) {
            throw new UnsupportedOperationException("Aggregation functions are not allowed in generated columns: " + function);
        }

        // check for optional defined type and add `cast` to expression if possible
        if (definedType != null && !definedType.equals(valueType)) {
            final DataType<?> columnDataType;
            if (ArrayType.NAME.equals(columnDefinitionWithExpressionSymbols.collectionType())) {
                columnDataType = new ArrayType<>(definedType);
            } else {
                columnDataType = definedType;
            }
            if (!valueType.isConvertableTo(columnDataType, false)) {
                throw new IllegalArgumentException(String.format(Locale.ENGLISH,
                    "expression value type '%s' not supported for conversion to '%s'",
                    valueType, columnDataType.getName())
                );
            }

            Symbol castFunction = CastFunctionResolver.generateCastFunction(function, columnDataType);
            formattedExpression = castFunction.toString(Style.UNQUALIFIED);
        } else {
            if (valueType instanceof ArrayType) {
                columnDefinitionEvaluated.collectionType(ArrayType.NAME);
                columnDefinitionEvaluated.dataType(ArrayType.unnest(valueType));
            } else {
                columnDefinitionEvaluated.dataType(valueType);
            }
            formattedExpression = function.toString(Style.UNQUALIFIED);
        }
        formattedExpressionConsumer.accept(formattedExpression);
    }

    private static <T> void buildReference(RelationName relationName,
                                           AnalyzedColumnDefinition<T> columnDefinition,
                                           List<Reference> references) {

        DataType<?> type = columnDefinition.dataType() == null ? DataTypes.UNDEFINED : columnDefinition.dataType();
        DataType<?> realType = ArrayType.NAME.equals(columnDefinition.collectionType())
            ? new ArrayType<>(type)
            : type;

        SimpleReference simpleRef = new SimpleReference(
            new ReferenceIdent(relationName, columnDefinition.ident()),
            RowGranularity.DOC,
            realType,
            columnDefinition.position,
            null // not required in this context
        );
        Reference ref = columnDefinition.isGenerated()
            ? new GeneratedReference(simpleRef, columnDefinition.formattedGeneratedExpression(), null)
            : simpleRef;
        references.add(ref);
        for (AnalyzedColumnDefinition<T> childDefinition : columnDefinition.children()) {
            buildReference(relationName, childDefinition, references);
        }
    }

    private void addCopyToInfo(AnalyzedColumnDefinition<T> column) {
        if (!column.isIndexColumn()) {
            Set<String> targets = copyToMap.get(column.ident().fqn());
            if (targets != null) {
                column.addCopyTo(targets);
            }
        }
        for (AnalyzedColumnDefinition<T> child : column.children()) {
            addCopyToInfo(child);
        }
    }

    private static void validatePrimaryKeys(RelationName relationName, AnalyzedTableElements<Object> elements) {
        for (Object additionalPrimaryKey : elements.additionalPrimaryKeys) {
            ColumnIdent columnIdent = ColumnIdent.fromPath(additionalPrimaryKey.toString());
            if (!elements.columnIdents.contains(columnIdent)) {
                throw new ColumnUnknownException(columnIdent.sqlFqn(), relationName);
            }
        }
        // will collect both column constraint and additional defined once and check for duplicates
        primaryKeys(elements);
    }

    private static void validateIndexDefinitions(RelationName relationName, AnalyzedTableElements<Object> tableElements) {
        for (Map.Entry<Object, Set<String>> entry : tableElements.copyToMap.entrySet()) {
            ColumnIdent columnIdent = ColumnIdent.fromPath(entry.getKey().toString());
            if (!tableElements.columnIdents.contains(columnIdent)) {
                throw new ColumnUnknownException(columnIdent.sqlFqn(), relationName);
            }
            if (!DataTypes.STRING.equals(tableElements.columnTypes.get(columnIdent))) {
                throw new IllegalArgumentException("INDEX definition only support 'string' typed source columns");
            }
        }
    }

    void addCopyTo(T sourceColumn, String targetIndex) {
        Set<String> targetColumns = copyToMap.get(sourceColumn);
        if (targetColumns == null) {
            targetColumns = new HashSet<>();
            copyToMap.put(sourceColumn, targetColumns);
        }
        targetColumns.add(targetIndex);
    }

    public Set<ColumnIdent> columnIdents() {
        return columnIdents;
    }

    @Nullable
    private static AnalyzedColumnDefinition<Object> columnDefinitionByIdent(AnalyzedTableElements<Object> elements, ColumnIdent ident) {
        AnalyzedColumnDefinition<Object> result = null;
        ColumnIdent root = ident.getRoot();
        for (AnalyzedColumnDefinition<Object> column : elements.columns) {
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

    private static AnalyzedColumnDefinition<Object> findInChildren(AnalyzedColumnDefinition<Object> column,
                                                                   ColumnIdent ident) {
        AnalyzedColumnDefinition<Object> result = null;
        for (AnalyzedColumnDefinition<Object> child : column.children()) {
            if (child.ident().equals(ident)) {
                result = child;
                break;
            }
            AnalyzedColumnDefinition<Object> inChildren = findInChildren(child, ident);
            if (inChildren != null) {
                return inChildren;
            }
        }
        return result;
    }

    public static void changeToPartitionedByColumn(AnalyzedTableElements<Object> elements,
                                                   ColumnIdent partitionedByIdent,
                                                   boolean skipIfNotFound,
                                                   RelationName relationName) {
        if (partitionedByIdent.name().startsWith("_")) {
            throw new IllegalArgumentException("Cannot use system columns in PARTITIONED BY clause");
        }

        // need to call primaryKeys() before the partition column is removed from the columns list
        if (!primaryKeys(elements).isEmpty() && !primaryKeys(elements).contains(partitionedByIdent.fqn())) {
            throw new IllegalArgumentException(String.format(Locale.ENGLISH,
                                                             "Cannot use non primary key column '%s' in PARTITIONED BY clause if primary key is set on table",
                                                             partitionedByIdent.sqlFqn()));
        }

        AnalyzedColumnDefinition<Object> columnDefinition = columnDefinitionByIdent(elements, partitionedByIdent);
        if (columnDefinition == null) {
            if (skipIfNotFound) {
                return;
            }
            throw new ColumnUnknownException(partitionedByIdent.sqlFqn(), relationName);
        }
        DataType<?> columnType = columnDefinition.dataType();
        if (!DataTypes.isPrimitive(columnType)) {
            throw new IllegalArgumentException(String.format(Locale.ENGLISH,
                                                             "Cannot use column %s of type %s in PARTITIONED BY clause",
                                                             columnDefinition.ident().sqlFqn(), columnDefinition.dataType()));
        }
        if (columnDefinition.isArrayOrInArray()) {
            throw new IllegalArgumentException(String.format(Locale.ENGLISH,
                                                             "Cannot use array column %s in PARTITIONED BY clause", columnDefinition.ident().sqlFqn()));


        }
        if (columnDefinition.indexConstraint() == IndexType.FULLTEXT) {
            throw new IllegalArgumentException(String.format(Locale.ENGLISH,
                                                             "Cannot use column %s with fulltext index in PARTITIONED BY clause",
                                                             columnDefinition.ident().sqlFqn()));
        }
        elements.columnIdents.remove(columnDefinition.ident());
        columnDefinition.indexConstraint(IndexType.NONE);
        elements.partitionedByColumns.add(columnDefinition);
    }

    public List<AnalyzedColumnDefinition<T>> columns() {
        return columns;
    }

    private void addCheckConstraint(String fqRelationName,
                                    @Nullable String columnName,
                                    @Nullable String name,
                                    String expressionStr) {
        String uniqueName = name;
        if (uniqueName == null) {
            uniqueName = uniqueCheckConstraintName(fqRelationName, columnName);
        }
        if (checkConstraints.put(uniqueName, expressionStr) != null) {
            throw new IllegalArgumentException(String.format(
                Locale.ENGLISH, "a check constraint of the same name is already declared [%s]", uniqueName));
        }
    }

    private static String uniqueCheckConstraintName(String fqTableName, @Nullable String columnName) {
        StringBuilder sb = new StringBuilder(fqTableName.replaceAll("\\.", "_"));
        if (columnName != null) {
            sb.append("_").append(columnName);
        }
        sb.append("_check_");
        String uuid = UUIDs.dirtyUUID().toString();
        int idx = uuid.lastIndexOf("-");
        sb.append(idx > 0 ? uuid.substring(idx + 1) : uuid);
        return sb.toString();
    }

    public void addCheckConstraint(RelationName relationName, CheckConstraint<?> check) {
        addCheckConstraint(relationName.fqn(), check.columnName(), check.name(), check.expressionStr());
    }

    public void addCheckColumnConstraint(RelationName relationName, CheckColumnConstraint<?> check) {
        addCheckConstraint(relationName.fqn(), check.columnName(), check.name(), check.expressionStr());
    }

    @VisibleForTesting
    public Map<String, String> getCheckConstraints() {
        return Map.copyOf(checkConstraints);
    }

    public boolean hasGeneratedColumns() {
        return numGeneratedColumns > 0;
    }

}
