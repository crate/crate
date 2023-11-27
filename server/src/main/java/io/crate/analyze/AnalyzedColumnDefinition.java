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
import java.util.List;
import java.util.Locale;
import java.util.Objects;
import java.util.Set;
import java.util.function.Consumer;
import java.util.function.Function;

import org.elasticsearch.common.settings.Settings;
import org.jetbrains.annotations.Nullable;

import io.crate.common.annotations.VisibleForTesting;
import io.crate.common.collections.Lists2;
import io.crate.expression.symbol.Symbol;
import io.crate.expression.symbol.SymbolVisitors;
import io.crate.expression.symbol.Symbols;
import io.crate.metadata.ColumnIdent;
import io.crate.metadata.FulltextAnalyzerResolver;
import io.crate.metadata.IndexType;
import io.crate.sql.tree.ColumnPolicy;
import io.crate.sql.tree.GenericProperties;
import io.crate.types.ArrayType;
import io.crate.types.DataType;
import io.crate.types.DataTypes;
import io.crate.types.ObjectType;
import io.crate.types.StringType;

public class AnalyzedColumnDefinition<T> {

    private static final Set<Integer> UNSUPPORTED_PK_TYPE_IDS = Set.of(
        ObjectType.ID,
        DataTypes.GEO_POINT.id(),
        DataTypes.GEO_SHAPE.id()
    );

    public static final Set<Integer> UNSUPPORTED_INDEX_TYPE_IDS = Set.of(
        ObjectType.ID,
        DataTypes.GEO_POINT.id(),
        DataTypes.GEO_SHAPE.id()
    );

    public static final String COLUMN_STORE_PROPERTY = "columnstore";

    private final AnalyzedColumnDefinition<T> parent;
    private ColumnIdent ident;
    private String name;
    private DataType<?> dataType = DataTypes.UNDEFINED;

    private String geoTree;
    @Nullable
    private GenericProperties<T> geoProperties;

    private IndexType indexType = IndexType.PLAIN;
    @Nullable
    private T analyzer;
    private String indexMethod;
    private Settings analyzerSettings = Settings.EMPTY;

    @VisibleForTesting
    ColumnPolicy columnPolicy = ColumnPolicy.DYNAMIC;

    private boolean isPrimaryKey = false;
    private boolean isNotNull = false;
    private boolean docValues;


    private List<AnalyzedColumnDefinition<T>> children = new ArrayList<>();
    private boolean isIndex = false;
    private List<String> sources = new ArrayList<>();
    private boolean isParentColumn;
    @Nullable
    private GenericProperties<T> storageProperties;

    private boolean generated;

    @Nullable
    private String formattedGeneratedExpression;
    @Nullable
    private T generatedExpression;

    @Nullable
    private String formattedDefaultExpression;

    @Nullable
    private T defaultExpression;

    public AnalyzedColumnDefinition(@Nullable AnalyzedColumnDefinition<T> parent) {
        this.parent = parent;
    }

    private AnalyzedColumnDefinition(AnalyzedColumnDefinition<T> parent,
                                     ColumnIdent ident,
                                     String name,
                                     DataType<?> dataType,
                                     IndexType indexType,
                                     String geoTree,
                                     T analyzer,
                                     String indexMethod,
                                     ColumnPolicy columnPolicy,
                                     boolean isPrimaryKey,
                                     boolean isNotNull,
                                     Settings analyzerSettings,
                                     GenericProperties<T> geoProperties,
                                     List<AnalyzedColumnDefinition<T>> children,
                                     boolean isIndex,
                                     List<String> sources,
                                     boolean isParentColumn,
                                     GenericProperties<T> storageProperties,
                                     @Nullable String formattedGeneratedExpression,
                                     @Nullable T generatedExpression,
                                     @Nullable String formattedDefaultExpression,
                                     @Nullable T defaultExpression,
                                     boolean generated) {
        this.parent = parent;
        this.ident = ident;
        this.name = name;
        this.dataType = dataType;
        this.indexType = indexType;
        this.geoTree = geoTree;
        this.analyzer = analyzer;
        this.indexMethod = indexMethod;
        this.columnPolicy = columnPolicy;
        this.isPrimaryKey = isPrimaryKey;
        this.isNotNull = isNotNull;
        this.analyzerSettings = analyzerSettings;
        this.geoProperties = geoProperties;
        this.children = children;
        this.isIndex = isIndex;
        this.sources = sources;
        this.isParentColumn = isParentColumn;
        this.storageProperties = storageProperties;
        this.formattedGeneratedExpression = formattedGeneratedExpression;
        this.generatedExpression = generatedExpression;
        this.formattedDefaultExpression = formattedDefaultExpression;
        this.defaultExpression = defaultExpression;
        this.generated = generated;
    }

    @SuppressWarnings("unchecked")
    public <U> AnalyzedColumnDefinition<U> map(Function<? super T, ? extends U> mapper) {
        return new AnalyzedColumnDefinition<>(
            parent == null ? null : (AnalyzedColumnDefinition<U>) parent,   // parent is expected to be mapped already
            ident,
            name,
            dataType,
            indexType,
            geoTree,
            analyzer == null ? null : mapper.apply(analyzer),
            indexMethod,
            columnPolicy,
            isPrimaryKey,
            isNotNull,
            analyzerSettings,
            geoProperties == null ? null : geoProperties.map(mapper),
            Lists2.map(children, x -> x.map(mapper)),
            isIndex,
            sources,
            isParentColumn,
            storageProperties == null ? null : storageProperties.map(mapper),
            formattedGeneratedExpression,
            generatedExpression == null ? null : mapper.apply(generatedExpression),
            formattedDefaultExpression,
            defaultExpression == null ? null : mapper.apply(defaultExpression),
            generated
        );
    }

    public void visitSymbols(Consumer<? super T> consumer) {
        if (analyzer != null) {
            consumer.accept(analyzer);
        }
        if (geoProperties != null) {
            geoProperties.properties().values().forEach(consumer);
        }
        for (var child : children) {
            child.visitSymbols(consumer);
        }
        if (storageProperties != null) {
            storageProperties.properties().values().forEach(consumer);
        }
        if (generatedExpression != null) {
            consumer.accept(generatedExpression);
        }
        if (defaultExpression != null) {
            consumer.accept(defaultExpression);
        }
    }

    public void name(String name) {
        this.name = name;
        if (this.parent != null) {
            this.ident = ColumnIdent.getChildSafe(this.parent.ident, name);
        } else {
            this.ident = ColumnIdent.fromNameSafe(name, List.of());
        }
    }

    public void analyzer(T analyzer) {
        this.analyzer = analyzer;
    }

    public String analyzer() {
        if (analyzer == null) {
            return null;
        }
        if (analyzer instanceof String str) {
            return str;
        }
        throw new IllegalStateException("Trying to access not evaluated analyzer");
    }

    void indexMethod(String indexMethod) {
        this.indexMethod = indexMethod;
    }

    public void indexType(IndexType indexType) {
        assert indexType != null : "IndexType must not be null";
        this.indexType = indexType;
    }

    public IndexType indexType() {
        return indexType;
    }

    private void analyzerSettings(Settings settings) {
        this.analyzerSettings = settings;
    }

    void geoTree(String geoTree) {
        this.geoTree = geoTree;
    }

    void geoProperties(GenericProperties<T> properties) {
        this.geoProperties = properties;
    }

    /**
     * Initializes from a given DataType including internal parameters
     */
    public void dataType(DataType<?> dataType) {
        this.dataType = dataType;
    }

    public DataType<?> dataType() {
        return this.dataType;
    }

    void columnPolicy(ColumnPolicy objectType) {
        this.columnPolicy = objectType;
    }

    public ColumnPolicy columnPolicy() {
        return this.columnPolicy;
    }

    public boolean isIndexColumn() {
        return isIndex;
    }

    public String geoTree() {
        return this.geoTree;
    }

    public GenericProperties<T> geoProperties() {
        return this.geoProperties;
    }

    void setAsIndexColumn() {
        this.isIndex = true;
    }

    public void addChild(AnalyzedColumnDefinition<T> analyzedColumnDefinition) {
        children.add(analyzedColumnDefinition);
    }

    public boolean hasChildren() {
        return !children.isEmpty();
    }

    Settings builtAnalyzerSettings() {
        if (!children().isEmpty()) {
            Settings.Builder builder = Settings.builder();
            builder.put(analyzerSettings);
            for (AnalyzedColumnDefinition<T> child : children()) {
                builder.put(child.builtAnalyzerSettings());
            }
            return builder.build();
        }
        return analyzerSettings;
    }

    /**
     * Validates and sets either specified or default docValues for the given column definition.
     * If column definition represents an object with sub-column, computes docValues for all sub-columns.
     */
    public static void validateAndComputeDocValues(AnalyzedColumnDefinition<Object> definition) {
        if (definition.storageProperties == null) {
            // Take default if not specified
            definition.docValues = definition.dataType.storageSupport().getComputedDocValuesDefault(definition.indexType);
        } else {
            Settings storageSettings = GenericPropertiesConverter.genericPropertiesToSettings(definition.storageProperties);
            for (String property : storageSettings.names()) {
                if (property.equals(COLUMN_STORE_PROPERTY)) {
                    DataType<?> dataType = definition.dataType();
                    boolean val = storageSettings.getAsBoolean(property, true);
                    if (val == false) {
                        if (dataType.storageSupport().supportsDocValuesOff()) {
                            definition.docValues = false;
                        } else {
                            throw new IllegalArgumentException(
                                String.format(Locale.ENGLISH,
                                              "Invalid storage option \"columnstore\" for data type \"%s\"",
                                              dataType.getName()));
                        }
                    }
                } else {
                    throw new IllegalArgumentException(
                        String.format(Locale.ENGLISH, "Invalid STORAGE WITH option `%s`", property));
                }
            }
        }
        // Children is not nullable, empty by default
        for (int i = 0; i < definition.children.size(); i++) {
            validateAndComputeDocValues(definition.children.get(i));
        }
    }

    static void applyAndValidateAnalyzerSettings(AnalyzedColumnDefinition<Object> definition,
                                                 FulltextAnalyzerResolver fulltextAnalyzerResolver) {
        if (definition.analyzer == null) {
            if (definition.indexMethod != null) {
                if (definition.indexMethod.equals("plain")) {
                    definition.analyzer("keyword");
                } else {
                    definition.analyzer("standard");
                }
            }
        } else {
            if (definition.analyzer instanceof Object[]) {
                throw new IllegalArgumentException("array literal not allowed for the analyzer property");
            }

            String analyzerName = DataTypes.STRING.sanitizeValue(definition.analyzer);
            if (fulltextAnalyzerResolver.hasCustomAnalyzer(analyzerName)) {
                Settings settings = fulltextAnalyzerResolver.resolveFullCustomAnalyzerSettings(analyzerName);
                definition.analyzerSettings(settings);
            }
        }

        for (AnalyzedColumnDefinition<Object> child : definition.children()) {
            applyAndValidateAnalyzerSettings(child, fulltextAnalyzerResolver);
        }
    }

    public void validate() {
        if (indexType == IndexType.FULLTEXT && !DataTypes.STRING.equals(ArrayType.unnest(dataType))) {
            throw new IllegalArgumentException(String.format(
                Locale.ENGLISH,
                "Can't use an Analyzer on column %s because analyzers are only allowed on " +
                "columns of type \"%s\" of the unbound length limit.",
                ident.sqlFqn(),
                DataTypes.STRING.getName()
            ));
        }
        if (indexType != IndexType.PLAIN && UNSUPPORTED_INDEX_TYPE_IDS.contains(dataType.id())) {
            throw new IllegalArgumentException(String.format(Locale.ENGLISH,
                                                             "INDEX constraint cannot be used on columns of type \"%s\"", dataType));
        }

        if (dataType.storageSupport() == null) {
            throw new IllegalArgumentException("Cannot use the type `" + dataType.getName() + "` for column: " + name);
        }
        if (hasPrimaryKeyConstraint()) {
            ensureTypeCanBeUsedAsKey();
        }
        for (AnalyzedColumnDefinition<T> child : children) {
            child.validate();
        }
        if (dataType.id() == ObjectType.ID && defaultExpression != null) {
            throw new IllegalArgumentException("Default values are not allowed for object columns: " + name);
        }
    }

    private void ensureTypeCanBeUsedAsKey() {
        if (dataType instanceof ArrayType) {
            throw new UnsupportedOperationException(
                String.format(Locale.ENGLISH, "Cannot use columns of type \"%s\" as primary key", dataType.getName()));
        }
        if (UNSUPPORTED_PK_TYPE_IDS.contains(dataType.id())) {
            throw new UnsupportedOperationException(
                String.format(Locale.ENGLISH, "Cannot use columns of type \"%s\" as primary key", dataType));
        }
        if (isArrayOrInArray()) {
            throw new UnsupportedOperationException(
                String.format(Locale.ENGLISH, "Cannot use column \"%s\" as primary key within an array object", name));
        }
    }

    public String name() {
        return name;
    }

    public static String typeNameForESMapping(DataType<?> dataType, @Nullable String analyzer, boolean isIndex) {
        if (StringType.ID == dataType.id()) {
            return analyzer == null && !isIndex ? "keyword" : "text";
        }
        return DataTypes.esMappingNameFrom(dataType.id());
    }

    public ColumnIdent ident() {
        return ident;
    }

    public void setPrimaryKeyConstraint() {
        this.isPrimaryKey = true;
    }

    boolean hasPrimaryKeyConstraint() {
        return this.isPrimaryKey;
    }

    void setNotNullConstraint() {
        isNotNull = true;
    }

    boolean hasNotNullConstraint() {
        return isNotNull;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (!(o instanceof AnalyzedColumnDefinition<?> that)) {
            return false;
        }

        return Objects.equals(ident, that.ident);
    }

    @Override
    public int hashCode() {
        return ident != null ? ident.hashCode() : 0;
    }

    @Override
    public String toString() {
        return "AnalyzedColumnDefinition{" +
               "ident=" + ident +
               '}';
    }

    public List<AnalyzedColumnDefinition<T>> children() {
        return children;
    }

    void sources(List<String> sources) {
        this.sources = sources;
    }

    boolean isArrayOrInArray() {
        return dataType instanceof ArrayType || (parent != null && parent.isArrayOrInArray());
    }

    void markAsParentColumn() {
        this.isParentColumn = true;
    }

    /**
     * @return true if this column has a defined child
     * (which is not coming from an object column definition payload in case of ADD COLUMN)
     */
    public boolean isParentColumn() {
        return isParentColumn;
    }

    public void setGenerated(boolean generated) {
        this.generated = generated;
    }

    public boolean isGenerated() {
        return generated;
    }

    public void formattedGeneratedExpression(String formattedGeneratedExpression) {
        this.formattedGeneratedExpression = formattedGeneratedExpression;
    }

    @Nullable
    public String formattedGeneratedExpression() {
        return formattedGeneratedExpression;
    }

    public void generatedExpression(T generatedExpression) {
        this.generatedExpression = generatedExpression;
        if (generatedExpression instanceof Symbol symbol) {
            if (SymbolVisitors.any(Symbols::isTableFunction, symbol)) {
                throw new UnsupportedOperationException(
                    "Cannot use table function in generated expression of column `" + name + "`");
            }
        }
    }

    @Nullable
    public T generatedExpression() {
        return generatedExpression;
    }

    void formattedDefaultExpression(String formattedDefaultExpression) {
        this.formattedDefaultExpression = formattedDefaultExpression;
    }

    @Nullable
    public T defaultExpression() {
        return defaultExpression;
    }

    public void defaultExpression(T defaultExpression) {
        this.defaultExpression = defaultExpression;
        if (defaultExpression instanceof Symbol symbol && SymbolVisitors.any(Symbols::isTableFunction, symbol)) {
            throw new UnsupportedOperationException(
                "Cannot use table function in default expression of column `" + name + "`");
        }
    }

    void setStorageProperties(GenericProperties<T> storageProperties) {
        this.storageProperties = storageProperties;
    }

    public boolean docValues() {
        return docValues;
    }

    public List<String> sources() {
        return sources;
    }
}
