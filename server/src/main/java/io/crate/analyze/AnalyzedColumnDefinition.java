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

import static org.elasticsearch.index.mapper.TypeParsers.DOC_VALUES;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.function.Consumer;
import java.util.function.Function;

import javax.annotation.Nullable;

import org.elasticsearch.common.settings.Settings;

import io.crate.analyze.ddl.GeoSettingsApplier;
import io.crate.common.annotations.VisibleForTesting;
import io.crate.common.collections.Lists2;
import io.crate.metadata.ColumnIdent;
import io.crate.metadata.FulltextAnalyzerResolver;
import io.crate.metadata.IndexType;
import io.crate.metadata.table.ColumnPolicies;
import io.crate.sql.tree.ColumnPolicy;
import io.crate.sql.tree.GenericProperties;
import io.crate.types.BitStringType;
import io.crate.types.CharacterType;
import io.crate.types.DataType;
import io.crate.types.DataTypes;
import io.crate.types.GeoShapeType;
import io.crate.types.ObjectType;
import io.crate.types.StringType;
import io.crate.types.TimestampType;

public class AnalyzedColumnDefinition<T> {

    private static final Set<Integer> UNSUPPORTED_PK_TYPE_IDS = Set.of(
        ObjectType.ID,
        DataTypes.GEO_POINT.id(),
        DataTypes.GEO_SHAPE.id()
    );

    private static final Set<Integer> UNSUPPORTED_INDEX_TYPE_IDS = Set.of(
        ObjectType.ID,
        DataTypes.GEO_POINT.id(),
        DataTypes.GEO_SHAPE.id()
    );

    private static final String COLUMN_STORE_PROPERTY = "columnstore";

    private final AnalyzedColumnDefinition<T> parent;
    public int position;
    private ColumnIdent ident;
    private String name;
    private DataType dataType;
    private String collectionType;

    private String geoTree;
    @Nullable
    private GenericProperties<T> geoProperties;

    private IndexType indexType;
    @Nullable
    private T analyzer;
    private String indexMethod;
    private Settings analyzerSettings = Settings.EMPTY;

    @VisibleForTesting
    ColumnPolicy objectType = ColumnPolicy.DYNAMIC;

    private boolean isPrimaryKey = false;
    private boolean isNotNull = false;

    private List<AnalyzedColumnDefinition<T>> children = new ArrayList<>();
    private boolean isIndex = false;
    private ArrayList<String> copyToTargets;
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

    public AnalyzedColumnDefinition(int position, @Nullable AnalyzedColumnDefinition<T> parent) {
        this.position = position;
        this.parent = parent;
    }

    private AnalyzedColumnDefinition(AnalyzedColumnDefinition<T> parent,
                                     int position,
                                     ColumnIdent ident,
                                     String name,
                                     DataType dataType,
                                     String collectionType,
                                     IndexType indexType,
                                     String geoTree,
                                     T analyzer,
                                     String indexMethod,
                                     ColumnPolicy objectType,
                                     boolean isPrimaryKey,
                                     boolean isNotNull,
                                     Settings analyzerSettings,
                                     GenericProperties<T> geoProperties,
                                     List<AnalyzedColumnDefinition<T>> children,
                                     boolean isIndex,
                                     ArrayList<String> copyToTargets,
                                     boolean isParentColumn,
                                     GenericProperties<T> storageProperties,
                                     @Nullable String formattedGeneratedExpression,
                                     @Nullable T generatedExpression,
                                     @Nullable String formattedDefaultExpression,
                                     @Nullable T defaultExpression,
                                     boolean generated) {
        this.parent = parent;
        this.position = position;
        this.ident = ident;
        this.name = name;
        this.dataType = dataType;
        this.collectionType = collectionType;
        this.indexType = indexType;
        this.geoTree = geoTree;
        this.analyzer = analyzer;
        this.indexMethod = indexMethod;
        this.objectType = objectType;
        this.isPrimaryKey = isPrimaryKey;
        this.isNotNull = isNotNull;
        this.analyzerSettings = analyzerSettings;
        this.geoProperties = geoProperties;
        this.children = children;
        this.isIndex = isIndex;
        this.copyToTargets = copyToTargets;
        this.isParentColumn = isParentColumn;
        this.storageProperties = storageProperties;
        this.formattedGeneratedExpression = formattedGeneratedExpression;
        this.generatedExpression = generatedExpression;
        this.formattedDefaultExpression = formattedDefaultExpression;
        this.defaultExpression = defaultExpression;
        this.generated = generated;
    }

    public <U> AnalyzedColumnDefinition<U> map(Function<? super T, ? extends U> mapper) {
        return new AnalyzedColumnDefinition<>(
            parent == null ? null : (AnalyzedColumnDefinition<U>) parent,   // parent is expected to be mapped already
            position,
            ident,
            name,
            dataType,
            collectionType,
            indexType,
            geoTree,
            analyzer == null ? null : mapper.apply(analyzer),
            indexMethod,
            objectType,
            isPrimaryKey,
            isNotNull,
            analyzerSettings,
            geoProperties == null ? null : geoProperties.map(mapper),
            Lists2.map(children, x -> x.map(mapper)),
            isIndex,
            copyToTargets,
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

    void indexMethod(String indexMethod) {
        this.indexMethod = indexMethod;
    }

    public void indexConstraint(IndexType indexType) {
        this.indexType = indexType;
    }

    @Nullable
    IndexType indexConstraint() {
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

    public void dataType(String dataType) {
        dataType(dataType, List.of(), true);
    }

    public void dataType(String typeName, List<Integer> parameters, boolean logWarnings) {
        this.dataType = DataTypes.of(typeName, parameters);
    }

    public DataType dataType() {
        return this.dataType;
    }

    void objectType(ColumnPolicy objectType) {
        this.objectType = objectType;
    }

    void collectionType(String type) {
        this.collectionType = type;
    }

    String collectionType() {
        return collectionType;
    }

    public boolean isIndexColumn() {
        return isIndex;
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

    private static void applyAndValidateStorageSettings(Map<String, Object> mapping,
                                                        AnalyzedColumnDefinition<Object> definition) {
        if (definition.storageProperties == null) {
            return;
        }
        Settings storageSettings = GenericPropertiesConverter.genericPropertiesToSettings(definition.storageProperties);
        for (String property : storageSettings.names()) {
            if (property.equals(COLUMN_STORE_PROPERTY)) {
                DataType<?> dataType = definition.dataType();
                boolean val = storageSettings.getAsBoolean(property, true);
                if (val == false) {
                    if (dataType.id() != DataTypes.STRING.id()) {
                        throw new IllegalArgumentException(
                            String.format(Locale.ENGLISH, "Invalid storage option \"columnstore\" for data type \"%s\"",
                                          dataType.getName()));
                    }

                    mapping.put(DOC_VALUES, "false");
                }
            } else {
                throw new IllegalArgumentException(
                    String.format(Locale.ENGLISH, "Invalid STORAGE WITH option `%s`", property));
            }
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
        if (indexType == IndexType.FULLTEXT && !DataTypes.STRING.equals(dataType)) {
            throw new IllegalArgumentException(String.format(
                Locale.ENGLISH,
                "Can't use an Analyzer on column %s because analyzers are only allowed on " +
                "columns of type \"" + DataTypes.STRING.getName() + "\" of the unbound length limit.",
                ident.sqlFqn()
            ));
        }
        if (indexType != null && UNSUPPORTED_INDEX_TYPE_IDS.contains(dataType.id())) {
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
    }

    private void ensureTypeCanBeUsedAsKey() {
        if (collectionType != null) {
            throw new UnsupportedOperationException(
                String.format(Locale.ENGLISH, "Cannot use columns of type \"%s\" as primary key", collectionType));
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

    static Map<String, Object> toMapping(AnalyzedColumnDefinition<Object> definition) {
        Map<String, Object> mapping = new HashMap<>();
        addTypeOptions(mapping, definition);
        mapping.put("type", definition.typeNameForESMapping());

        assert definition.position != 0 : "position should not be 0";
        mapping.put("position", definition.position);

        if (definition.indexType == IndexType.NONE) {
            // we must use a boolean <p>false</p> and NO string "false", otherwise parser support for old indices will fail
            mapping.put("index", false);
        }
        if (definition.copyToTargets != null) {
            mapping.put("copy_to", definition.copyToTargets);
        }

        if ("array".equals(definition.collectionType)) {
            Map<String, Object> outerMapping = new HashMap<>();
            outerMapping.put("type", "array");
            if (definition.dataType().id() == ObjectType.ID) {
                objectMapping(mapping, definition);
            }
            outerMapping.put("inner", mapping);
            return outerMapping;
        } else if (definition.dataType().id() == ObjectType.ID) {
            objectMapping(mapping, definition);
        }

        applyAndValidateStorageSettings(mapping, definition);

        if (definition.formattedDefaultExpression != null) {
            mapping.put("default_expr", definition.formattedDefaultExpression);
        }

        return mapping;
    }

    String typeNameForESMapping() {
        if (StringType.ID == dataType.id()) {
            return analyzer == null && !isIndex ? "keyword" : "text";
        }
        return DataTypes.esMappingNameFrom(dataType.id());
    }

    private static void addTypeOptions(Map<String, Object> mapping, AnalyzedColumnDefinition<Object> definition) {
        switch (definition.dataType.id()) {
            case TimestampType.ID_WITH_TZ:
                /*
                 * We want 1000 not be be interpreted as year 1000AD but as 1970-01-01T00:00:01.000
                 * so prefer date mapping format epoch_millis over strict_date_optional_time
                 */
                mapping.put("format", "epoch_millis||strict_date_optional_time");
                break;
            case TimestampType.ID_WITHOUT_TZ:
                mapping.put("format", "epoch_millis||strict_date_optional_time");
                mapping.put("ignore_timezone", true);
                break;
            case GeoShapeType.ID:
                if (definition.geoProperties != null) {
                    GeoSettingsApplier.applySettings(mapping, definition.geoProperties, definition.geoTree);
                }
                break;

            case StringType.ID:
                if (definition.analyzer != null) {
                    mapping.put("analyzer", DataTypes.STRING.sanitizeValue(definition.analyzer));
                }
                var stringType = (StringType) definition.dataType;
                if (!stringType.unbound()) {
                    mapping.put("length_limit", stringType.lengthLimit());
                }
                break;
            case CharacterType.ID:
                var type = (CharacterType) definition.dataType;
                mapping.put("length_limit", type.lengthLimit());
                mapping.put("blank_padding", true);
                break;

            case BitStringType.ID:
                int length = ((BitStringType) definition.dataType).length();
                mapping.put("length", length);
                break;

            default:
                // noop
                break;
        }
    }

    private static void objectMapping(Map<String, Object> mapping, AnalyzedColumnDefinition<Object> definition) {
        mapping.put("dynamic", ColumnPolicies.encodeMappingValue(definition.objectType));
        Map<String, Object> childProperties = new HashMap<>();
        for (AnalyzedColumnDefinition<Object> child : definition.children) {
            childProperties.put(child.name(), toMapping(child));
        }
        mapping.put("properties", childProperties);
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

    Map<String, Object> toMetaIndicesMapping() {
        return Map.of();
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof AnalyzedColumnDefinition)) return false;

        AnalyzedColumnDefinition that = (AnalyzedColumnDefinition) o;

        return ident != null ? ident.equals(that.ident) : that.ident == null;
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

    void addCopyTo(Set<String> targets) {
        this.copyToTargets = new ArrayList<>(targets);
    }

    public void ident(ColumnIdent ident) {
        assert this.ident == null : "ident must be null";
        this.ident = ident;
        this.name = ident.leafName();
    }

    boolean isArrayOrInArray() {
        return collectionType != null || (parent != null && parent.isArrayOrInArray());
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
    }

    void setStorageProperties(GenericProperties<T> storageProperties) {
        this.storageProperties = storageProperties;
    }
}
