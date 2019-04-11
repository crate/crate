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

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.MoreObjects;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import io.crate.analyze.ddl.GeoSettingsApplier;
import io.crate.metadata.ColumnIdent;
import io.crate.metadata.Reference;
import io.crate.metadata.table.ColumnPolicies;
import io.crate.sql.tree.ColumnPolicy;
import io.crate.sql.tree.Expression;
import io.crate.types.DataType;
import io.crate.types.DataTypes;
import io.crate.types.GeoShapeType;
import io.crate.types.ObjectType;
import io.crate.types.StringType;
import io.crate.types.TimestampType;
import org.apache.logging.log4j.LogManager;
import org.elasticsearch.common.logging.DeprecationLogger;
import org.elasticsearch.common.settings.Settings;

import javax.annotation.Nullable;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;

import static org.elasticsearch.index.mapper.TypeParsers.DOC_VALUES;

public class AnalyzedColumnDefinition {

    private static final DeprecationLogger DEPRECATION_LOGGER =
        new DeprecationLogger(LogManager.getLogger(AnalyzedColumnDefinition.class));

    private static final Set<Integer> UNSUPPORTED_PK_TYPE_IDS = Sets.newHashSet(
        ObjectType.ID,
        DataTypes.GEO_POINT.id(),
        DataTypes.GEO_SHAPE.id()
    );

    private static final Set<Integer> UNSUPPORTED_INDEX_TYPE_IDS = Sets.newHashSet(
        ObjectType.ID,
        DataTypes.GEO_POINT.id(),
        DataTypes.GEO_SHAPE.id()
    );

    private final AnalyzedColumnDefinition parent;
    public Integer position;
    private ColumnIdent ident;
    private String name;
    private DataType dataType;
    private String collectionType;
    private Reference.IndexType indexType;
    private String geoTree;
    private String analyzer;

    @VisibleForTesting
    ColumnPolicy objectType = ColumnPolicy.DYNAMIC;

    private boolean isPrimaryKey = false;
    private boolean isNotNull = false;
    private Settings analyzerSettings = Settings.EMPTY;
    private Settings geoSettings = Settings.EMPTY;

    private List<AnalyzedColumnDefinition> children = new ArrayList<>();
    private boolean isIndex = false;
    private ArrayList<String> copyToTargets;
    private boolean isParentColumn;
    private boolean columnStore = true;

    @Nullable
    private String formattedGeneratedExpression;
    @Nullable
    private Expression generatedExpression;

    AnalyzedColumnDefinition(Integer position, @Nullable AnalyzedColumnDefinition parent) {
        this.position = position;
        this.parent = parent;
    }

    public void name(String name) {
        this.name = name;
        if (this.parent != null) {
            this.ident = ColumnIdent.getChildSafe(this.parent.ident, name);
        } else {
            this.ident = ColumnIdent.fromNameSafe(name);
        }
    }

    public void analyzer(String analyzer) {
        this.analyzer = analyzer;
    }

    void indexConstraint(Reference.IndexType indexType) {
        this.indexType = indexType;
    }

    @Nullable
    Reference.IndexType indexConstraint() {
        return indexType;
    }

    void geoTree(String geoTree) {
        this.geoTree = geoTree;
    }

    public void analyzerSettings(Settings settings) {
        this.analyzerSettings = settings;
    }

    void geoSettings(Settings settings) {
        this.geoSettings = settings;
    }

    public void dataType(String dataType) {
        if ("timestamp".equals(dataType)) {
            DEPRECATION_LOGGER.deprecated(
                "Column [{}]: Usage of the `TIMESTAMP` data type as a timestamp with zone " +
                "is deprecated, use the `TIMESTAMPTZ` or `TIMESTAMP WITH TIME ZONE` data type instead.",
                ident.fqn()
            );
        }
        this.dataType = DataTypes.ofName(dataType);
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

    boolean isIndexColumn() {
        return isIndex;
    }

    void setAsIndexColumn() {
        this.isIndex = true;
    }

    void addChild(AnalyzedColumnDefinition analyzedColumnDefinition) {
        children.add(analyzedColumnDefinition);
    }

    boolean hasChildren() {
        return !children.isEmpty();
    }

    public Settings analyzerSettings() {
        if (!children().isEmpty()) {
            Settings.Builder builder = Settings.builder();
            builder.put(analyzerSettings);
            for (AnalyzedColumnDefinition child : children()) {
                builder.put(child.analyzerSettings());
            }
            return builder.build();
        }
        return analyzerSettings;
    }

    public void validate() {
        if (analyzer != null && !"not_analyzed".equals(analyzer) && !DataTypes.STRING.equals(dataType)) {
            throw new IllegalArgumentException(
                String.format(Locale.ENGLISH, "Can't use an Analyzer on column %s because analyzers are only allowed on columns of type \"string\".",
                    ident.sqlFqn()
                ));
        }
        if (indexType != null && UNSUPPORTED_INDEX_TYPE_IDS.contains(dataType.id())) {
            throw new IllegalArgumentException(String.format(Locale.ENGLISH,
                "INDEX constraint cannot be used on columns of type \"%s\"", dataType));
        }
        if (hasPrimaryKeyConstraint()) {
            ensureTypeCanBeUsedAsKey();
        }
        for (AnalyzedColumnDefinition child : children) {
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

    Map<String, Object> toMapping() {
        Map<String, Object> mapping = new HashMap<>();
        addTypeOptions(mapping);
        mapping.put("type", typeNameForESMapping());

        if (position != null) {
            mapping.put("position", position);
        }
        if (indexType == Reference.IndexType.NO) {
            // we must use a boolean <p>false</p> and NO string "false", otherwise parser support for old indices will fail
            mapping.put("index", false);
        }
        if (copyToTargets != null) {
            mapping.put("copy_to", copyToTargets);
        }

        if ("array".equals(collectionType)) {
            Map<String, Object> outerMapping = new HashMap<>();
            outerMapping.put("type", "array");
            if (dataType().id() == ObjectType.ID) {
                objectMapping(mapping);
            }
            outerMapping.put("inner", mapping);
            return outerMapping;
        } else if (dataType().id() == ObjectType.ID) {
            objectMapping(mapping);
        }

        if (columnStore == false) {
            mapping.put(DOC_VALUES, "false");
        }
        return mapping;
    }

    String typeNameForESMapping() {
        switch (dataType.id()) {
            case TimestampType.ID:
                return "date";

            case StringType.ID:
                return analyzer == null && !isIndex ? "keyword" : "text";

            default:
                return DataTypes.esMappingNameFrom(dataType.id());
        }
    }

    private void addTypeOptions(Map<String, Object> mapping) {
        switch (dataType.id()) {
            case TimestampType.ID:
                /*
                 * We want 1000 not be be interpreted as year 1000AD but as 1970-01-01T00:00:01.000
                 * so prefer date mapping format epoch_millis over strict_date_optional_time
                 */
                mapping.put("format", "epoch_millis||strict_date_optional_time");
                break;
            case GeoShapeType.ID:
                GeoSettingsApplier.applySettings(mapping, geoSettings, geoTree);
                break;
            case StringType.ID:
                if (analyzer != null) {
                    mapping.put("analyzer", analyzer);
                }
                break;

            default:
                // noop
                break;
        }
    }

    private void objectMapping(Map<String, Object> mapping) {
        mapping.put("dynamic", ColumnPolicies.encodeMappingValue(objectType));
        Map<String, Object> childProperties = new HashMap<>();
        for (AnalyzedColumnDefinition child : children) {
            childProperties.put(child.name(), child.toMapping());
        }
        mapping.put("properties", childProperties);
    }

    public ColumnIdent ident() {
        return ident;
    }

    void setPrimaryKeyConstraint() {
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
        return ImmutableMap.of();
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
        return MoreObjects.toStringHelper(this).add("ident", ident).toString();
    }

    public List<AnalyzedColumnDefinition> children() {
        return children;
    }

    void addCopyTo(Set<String> targets) {
        this.copyToTargets = Lists.newArrayList(targets);
    }

    public void ident(ColumnIdent ident) {
        assert this.ident == null : "ident must be null";
        this.ident = ident;
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
    boolean isParentColumn() {
        return isParentColumn;
    }

    public void formattedGeneratedExpression(String formattedGeneratedExpression) {
        this.formattedGeneratedExpression = formattedGeneratedExpression;
    }

    @Nullable
    public String formattedGeneratedExpression() {
        return formattedGeneratedExpression;
    }

    public void generatedExpression(Expression generatedExpression) {
        this.generatedExpression = generatedExpression;
    }

    @Nullable
    public Expression generatedExpression() {
        return generatedExpression;
    }

    void setColumnStore(boolean columnStore) {
        this.columnStore = columnStore;
    }

    public boolean isColumnStoreEnabled() {
        return columnStore;
    }
}
