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
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import io.crate.analyze.ddl.GeoSettingsApplier;
import io.crate.exceptions.InvalidColumnNameException;
import io.crate.metadata.ColumnIdent;
import io.crate.sql.tree.Expression;
import io.crate.types.DataTypes;
import org.elasticsearch.common.settings.Settings;

import javax.annotation.Nullable;
import java.util.*;

public class AnalyzedColumnDefinition {

    private final static Set<String> UNSUPPORTED_PK_TYPES = Sets.newHashSet(
        DataTypes.OBJECT.getName(),
        DataTypes.GEO_POINT.getName(),
        DataTypes.GEO_SHAPE.getName()
    );

    private final static Set<String> UNSUPPORTED_INDEX_TYPES = Sets.newHashSet(
        "array",
        DataTypes.OBJECT.getName(),
        DataTypes.GEO_POINT.getName(),
        DataTypes.GEO_SHAPE.getName()
    );

    private final AnalyzedColumnDefinition parent;
    private ColumnIdent ident;
    private String name;
    private String dataType;
    private String collectionType;
    private String index;
    private String geoTree;
    private String analyzer;
    @VisibleForTesting
    String objectType = "true"; // dynamic = true
    private boolean isPrimaryKey = false;
    private boolean isNotNull = false;
    private Settings analyzerSettings = Settings.EMPTY;
    private Settings geoSettings = Settings.EMPTY;

    private List<AnalyzedColumnDefinition> children = new ArrayList<>();
    private boolean isIndex = false;
    private ArrayList<String> copyToTargets;
    private boolean isParentColumn;

    @Nullable
    private String formattedGeneratedExpression;
    @Nullable
    private Expression generatedExpression;
    private final static Set<String> NO_DOC_VALUES_SUPPORT = Sets.newHashSet("object", "geo_shape");

    public static void validateName(String name) {
        Preconditions.checkArgument(!name.startsWith("_"), "Column name must not start with '_'");
        if (ColumnIdent.INVALID_COLUMN_NAME_PREDICATE.apply(name)) {
            throw new InvalidColumnNameException(name);
        }
    }

    AnalyzedColumnDefinition(@Nullable AnalyzedColumnDefinition parent) {
        this.parent = parent;
    }

    public void name(String name) {
        validateName(name);
        this.name = name;
        if (this.parent != null) {
            this.ident = ColumnIdent.getChild(this.parent.ident, name);
        } else {
            this.ident = new ColumnIdent(name);
        }
    }

    public void analyzer(String analyzer) {
        this.analyzer = analyzer;
    }

    void indexConstraint(String index) {
        this.index = index;
    }

    String indexConstraint() {
        return MoreObjects.firstNonNull(index, "not_analyzed");
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
        switch (dataType) {
            case "timestamp":
                this.dataType = "date";
                break;
            case "int":
                this.dataType = "integer";
                break;
            default:
                this.dataType = dataType;
        }
    }

    public String dataType() {
        return this.dataType;
    }

    void objectType(String objectType) {
        this.objectType = objectType;
    }

    void collectionType(String type) {
        this.collectionType = type;
    }

    private boolean docValues() {
        return !isIndexColumn()
               && collectionType == null
               && indexConstraint().equals("not_analyzed");
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
        if (analyzer != null && !analyzer.equals("not_analyzed") && !dataType.equals("string")) {
            throw new IllegalArgumentException(
                String.format(Locale.ENGLISH, "Can't use an Analyzer on column %s because analyzers are only allowed on columns of type \"string\".",
                    ident.sqlFqn()
                ));
        }
        if (index != null && UNSUPPORTED_INDEX_TYPES.contains(dataType)) {
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
        if (UNSUPPORTED_PK_TYPES.contains(dataType)) {
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

        mapping.put("type", dataType);
        addTypeOptions(mapping);

        if (!NO_DOC_VALUES_SUPPORT.contains(dataType)) {
            mapping.put("doc_values", docValues());
            mapping.put("index", indexConstraint());
            mapping.put("store", false);
        }

        if (copyToTargets != null) {
            mapping.put("copy_to", copyToTargets);
        }
        if ("array".equals(collectionType)) {
            Map<String, Object> outerMapping = new HashMap<String, Object>() {{
                put("type", "array");
            }};
            if (dataType().equals("object")) {
                objectMapping(mapping);
            }
            outerMapping.put("inner", mapping);
            return outerMapping;
        } else if (dataType().equals("object")) {
            objectMapping(mapping);
        }
        return mapping;
    }

    private void addTypeOptions(Map<String, Object> mapping) {
        switch (dataType) {
            case "date":
                /*
                 * We want 1000 not be be interpreted as year 1000AD but as 1970-01-01T00:00:01.000
                 * so prefer date mapping format epoch_millis over strict_date_optional_time
                 */
                mapping.put("format", "epoch_millis||strict_date_optional_time");
                break;
            case "geo_shape":
                GeoSettingsApplier.applySettings(mapping, geoSettings, geoTree);
                break;
            case "string":
                if (analyzer != null) {
                    mapping.put("analyzer", analyzer);
                }
                break;
        }
    }

    private void objectMapping(Map<String, Object> mapping) {
        mapping.put("dynamic", objectType);
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

}
