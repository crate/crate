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

import com.google.common.base.MoreObjects;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import io.crate.exceptions.InvalidColumnNameException;
import io.crate.metadata.ColumnIdent;
import io.crate.sql.tree.Expression;
import io.crate.types.DataTypes;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.settings.SettingsException;
import org.elasticsearch.common.unit.DistanceUnit;

import javax.annotation.Nullable;
import java.util.*;

public class AnalyzedColumnDefinition {

    private final AnalyzedColumnDefinition parent;
    private ColumnIdent ident;
    private String name;
    private String dataType;
    private String collectionType;
    private String index;
    private String geoTree;
    private String analyzer;
    private String objectType = "true"; // dynamic = true
    private boolean isPrimaryKey = false;
    private boolean isNotNull = false;
    private Settings analyzerSettings = Settings.EMPTY;
    private Settings geoSettings = Settings.EMPTY;

    private List<AnalyzedColumnDefinition> children = new ArrayList<>();
    private boolean isIndex = false;
    private ArrayList<String> copyToTargets;
    private boolean isParentColumn;

    private final static Set<String> UNSUPPORTED_PK_TYPES = Sets.newHashSet(
        DataTypes.OBJECT.getName(),
        DataTypes.GEO_POINT.getName(),
        DataTypes.GEO_SHAPE.getName()
    );

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

    public AnalyzedColumnDefinition(@Nullable AnalyzedColumnDefinition parent) {
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

    @Nullable
    public String analyzer() {
        return this.analyzer;
    }

    public void index(String index) {
        this.index = index;
    }

    public void geoTree(String geoTree) {
        this.geoTree = geoTree;
    }

    public void analyzerSettings(Settings settings) {
        this.analyzerSettings = settings;
    }

    public void geoSettings(Settings settings) {
        this.geoSettings = settings;
    }

    public String index() {
        return MoreObjects.firstNonNull(index, "not_analyzed");
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

    public void objectType(String objectType) {
        this.objectType = objectType;
    }

    public void collectionType(String type) {
        this.collectionType = type;
    }

    public boolean docValues() {
        return !isIndex()
               && collectionType == null
               && index().equals("not_analyzed");
    }

    protected boolean isIndex() {
        return isIndex;
    }

    public void isIndex(boolean isIndex) {
        this.isIndex = isIndex;
    }

    public void addChild(AnalyzedColumnDefinition analyzedColumnDefinition) {
        children.add(analyzedColumnDefinition);
    }

    public boolean hasChildren() {
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
        if (isPrimaryKey()) {
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

    public Map<String, Object> toMapping() {
        Map<String, Object> mapping = new HashMap<>();

        mapping.put("type", dataType());

        if ("date".equals(dataType())) {
            /**
             * We want 1000 not be be interpreted as year 1000AD but as 1970-01-01T00:00:01.000
             * so prefer date mapping format epoch_millis over strict_date_optional_time
             */
            mapping.put("format", "epoch_millis||strict_date_optional_time");
        }
        if (!NO_DOC_VALUES_SUPPORT.contains(dataType)) {
            mapping.put("doc_values", docValues());
            mapping.put("index", index());
            mapping.put("store", false);
        }

        if (geoTree != null) {
            mapping.put("tree", geoTree);
        }

        if (dataType().equals("geo_shape")) {
            String precision = geoSettings.get("precision");
            if (precision != null) {
                try {
                    DistanceUnit.parse(precision, DistanceUnit.DEFAULT, DistanceUnit.DEFAULT);
                } catch (IllegalArgumentException e) {
                    throw new IllegalArgumentException(
                        String.format(Locale.ENGLISH, "Value '%s' of setting precision is not a valid distance unit", precision)
                    );
                }
                mapping.put("precision", precision);
            } else {
                Integer treeLevels = geoSettings.getAsInt("tree_levels", null);
                if (treeLevels != null) {
                    mapping.put("tree_levels", treeLevels);
                }

            }
            try {
                Float errorPct = geoSettings.getAsFloat("distance_error_pct", null);
                if (errorPct != null) {
                    mapping.put("distance_error_pct", errorPct);
                }
            } catch (SettingsException e) {
                throw new IllegalArgumentException(
                    String.format(Locale.ENGLISH, "Value '%s' of setting distance_error_pct is not a float value", geoSettings.get("distance_error_pct"))
                );
            }

        }

        for (String setting : geoSettings.names()) {
            if (!setting.equals("precision") && !setting.equals("distance_error_pct") &&
                !setting.equals("tree_levels")) {
                throw new IllegalArgumentException(
                    String.format(Locale.ENGLISH, "Setting \"%s\" ist not supported on geo_shape index", setting));
            }
        }

        if (copyToTargets != null) {
            mapping.put("copy_to", copyToTargets);
        }
        if (dataType().equals("string") && analyzer != null) {
            mapping.put("analyzer", analyzer());
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

    public void isPrimaryKey(boolean isPrimaryKey) {
        this.isPrimaryKey = isPrimaryKey;
    }

    public boolean isPrimaryKey() {
        return this.isPrimaryKey;
    }

    public void isNotNull(boolean notNull) {
        isNotNull = notNull;
    }

    public boolean isNotNull() {
        return isNotNull;
    }

    public Map<String, Object> toMetaIndicesMapping() {
        return ImmutableMap.of();
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof AnalyzedColumnDefinition)) return false;

        AnalyzedColumnDefinition that = (AnalyzedColumnDefinition) o;

        if (ident != null ? !ident.equals(that.ident) : that.ident != null) return false;

        return true;
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

    public void addCopyTo(Set<String> targets) {
        this.copyToTargets = Lists.newArrayList(targets);
    }

    public void ident(ColumnIdent ident) {
        assert this.ident == null;
        this.ident = ident;
    }

    public boolean isArrayOrInArray() {
        return collectionType != null || (parent != null && parent.isArrayOrInArray());
    }

    public void isParentColumn(boolean isParentColumn) {
        this.isParentColumn = isParentColumn;
    }

    /**
     * @return true if this column has a defined child
     * (which is not coming from an object column definition payload in case of ADD COLUMN)
     */
    public boolean isParentColumn() {
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
