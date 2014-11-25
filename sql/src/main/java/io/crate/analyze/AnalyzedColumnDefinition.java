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

import com.google.common.base.Objects;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import io.crate.metadata.ColumnIdent;
import org.elasticsearch.common.settings.ImmutableSettings;
import org.elasticsearch.common.settings.Settings;

import javax.annotation.Nullable;
import java.util.*;

public class AnalyzedColumnDefinition {

    private final AnalyzedColumnDefinition parent;
    private ColumnIdent ident;
    private String name;
    private String dataType;
    private String collectionType;
    private String index;
    private String analyzer;
    private String objectType = "true"; // dynamic = true
    private boolean isPrimaryKey = false;
    private Settings analyzerSettings = ImmutableSettings.EMPTY;

    private List<AnalyzedColumnDefinition> children = new ArrayList<>();
    private boolean isIndex = false;
    private ArrayList<String> copyToTargets;
    private boolean isParentColumn;

    public AnalyzedColumnDefinition(@Nullable AnalyzedColumnDefinition parent) {
        this.parent = parent;
    }

    public void name(String name) {
        Preconditions.checkArgument(!name.startsWith("_"), "Column ident must not start with '_'");
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

    public void analyzerSettings(Settings settings) {
        this.analyzerSettings = settings;
    }

    public String index() {
        return Objects.firstNonNull(index, "not_analyzed");
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
                && !dataType.equals("object")
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
            ImmutableSettings.Builder builder = ImmutableSettings.builder();
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
                    String.format("Can't use an Analyzer on column \"%s\" because analyzers are only allowed on columns of type \"string\".",
                            ident.sqlFqn()
                    ));
        }
        if (isPrimaryKey() && collectionType != null) {
            throw new UnsupportedOperationException(
                    String.format("Cannot use columns of type \"%s\" as primary key", collectionType));
        }
        for (AnalyzedColumnDefinition child : children) {
            child.validate();
        }
    }

    public String name() {
        return name;
    }

    public Map<String, Object> toMapping() {
        Map<String, Object> mapping = new HashMap<>();

        mapping.put("doc_values", docValues());
        mapping.put("type", dataType());
        mapping.put("index", index());
        mapping.put("store", false);

        if (copyToTargets != null) {
            mapping.put("copy_to", copyToTargets);
        }
        if (dataType().equals("string") && analyzer != null) {
            mapping.put("analyzer", analyzer());
        } else if(collectionType == "array"){
            Map<String, Object> outerMapping = new HashMap<String, Object>(){{
                put("type", "array");
            }};
            if(dataType().equals("object")){
                objectMapping(mapping);
            }
            outerMapping.put("inner", mapping);
            return outerMapping;
        } else if (dataType().equals("object")) {
            objectMapping(mapping);
        }
        return mapping;
    }

    private void objectMapping(Map<String, Object> mapping){
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

    public boolean hasMetaInfo() {
        for (AnalyzedColumnDefinition child : children) {
            if (child.hasMetaInfo()) {
                return true;
            }
        }
        return collectionType != null;
    }

    public void isPrimaryKey(boolean isPrimaryKey) {
        this.isPrimaryKey = isPrimaryKey;
    }

    public boolean isPrimaryKey() {
        return this.isPrimaryKey;
    }

    public Map<String, Object> toMetaMapping() {
        assert hasMetaInfo();
        if (dataType().equals("object")) {
            Map<String, Object> metaMapping = new HashMap<>();
            Map<String, Object> childrenMeta = new HashMap<>();
            metaMapping.put("properties", childrenMeta);

            for (AnalyzedColumnDefinition child : children) {
                if (child.hasMetaInfo()) {
                    childrenMeta.put(child.name, child.toMetaMapping());
                }
            }
            if (collectionType != null) {
                metaMapping.put("collection_type", collectionType);
            }
            return metaMapping;
        } else {
            return ImmutableMap.<String, Object>of("collection_type", collectionType);
        }
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
        return Objects.toStringHelper(this).add("ident", ident).toString();
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
}
