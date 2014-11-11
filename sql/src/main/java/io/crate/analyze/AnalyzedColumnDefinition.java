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
import com.google.common.collect.ComparisonChain;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import io.crate.metadata.ColumnIdent;
import io.crate.planner.symbol.Reference;
import io.crate.types.CollectionType;
import io.crate.types.DataType;
import org.elasticsearch.common.settings.ImmutableSettings;
import org.elasticsearch.common.settings.Settings;

import javax.annotation.Nullable;
import java.util.*;

public class AnalyzedColumnDefinition implements Comparable<AnalyzedColumnDefinition> {
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
    private boolean isObjectExtension = false;


    public AnalyzedColumnDefinition(@Nullable AnalyzedColumnDefinition parent) {
        this.parent = parent;
    }

    public AnalyzedColumnDefinition(Reference column,
                                    @Nullable AnalyzedColumnDefinition parent) {
        this(column.info().ident().columnIdent(), column.valueType(), parent);
    }

    public AnalyzedColumnDefinition(ColumnIdent columnIdent, DataType dataType,
                                    @Nullable AnalyzedColumnDefinition parent) {
        this.parent = parent;
        ident(columnIdent);
        if (dataType instanceof CollectionType){
            //TODO: update collection type to get kind of collection
            collectionType("array");
            dataType(((CollectionType) dataType).innerType().getName());
        } else {
            dataType(dataType.getName());
        }
    }

    /**
     * Set the name of the column, will set the {@linkplain #ident} as well.
     * Do not use together with {@linkplain #ident(io.crate.metadata.ColumnIdent)}.
     */
    public void name(String name) {
        Preconditions.checkArgument(!name.startsWith("_"), "Column ident must not start with '_'");
        this.name = name;
        if (this.parent != null) {
            this.ident = ColumnIdent.getChild(this.parent.ident, name);
        } else {
            this.ident = new ColumnIdent(name);
        }
    }

    /**
     * Set the ident of the column, will set the {@linkplain #name} as well.
     * Do not use together with {@linkplain #name(String)}.
     */
    public void ident(ColumnIdent ident) {
        Preconditions.checkArgument(!ident.isSystemColumn(), "Column ident must not start with '_'");
        assert this.ident == null;
        if (parent != null) {
            Preconditions.checkArgument(ident.isChildOf(parent.ident()),
                    String.format(Locale.ENGLISH,
                            "given ident '%s' is no child of parent '%s'",
                            ident.fqn(), parent.ident().fqn()));
        }
        this.ident = ident;


        if (this.name == null) {
            if (ident.isColumn()) {
                this.name = ident.name();
            } else {
                this.name = Iterables.getLast(ident.path());
            }
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
        } else if (dataType().equals("object")) {
            mapping.put("dynamic", objectType);
            Map<String, Object> childProperties = new HashMap<>();
            for (AnalyzedColumnDefinition child : children) {
                childProperties.put(child.name(), child.toMapping());
            }
            mapping.put("properties", childProperties);
        }
        return mapping;
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

    public boolean isArrayOrInArray() {
        return collectionType != null || (parent != null && parent.isArrayOrInArray());
    }

    public void isObjectExtension(boolean isObjectExtension) {
        this.isObjectExtension = isObjectExtension;
    }

    public boolean isObjectExtension() {
        return this.isObjectExtension;
    }

    /**
     * recursively merge given list of <code>otherChildren</code> with its own children.
     * If element in <code>otherChildren</code> is not contained in own children,
     * (determined by comparing {@linkplain #ident()} only)
     * add it. If it is inside the own children, compare for equality in terms of type
     * and properties etc.
     * @param otherChildren the children to merge
     * @throws java.lang.IllegalArgumentException if a column is contained in both
     *         childrens list (checked recursively) but differs in type and/or other properties
     */
    public void mergeChildren(List<AnalyzedColumnDefinition> otherChildren) {
        for (AnalyzedColumnDefinition otherChild : otherChildren) {
            if (children.contains(otherChild)) {
                AnalyzedColumnDefinition child = findInChildren(otherChild.ident(), false);
                if (child != null) {
                    if (child.compareTo(otherChild) != 0) {
                        throw new IllegalArgumentException(
                                String.format(Locale.ENGLISH,
                                    "The values given for column '%s' differ in their types",
                                    child.ident().fqn()));
                    }
                    child.mergeChildren(otherChild.children());
                }
            } else {
               addChild(otherChild);
            }
        }
    }

    @Nullable
    public AnalyzedColumnDefinition findInChildren(ColumnIdent ident,
                                                    boolean removeIfFound) {
        AnalyzedColumnDefinition result = null;
        for (AnalyzedColumnDefinition child : children) {
            if (child.ident().equals(ident)) {
                result = child;
                break;
            } else if (ident.isChildOf(child.ident())) {
                AnalyzedColumnDefinition inChildren = child.findInChildren(ident, removeIfFound);
                if (inChildren != null) {
                    return inChildren;
                }
            }
        }

        if (removeIfFound && result != null) {
            children.remove(result);
        }
        return result;
    }

    /**
     * does a comparison on two AnalyzedColumnDefinitions comparing
     * all properties, not just the columnIdent,
     * but does not consider children for comparison.
     */
    @Override
    public int compareTo(AnalyzedColumnDefinition o) {
        if (o == null) {
            return 1;
        }
        ComparisonChain chain = ComparisonChain.start();

        chain.compare(ident, o.ident)
                .compare(dataType, o.dataType)
                .compare(name, o.name)
                .compare(isIndex, o.isIndex)
                .compare(isPrimaryKey, isPrimaryKey);

        if (!Objects.equal(analyzer, o.analyzer)
                || !Objects.equal(analyzerSettings, o.analyzerSettings)
                || !Objects.equal(collectionType, o.collectionType)
                || !Objects.equal(index, o.index)
                || !Objects.equal(objectType, o.objectType)) {
            return -1;
        } else {
            return chain.result();
        }
    }
}
