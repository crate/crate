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

package io.crate.metadata.doc;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import io.crate.metadata.ColumnIdent;
import io.crate.metadata.ReferenceIdent;
import io.crate.metadata.ReferenceInfo;
import io.crate.metadata.TableIdent;
import io.crate.planner.RowGranularity;
import org.cratedb.Constants;
import org.cratedb.DataType;
import org.cratedb.sql.TableAliasSchemaException;
import org.elasticsearch.cluster.metadata.IndexMetaData;
import org.elasticsearch.cluster.metadata.MappingMetaData;
import org.elasticsearch.common.Booleans;
import org.elasticsearch.common.collect.Tuple;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

public class DocIndexMetaData {

    private static final String ID = "_id";
    private final IndexMetaData metaData;


    private final MappingMetaData defaultMappingMetaData;
    private final Map<String, Object> defaultMappingMap;

    private final ImmutableList.Builder<ReferenceInfo> columnsBuilder = ImmutableList.builder();
    private final ImmutableMap.Builder<ColumnIdent, ReferenceInfo> referencesBuilder = ImmutableMap.builder();

    private final TableIdent ident;
    private ImmutableList<ReferenceInfo> columns;
    private ImmutableMap<ColumnIdent, ReferenceInfo> references;
    private ImmutableList<String> primaryKey;
    private String routingCol;

    public DocIndexMetaData(IndexMetaData metaData, TableIdent ident) throws IOException {
        this.ident = ident;
        this.metaData = metaData;
        this.defaultMappingMetaData = this.metaData.mappingOrDefault(Constants.DEFAULT_MAPPING_TYPE);
        if (defaultMappingMetaData == null) {
            this.defaultMappingMap = new HashMap<>();
        } else {
            this.defaultMappingMap = this.defaultMappingMetaData.sourceAsMap();
        }
    }

    private void add(ReferenceInfo info) {
        if (info.ident().isColumn()) {
            columnsBuilder.add(info);
        }
        referencesBuilder.put(info.ident().columnIdent(), info);
        for (ReferenceInfo nested : info.nestedColumns()) {
            referencesBuilder.put(nested.ident().columnIdent(), nested);
        }
    }

    private ReferenceInfo newInfo(ColumnIdent column, DataType type) {
        return new ReferenceInfo(new ReferenceIdent(ident, column), RowGranularity.DOC, type);
    }

    /**
     * extract dataType from given columnProperties
     *
     * @param columnProperties map of String to Object containing column properties
     * @return dataType of the column with columnProperties
     */
    private static DataType getColumnDataType(Map<String, Object> columnProperties) {
        String typeName = (String) columnProperties.get("type");
        if (typeName == null) {
            if (columnProperties.get("properties") != null) {
                return DataType.OBJECT;
            }
        } else {
            switch (typeName.toLowerCase()) {
                case "date":
                    return DataType.TIMESTAMP;
                case "string":
                    return DataType.STRING;
                case "boolean":
                    return DataType.BOOLEAN;
                case "byte":
                    return DataType.BYTE;
                case "short":
                    return DataType.SHORT;
                case "integer":
                    return DataType.INTEGER;
                case "long":
                    return DataType.LONG;
                case "float":
                    return DataType.FLOAT;
                case "double":
                    return DataType.DOUBLE;
                case "ip":
                    return DataType.IP;
                case "object":
                case "nested":
                    return DataType.OBJECT;
                default:
                    return DataType.NOT_SUPPORTED;
            }
        }
        return null;
    }

    private ColumnIdent childIdent(ReferenceInfo.Builder infoBuilder, String name) {

        if (infoBuilder == null || infoBuilder.ident() == null || infoBuilder.ident().columnIdent() == null) {
            return new ColumnIdent(name);
        }
        ColumnIdent ident = infoBuilder.ident().columnIdent();
        if (ident.isColumn()) {
            return new ColumnIdent(ident.name(), name);
        } else {
            ImmutableList.Builder<String> builder = ImmutableList.builder();
            for (String s : ident.path()) {
                builder.add(s);
            }
            builder.add(name);
            return new ColumnIdent(ident.name(), builder.build());
        }
    }

    @SuppressWarnings("unchecked")
    private void internalExtractColumnDefinitions(ReferenceInfo.Builder infoBuilder,
                                                  Map<String, Object> propertiesMap) {
        if (propertiesMap == null) {
            return;
        }
        for (Map.Entry<String, Object> columnEntry : propertiesMap.entrySet()) {
            Map<String, Object> columnProperties = (Map) columnEntry.getValue();
            DataType columnDataType = getColumnDataType(columnProperties);
            ReferenceInfo.Builder builder = ReferenceInfo.builder().granularity(RowGranularity.NODE);

            if (columnProperties.get("type") != null
                    && columnProperties.get("type").equals("multi_field")) {
                for (Map.Entry<String, Object> multiColumnEntry :
                        ((Map<String, Object>) columnProperties.get("fields")).entrySet()) {

                    Map<String, Object> multiColumnProperties = (Map) multiColumnEntry.getValue();
                    if (multiColumnEntry.getKey().equals(columnEntry.getKey())) {
                        ColumnIdent columnIdent = childIdent(infoBuilder, columnEntry.getKey());
                        builder.type(getColumnDataType(multiColumnProperties)).ident(ident, columnIdent);
                    }
                }
            } else if (columnDataType == DataType.OBJECT) {
                boolean strict = columnProperties.get("dynamic") != null
                        && columnProperties.get("dynamic").equals("strict");
                boolean dynamic = columnProperties.get("dynamic") == null ||
                        (!strict &&
                                !columnProperties.get("dynamic").equals(false) &&
                                !Booleans.isExplicitFalse(columnProperties.get("dynamic").toString()));
                ColumnIdent columnIdent = childIdent(infoBuilder, columnEntry.getKey());
                builder.type(columnDataType)
                       .ident(new ReferenceIdent(ident, columnIdent))
                       .type(columnDataType)
                       .objectType(dynamic, strict);
                if (columnProperties.get("properties") != null) {
                    // walk nested
                    internalExtractColumnDefinitions(builder, (Map<String, Object>) columnProperties.get("properties"));
                }

            } else {
                ColumnIdent columnIdent = childIdent(infoBuilder, columnEntry.getKey());
                builder.type(columnDataType).ident(new ReferenceIdent(ident, columnIdent));
            }
            ReferenceInfo info = builder.build();

            if (infoBuilder != null) {
                infoBuilder.addNestedColumn(info);
            } else {
                add(info);
            }
        }
    }

    private ImmutableList<String> getPrimaryKey() {
        @SuppressWarnings("unchecked")
        Map<String, Object> metaMap = (Map<String, Object>) defaultMappingMap.get("_meta");
        if (metaMap != null) {
            return ImmutableList.of((String) metaMap.get("primary_keys"));
        }
        return ImmutableList.of("_id");
    }

    private void createColumnDefinitions() {
        @SuppressWarnings("unchecked")
        Map<String, Object> propertiesMap = (Map<String, Object>) defaultMappingMap.get("properties");
        internalExtractColumnDefinitions(null, propertiesMap);
    }

    private String getRoutingCol() {
        if (defaultMappingMetaData != null) {
            if (defaultMappingMetaData.routing().hasPath()) {
                return defaultMappingMetaData.routing().path();
            }
        }
        if (primaryKey.size() > 0) {
            return primaryKey.get(0);
        }
        return ID;
    }

    public DocIndexMetaData build() {
        createColumnDefinitions();
        columns = columnsBuilder.build();

        for (Tuple<ColumnIdent, ReferenceInfo> sysColumns : DocSysColumns.forTable(ident)) {
            referencesBuilder.put(sysColumns.v1(), sysColumns.v2());
        }

        references = referencesBuilder.build();
        primaryKey = getPrimaryKey();
        routingCol = getRoutingCol();
        return this;
    }

    public ImmutableMap<ColumnIdent, ReferenceInfo> references() {
        return references;
    }

    public ImmutableList<ReferenceInfo> columns() {
        return columns;
    }

    public ImmutableList<String> primaryKey() {
        return primaryKey;
    }

    public String routingCol() {
        return routingCol;
    }

    public boolean schemaEquals(DocIndexMetaData other) {
        if (this == other) return true;
        if (other == null) return false;

        if (columns != null ? !columns.equals(other.columns) : other.columns != null) return false;
        if (primaryKey != null ? !primaryKey.equals(other.primaryKey) : other.primaryKey != null) return false;
        if (references != null ? !references.equals(other.references) : other.references != null) return false;
        if (!routingCol.equals(other.routingCol)) return false;

        return true;
    }

    public DocIndexMetaData merge(DocIndexMetaData other) {
        // TODO: merge schemas if not equal, for now we just return this after making sure the schema is the same
        if (schemaEquals(other)) {
            return this;
        } else {
            throw new TableAliasSchemaException(other.name());
        }
    }

    private String name() {
        return ident.name();
    }

}
