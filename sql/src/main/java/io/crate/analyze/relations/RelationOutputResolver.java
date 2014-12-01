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

package io.crate.analyze.relations;

import com.google.common.collect.Iterables;
import io.crate.exceptions.AmbiguousColumnException;
import io.crate.exceptions.ColumnUnknownException;
import io.crate.exceptions.SchemaUnknownException;
import io.crate.exceptions.TableUnknownException;
import io.crate.metadata.ColumnIdent;
import io.crate.metadata.ReferenceInfo;
import io.crate.metadata.sys.SysSchemaInfo;
import io.crate.metadata.table.SchemaInfo;
import io.crate.metadata.table.TableInfo;
import io.crate.planner.symbol.DynamicReference;
import io.crate.planner.symbol.Reference;
import io.crate.planner.symbol.RelationOutput;
import io.crate.sql.tree.QualifiedName;
import org.elasticsearch.common.collect.Tuple;

import javax.annotation.Nullable;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.Map;

public class RelationOutputResolver {

    private Map<QualifiedName, AnalyzedRelation> sources;
    private SchemaInfo sysSchemaInfo;

    public RelationOutputResolver(Map<QualifiedName, AnalyzedRelation> sources, SchemaInfo sysSchemaInfo) {
        assert !sources.isEmpty() : "Must have at least one source";
        this.sources = sources;
        this.sysSchemaInfo = sysSchemaInfo;
    }

    public RelationOutput getRelationOutput(QualifiedName qualifiedName, boolean forWrite) {
        return getRelationOutput(qualifiedName, null, forWrite);
    }

    public RelationOutput getRelationOutput(QualifiedName qualifiedName, @Nullable List<String> path, boolean forWrite) {
        List<String> parts = qualifiedName.getParts();
        String columnSchema = null;
        String columnTableName = null;
        ColumnIdent columnIdent = new ColumnIdent(parts.get(parts.size() - 1), path);
        switch (parts.size()) {
            case 1:
                break;
            case 2:
                columnTableName = parts.get(0).toLowerCase(Locale.ENGLISH);
                break;
            case 3:
                columnSchema = parts.get(0).toLowerCase(Locale.ENGLISH);
                columnTableName = parts.get(1).toLowerCase(Locale.ENGLISH);

                if (columnSchema.equals(SysSchemaInfo.NAME)) {
                    if (forWrite) {
                        throw new UnsupportedOperationException("Cannot write on tables inside the 'sys' schema");
                    }
                    return fromSysTable(columnTableName, columnIdent);
                }
                break;
            default:
                throw new IllegalArgumentException("Column reference \"%s\" has too many parts. " +
                        "A column reference can have at most 3 parts and must have one of the following formats:  " +
                        "\"<column>\", \"<table>.<column>\" or \"<schema>.<table>.<column>\"");
        }

        boolean schemaMatched = false;
        boolean tableNameMatched = false;
        List<Tuple<Reference, AnalyzedRelation>> matches = new ArrayList<>();
        List<Tuple<Reference, AnalyzedRelation>> dynamicMatches = new ArrayList<>();
        for (Map.Entry<QualifiedName, AnalyzedRelation> entry : sources.entrySet()) {
            List<String> sourceParts = entry.getKey().getParts();
            String sourceSchema = null;
            String sourceTableOrAlias;

            if (sourceParts.size() == 1) {
                sourceTableOrAlias = sourceParts.get(0);
            } else if (sourceParts.size() == 2) {
                sourceSchema = sourceParts.get(0);
                sourceTableOrAlias = sourceParts.get(1);
            } else {
                throw new UnsupportedOperationException(String.format(Locale.ENGLISH,
                        "sources key (QualifiedName) must have 1 or 2 parts, not %d", sourceParts.size()));
            }
            AnalyzedRelation sourceRelation = entry.getValue();

            if (columnSchema != null && !columnSchema.equals(sourceSchema)) {
                continue;
            }
            schemaMatched = true;
            if (columnTableName != null && !sourceTableOrAlias.equals(columnTableName)) {
                continue;
            }
            tableNameMatched = true;
            Reference newReference = sourceRelation.getReference(columnIdent, forWrite);
            if (newReference != null) {
                if (newReference instanceof DynamicReference) {
                    dynamicMatches.add(new Tuple<>(newReference, sourceRelation));
                } else {
                    matches.add(new Tuple<>(newReference, sourceRelation));
                }
            }
        }
        Tuple<Reference, AnalyzedRelation> match;
        switch (matches.size()) {
            case 1:
                match = Iterables.getOnlyElement(matches);
                return new RelationOutput(match.v2(), match.v1());
            case 0:
                switch (dynamicMatches.size()) {
                    case 0:
                        if (!schemaMatched) {
                            throw new SchemaUnknownException(columnSchema);
                        }
                        if (!tableNameMatched) {
                            throw new TableUnknownException(columnTableName);
                        }
                        throw new ColumnUnknownException(columnIdent.sqlFqn());
                    case 1:
                        match = Iterables.getOnlyElement(dynamicMatches);
                        return new RelationOutput(match.v2(), match.v1());
                }
        }
        throw new AmbiguousColumnException(columnIdent);
    }

    private RelationOutput fromSysTable(String tableName, ColumnIdent columnIdent) {
        TableInfo tableInfo = sysSchemaInfo.getTableInfo(tableName);
        if (tableInfo == null) {
            throw new TableUnknownException(tableName);
        }
        ReferenceInfo referenceInfo = tableInfo.getReferenceInfo(columnIdent);
        if (referenceInfo == null) {
            throw new ColumnUnknownException(columnIdent.sqlFqn());
        }
        return new RelationOutput(new TableRelation(tableInfo), new Reference(referenceInfo));
    }
}
