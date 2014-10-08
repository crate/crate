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

import io.crate.metadata.ColumnIdent;
import io.crate.metadata.ReferenceInfo;
import io.crate.metadata.ReferenceInfos;
import io.crate.metadata.TableIdent;
import io.crate.metadata.relation.AnalyzedRelation;
import io.crate.metadata.sys.SysSchemaInfo;
import io.crate.planner.symbol.Function;
import io.crate.planner.symbol.Reference;
import io.crate.sql.tree.QualifiedName;

import javax.annotation.Nullable;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;

public class AllocationContext {

    private final ReferenceInfos referenceInfos;
    public AnalyzedRelation currentRelation = null;
    public final Map<ReferenceInfo, Reference> allocatedReferences = new HashMap<>();
    public final Map<Function, Function> allocatedFunctions = new HashMap<>();

    boolean hasSysExpressions = false;

    public AllocationContext(ReferenceInfos referenceInfos) {
        this.referenceInfos = referenceInfos;
    }

    public Reference resolveReference(QualifiedName name) {
        return resolveReference(name, null);
    }

    public Reference resolveReference(QualifiedName name, @Nullable List<String> path) {
        assert currentRelation != null : "currentRelation must be set in order to resolve a qualifiedName";

        ColumnIdent columnIdent;
        List<String> parts = name.getParts();
        switch (parts.size()) {
            case 1:
                columnIdent = new ColumnIdent(parts.get(0), path);
                break;
            case 2:
                if (!currentRelation.addressedBy(parts.get(0))) {
                    throw new UnsupportedOperationException("table for reference not found in FROM: " + name);
                } else {
                    columnIdent = new ColumnIdent(parts.get(1), path);
                }
                break;
            case 3:
                String schemaName = parts.get(0).toLowerCase(Locale.ENGLISH);

                // enables statements like : select sys.shards.id, * from blob.myblobs
                if (schemaName.equals(SysSchemaInfo.NAME)) {
                    hasSysExpressions = true;
                    TableIdent sysTableIdent = new TableIdent(SysSchemaInfo.NAME, parts.get(1));
                    columnIdent = new ColumnIdent(parts.get(2), path);
                    ReferenceInfo referenceInfo = referenceInfos.getTableInfoSafe(sysTableIdent).getReferenceInfo(columnIdent);
                    Reference reference = new Reference(referenceInfo);
                    allocatedReferences.put(referenceInfo, reference);
                    return reference;
                } else if (!currentRelation.addressedBy(schemaName, parts.get(1))) {
                    throw new UnsupportedOperationException("table for reference not found in FROM: " + name);
                } else {
                    columnIdent = new ColumnIdent(parts.get(2), path);
                }
                break;
            default:
                throw new IllegalArgumentException("Column reference \"%s\" has too many parts. " +
                        "A column reference can have at most 3 parts and must have one of the following formats:  " +
                        "\"<column>\", \"<table>.<column>\" or \"<schema>.<table>.<column>\"");
        }

        // TODO: use RelationOutput
        Reference reference = currentRelation.getReference(null, null, columnIdent);
        allocatedReferences.put(reference.info(), reference);
        return reference;
    }
}
