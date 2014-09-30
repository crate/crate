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

import com.google.common.base.Predicate;
import io.crate.metadata.*;
import io.crate.metadata.relation.AnalyzedRelation;
import io.crate.metadata.sys.SysSchemaInfo;
import io.crate.planner.symbol.DynamicReference;
import io.crate.planner.symbol.Function;
import io.crate.planner.symbol.Reference;
import io.crate.sql.tree.QualifiedName;
import io.crate.types.ArrayType;
import io.crate.types.DataTypes;

import javax.annotation.Nullable;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;

public class AllocationContext {

    protected static final Predicate<ReferenceInfo> HAS_OBJECT_ARRAY_PARENT = new Predicate<ReferenceInfo>() {
             @Override
             public boolean apply(@Nullable ReferenceInfo input) {
                 return input != null
                         && input.type().id() == ArrayType.ID
                         && ((ArrayType)input.type()).innerType().equals(DataTypes.OBJECT);
             }
         };

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

        // TODO: logic here should be moved into TableInfo
        ReferenceInfo referenceInfo = currentRelation.getReferenceInfo(columnIdent);
        if (referenceInfo == null) {
            referenceInfo = currentRelation.getIndexReferenceInfo(columnIdent);
            if (referenceInfo == null) {
                DynamicReference dynamicReference = currentRelation.dynamicReference(columnIdent);
                allocatedReferences.put(dynamicReference.info(), dynamicReference);
                return dynamicReference;
            }
        }

        // TODO: need to return RelationOutput here so that it is possible to differentiate between
        // same Reference with different source relation.
        // for example in the case of: select a.name, b.name from t as a, t as b
        return allocateReference(referenceInfo);
    }

    public Reference allocateReference(ReferenceInfo referenceInfo) {
        Reference reference = allocatedReferences.get(referenceInfo);
        if (reference == null) {

            // TODO: this logic should be moved into TableInfo/AnalyzedRelation
            if (!referenceInfo.ident().columnIdent().isColumn() &&
                    hasMatchingParent(referenceInfo, HAS_OBJECT_ARRAY_PARENT)) {

                if (DataTypes.isCollectionType(referenceInfo.type())) {
                    // TODO: remove this limitation with next type refactoring
                    throw new UnsupportedOperationException(
                            "cannot query for arrays inside object arrays explicitly");
                }

                // for child fields of object arrays
                // return references of primitive types as array
                referenceInfo = new ReferenceInfo.Builder()
                        .ident(referenceInfo.ident())
                        .objectType(referenceInfo.objectType())
                        .granularity(referenceInfo.granularity())
                        .type(new ArrayType(referenceInfo.type()))
                        .build();
            }
            reference = new Reference(referenceInfo);
            allocatedReferences.put(referenceInfo, reference);
        }
        return reference;
    }


    /**
     * return true if the given {@linkplain com.google.common.base.Predicate}
     * returns true for a parent column of this one.
     * returns false if info has no parent column.
     */
    protected boolean hasMatchingParent(ReferenceInfo info,
                                        Predicate<ReferenceInfo> parentMatchPredicate) {
        ColumnIdent parent = info.ident().columnIdent().getParent();
        while (parent != null) {
            ReferenceInfo parentInfo = currentRelation.getReferenceInfo(parent);
            if (parentMatchPredicate.apply(parentInfo)) {
                return true;
            }
            parent = parent.getParent();
        }
        return false;
    }
}
