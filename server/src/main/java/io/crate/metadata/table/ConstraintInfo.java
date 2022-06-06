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

package io.crate.metadata.table;

import io.crate.metadata.RelationInfo;
import io.crate.metadata.RelationName;

import java.util.List;
import java.util.stream.Collectors;

/**
 * This class is used as information store of table constraints when
 * being displayed.
 */
public class ConstraintInfo {

    /**
     * Enum that contains type of constraints (that are currently supported
     * by CrateDB)
     *
     * PRIMARY KEY is the primary key constraint of a table.
     * CHECK       is only used for NOT NULL constraints.
     */
    public enum Type {
        PRIMARY_KEY("PRIMARY KEY", "p"),
        CHECK("CHECK", "c");

        private final String text;
        private final String postgresChar;

        Type(final String text, String postgresChar) {
            this.text = text;
            this.postgresChar = postgresChar;
        }

        @Override
        public String toString() {
            return text;
        }

        public String postgresChar() {
            return postgresChar;
        }
    }

    private final String constraintName;
    private final List<Short> conkey;
    private final RelationInfo relationInfo;
    private final Type constraintType;

    public ConstraintInfo(RelationInfo relationInfo, String constraintName, Type constraintType) {
        this.relationInfo = relationInfo;
        this.constraintName = constraintName;
        this.constraintType = constraintType;
        this.conkey = getConstraintColumnIndices(relationInfo, constraintName, constraintType);
    }

    private static List<Short> getConstraintColumnIndices(RelationInfo relationInfo, String constraintName, Type constraintType) {
        if (relationInfo instanceof TableInfo tableInfo) {
            if (constraintType == Type.PRIMARY_KEY) {
                return relationInfo.primaryKey().stream().map(column -> (short) tableInfo.getReference(column).position()).collect(Collectors.toList());
            } else if (constraintType == Type.CHECK) {
                return tableInfo.checkConstraints().stream()
                    .filter(checkConstraint -> checkConstraint.name().equals(constraintName))
                    .map(checkConstraint -> checkConstraint.positions())
                    .findFirst().orElse(List.of());
            }
        }
        return List.of();
    }

    public RelationName relationName() {
        return relationInfo.ident();
    }

    public RelationInfo relationInfo() {
        return relationInfo;
    }

    public String constraintName() {
        return constraintName;
    }

    public Type constraintType() {
        return constraintType;
    }

    public List<Short> conkey() {
        return conkey;
    }
}
