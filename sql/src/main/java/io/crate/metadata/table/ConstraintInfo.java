/*
 * Licensed to Crate under one or more contributor license agreements.
 * See the NOTICE file distributed with this work for additional
 * information regarding copyright ownership.  Crate licenses this file
 * to you under the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.  You may
 * obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
 * implied.  See the License for the specific language governing
 * permissions and limitations under the License.
 *
 * However, if you have executed another commercial license agreement
 * with Crate these terms will supersede the license and you may use the
 * software solely pursuant to the terms of the relevant commercial
 * agreement.
 */

package io.crate.metadata.table;

import io.crate.metadata.TableIdent;
import org.apache.lucene.util.BytesRef;

/**
 * This class is used as information store of table constraints when
 * beeing displayed.
 */
public class ConstraintInfo {

    /**
     * Enum that contains type of constraints (that are currently supported
     * by CrateDB)
     *
     * PRIMARY KEY is the primary key constraint of a table.
     * CHECK       is only used for NOT NULL constraints.
     */
    public enum Constraint {
        PRIMARY_KEY("PRIMARY KEY"),
        CHECK("CHECK");

        private final String text;

        Constraint(final String text) {
            this.text = text;
        }

        public BytesRef getReference() {
            return new BytesRef(this.text);
        }
    }

    private final String constraintName;
    private final TableIdent tableIdent;
    private final Constraint constraintType;

    public ConstraintInfo(TableIdent tableIdent, String constraintName, Constraint constraintType) {
        this.tableIdent = tableIdent;
        this.constraintName = constraintName;
        this.constraintType = constraintType;
    }

    public TableIdent tableIdent() {
        return this.tableIdent;
    }

    public String constraintName() {
        return this.constraintName;
    }

    public BytesRef constraintType() {
        return this.constraintType.getReference();
    }
}
