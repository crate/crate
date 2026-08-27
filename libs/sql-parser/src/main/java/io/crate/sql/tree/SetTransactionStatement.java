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

package io.crate.sql.tree;

import java.util.List;

import io.crate.common.collections.Lists;

public record SetTransactionStatement(List<TransactionMode> transactionModes) implements Statement {

    public interface TransactionMode {
    }

    public enum IsolationLevel implements TransactionMode {
        SERIALIZABLE,
        REPEATABLE_READ,
        READ_COMMITTED,
        READ_UNCOMMITTED;
    }

    public enum ReadMode implements TransactionMode {
        READ_WRITE,
        READ_ONLY;
    }

    public static class Deferrable implements TransactionMode {

        private final boolean not;

        public Deferrable(boolean not) {
            this.not = not;
        }

        @Override
        public String toString() {
            return not ? "NOT DEFERRABLE" : "DEFERRABLE";
        }
    }

    @Override
    public <R, C> R accept(AstVisitor<R, C> visitor, C context) {
        return (R) visitor.visitSetTransaction(this, context);
    }

    @Override
    public String toString() {
        return "SET TRANSACTION " + Lists.joinOn(", ", transactionModes, TransactionMode::toString);
    }
}
