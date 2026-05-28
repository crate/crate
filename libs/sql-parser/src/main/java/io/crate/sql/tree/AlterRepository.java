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
import java.util.Objects;

public class AlterRepository<T> extends Statement {

    private final String repository;
    private final GenericProperties<T> setProperties;
    private final List<String> resetProperties;

    public AlterRepository(String repository,
                           GenericProperties<T> setProperties,
                           List<String> resetProperties) {
        assert setProperties != null : "setProperties mustn't be null";
        assert resetProperties != null : "resetProperties mustn't be null";

        if (!setProperties.isEmpty() && !resetProperties.isEmpty()) {
            throw new IllegalArgumentException("ALTER REPOSITORY: cannot set and reset properties at the same time");
        }
        this.repository = repository;
        this.setProperties = setProperties;
        this.resetProperties = resetProperties;
    }

    public String repository() {
        return repository;
    }

    public GenericProperties<T> setProperties() {
        return setProperties;
    }

    public List<String> resetProperties() {
        return resetProperties;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (!(o instanceof AlterRepository<?> that)) {
            return false;
        }
        return Objects.equals(repository, that.repository) &&
               Objects.equals(setProperties, that.setProperties) &&
               Objects.equals(resetProperties, that.resetProperties);
    }

    @Override
    public int hashCode() {
        return Objects.hash(repository, setProperties, resetProperties);
    }

    @Override
    public String toString() {
        return "AlterRepository{" +
               "repository=" + repository +
               ", properties=" + setProperties +
               ", resetProperties=" + resetProperties +
               '}';
    }

    @Override
    public <R, C> R accept(AstVisitor<R, C> visitor, C context) {
        return visitor.visitAlterRepository(this, context);
    }
}
