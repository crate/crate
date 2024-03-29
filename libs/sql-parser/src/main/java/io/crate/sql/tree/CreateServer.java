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

import java.util.Map;
import java.util.Objects;

public class CreateServer extends Statement {

    private final String name;
    private final String fdw;
    private final boolean ifNotExists;
    private final Map<String, Expression> options;

    public CreateServer(String name,
                        String fdw,
                        boolean ifNotExists,
                        Map<String, Expression> options) {
        this.name = name;
        this.fdw = fdw;
        this.ifNotExists = ifNotExists;
        this.options = options;
    }

    public String name() {
        return name;
    }

    public String fdw() {
        return fdw;
    }

    public boolean ifNotExists() {
        return ifNotExists;
    }

    public Map<String, Expression> options() {
        return options;
    }

    @Override
    public int hashCode() {
        return Objects.hash(name, fdw, options);
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }
        if (obj == null) {
            return false;
        }
        if (getClass() != obj.getClass()) {
            return false;
        }
        CreateServer other = (CreateServer) obj;
        return Objects.equals(name, other.name) && Objects.equals(fdw, other.fdw)
                && Objects.equals(options, other.options);
    }

    @Override
    public String toString() {
        return "CreateServer{name=" + name + ", fdw=" + fdw + "}";
    }

    @Override
    public <R, C> R accept(AstVisitor<R, C> visitor, C context) {
        return visitor.visitCreateServer(this, context);
    }
}
