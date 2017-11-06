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

package io.crate.analyze;

import io.crate.analyze.symbol.Symbol;
import org.elasticsearch.common.Nullable;

import java.util.Map;
import java.util.function.Consumer;

public class CreateUserAnalyzedStatement implements DDLStatement {

    private final String userName;
    private final Map<String, Symbol> properties;

    public CreateUserAnalyzedStatement(String userName, @Nullable Map<String, Symbol> properties) {
        this.userName = userName;
        this.properties = properties;
    }

    @Override
    public <C, R> R accept(AnalyzedStatementVisitor<C, R> visitor, C context) {
        return visitor.visitCreateUserStatement(this, context);
    }

    public String userName() {
        return userName;
    }

    @Nullable
    public Map<String, Symbol> properties() {
        return properties;
    }

    @Override
    public void visitSymbols(Consumer<? super Symbol> consumer) {
        properties.values().forEach(consumer);
    }
}
