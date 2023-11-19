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

import static java.util.Objects.requireNonNull;

public class Explain extends Statement {

    public enum Option {
        ANALYZE,
        COSTS,
        VERBOSE
    }

    private final Statement statement;
    // Possible values for options is `true`, `false`, `null`
    private final Map<Option, Boolean> options;
    // The below flags exist only for SqlFormatter
    private final boolean analyze;
    private final boolean verbose;

    public Explain(Statement statement, boolean analyze, Map<Option, Boolean> options, boolean verbose) {
        this.statement = requireNonNull(statement, "statement is null");
        this.analyze = analyze;
        this.options = options;
        this.verbose = verbose;
    }

    public Statement getStatement() {
        return statement;
    }

    public boolean isAnalyze() {
        return analyze;
    }

    public boolean isVerbose() {
        return verbose;
    }

    public Map<Option, Boolean> options() {
        return options;
    }

    public boolean isOptionActivated(Explain.Option option) {
        // Option is activated if key is present and value true or null
        // e.g. explain (analyze true) or explain (analyze)
        return options.containsKey(option) && (options.get(option) == null || options.get(option) == true);
    }

    public boolean isOptionExplicitlyDeactivated(Explain.Option option) {
        return options.containsKey(option) && options.get(option) == false;
    }

    @Override
    public <R, C> R accept(AstVisitor<R, C> visitor, C context) {
        return visitor.visitExplain(this, context);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        Explain explain = (Explain) o;
        return analyze == explain.analyze &&
            verbose == explain.verbose &&
            Objects.equals(statement, explain.statement) &&
            Objects.equals(options, explain.options);
    }

    @Override
    public int hashCode() {
        return Objects.hash(statement, options, analyze, verbose);
    }

    public String toString() {
        return "Explain{" +
               "statement=" + statement +
               ", options=" + options +
               ", analyze=" + analyze +
               ", verbose=" + verbose +
               '}';
    }
}
