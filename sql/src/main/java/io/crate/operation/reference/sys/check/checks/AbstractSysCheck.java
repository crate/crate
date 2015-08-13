/*
 * Licensed to Crate.IO GmbH ("Crate") under one or more contributor
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

package io.crate.operation.reference.sys.check.checks;

import org.apache.lucene.util.BytesRef;

public abstract class AbstractSysCheck implements SysCheck {

    private final int id;
    private final BytesRef description;
    protected Severity severity;
    protected boolean passed;

    public AbstractSysCheck(int id, BytesRef description, Severity severity, boolean passed) {
        this.id = id;
        this.description = description;
        this.severity = severity;
        this.passed = passed;
    }

    public int id() {
        return id;
    }

    public BytesRef description() {
        return description;
    }

    public Severity severity() {
        return severity;
    }

    public boolean passed() {
        return passed;
    }

    public abstract boolean validate();

}
