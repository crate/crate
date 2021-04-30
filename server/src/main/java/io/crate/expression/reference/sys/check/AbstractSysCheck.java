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

package io.crate.expression.reference.sys.check;

import java.util.concurrent.CompletableFuture;

public abstract class AbstractSysCheck implements SysCheck {

    public static final String CLUSTER_CHECK_LINK_PATTERN = "https://cr8.is/d-cluster-check-";

    private final int id;
    private final String description;
    private final Severity severity;


    public AbstractSysCheck(int id, String description, Severity severity) {
        this(id, description, severity, CLUSTER_CHECK_LINK_PATTERN);
    }

    protected AbstractSysCheck(int id, String description, Severity severity, String linkPattern) {
        this.description = getLinkedDescription(id, description, linkPattern);
        this.id = id;
        this.severity = severity;
    }

    public static String getLinkedDescription(int id, String description, String linkPattern) {
        return description + " " + linkPattern + id;
    }

    public int id() {
        return id;
    }

    public String description() {
        return description;
    }

    public Severity severity() {
        return severity;
    }

    public abstract boolean isValid();

    @Override
    public CompletableFuture<?> computeResult() {
        return CompletableFuture.completedFuture(null);
    }
}
