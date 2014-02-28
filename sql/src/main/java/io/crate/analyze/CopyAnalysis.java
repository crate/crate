/*
 * Licensed to CRATE Technology GmbH ("Crate") under one or more contributor
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

package io.crate.analyze;

import io.crate.metadata.Functions;
import io.crate.metadata.ReferenceInfos;
import io.crate.metadata.ReferenceResolver;

public class CopyAnalysis extends AbstractDataAnalysis {

    public static enum Mode {
        FROM,
        INTO
    }

    private String path;
    private Mode mode;

    public CopyAnalysis(ReferenceInfos referenceInfos, Functions functions, Object[] parameters, ReferenceResolver referenceResolver) {
        super(referenceInfos, functions, parameters, referenceResolver);
    }

    public String path() {
        return path;
    }

    public void path(String path) {
        this.path = path;
    }

    @Override
    public Type type() {
        return Type.COPY;
    }

    public Mode mode() {
        return mode;
    }

    public void mode(Mode mode) {
        this.mode = mode;
    }



}
