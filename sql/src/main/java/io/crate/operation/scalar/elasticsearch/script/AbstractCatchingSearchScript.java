/*
 * Licensed to CRATE Technology GmbH ("Crate") under one or more contributor
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

package io.crate.operation.scalar.elasticsearch.script;

import org.elasticsearch.script.AbstractSearchScript;
import org.elasticsearch.script.ScriptException;

/**
 * abstract SearchScript that simply catches non-{@linkplain org.elasticsearch.script.ScriptException} and wraps them into a
 * {@linkplain org.elasticsearch.script.ScriptException}
 */
public abstract class AbstractCatchingSearchScript extends AbstractSearchScript {
    @Override
    public Object run() {
        try {
            return doRun();
        } catch (ScriptException e) {
            throw e; // rethrow
        } catch (Exception e) {
            throw new ScriptException(e.getMessage(), e);
        }
    }

    public abstract Object doRun();
}
