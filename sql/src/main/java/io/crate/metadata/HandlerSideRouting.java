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

package io.crate.metadata;

import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;

import java.io.IOException;

/**
 * A routing which represents a handler side data location via a table.
 */
public class HandlerSideRouting extends Routing {

    private final TableIdent tableIdent;

    public HandlerSideRouting(TableIdent tableIdent) {
        assert tableIdent != null;
        this.tableIdent = tableIdent;
    }

    public TableIdent tableIdent() {
        return tableIdent;
    }

    @Override
    public boolean hasLocations() {
        return false;
    }

    @Override
    public String toString() {
        return "HandlerSideRouting{" +
                "tableIdent=" + tableIdent +
                "} ";
    }

    @Override
    public final void readFrom(StreamInput in) throws IOException {
        throw new UnsupportedOperationException("HandlerSideRouting has no serialization support");
    }

    @Override
    public final void writeTo(StreamOutput out) throws IOException {
        throw new UnsupportedOperationException("HandlerSideRouting has no serialization support");
    }


}
