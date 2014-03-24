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

package io.crate.operation.projectors;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

/**
 * A projector which simply collects any rows it gets and makes them available afterwards.
 * Downstreams are not supported with this projector. This class should only be used as a fallback if
 * there are no other projectors in a chain but the result needs to be fetched at once after all
 * upstreams provided their rows.
 */
public class CollectingProjector implements Projector {

    public List<Object[]> rows = new ArrayList<>();

    @Override
    public void setDownStream(Projector downStream) {
        throw new UnsupportedOperationException("downstreams not supported by this projector");
    }

    @Override
    public Projector getDownstream() {
        return null;
    }

    @Override
    public void startProjection() {}

    @Override
    public synchronized boolean setNextRow(Object... row) {
        rows.add(row);
        return true;
    }

    @Override
    public void finishProjection() {}

    @Override
    public Object[][] getRows() throws IllegalStateException {
        return rows.toArray(new Object[rows.size()][]);
    }

    @Override
    public Iterator<Object[]> iterator() {
        return rows.iterator();
    }
}
