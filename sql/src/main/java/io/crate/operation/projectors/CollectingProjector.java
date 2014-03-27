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

import io.crate.operation.ProjectorUpstream;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * A projector which simply collects any rows it gets and makes them available afterwards.
 * Downstreams are not supported with this projector. This class should only be used as a fallback if
 * there are no other projectors in a chain but the result needs to be fetched at once after all
 * upstreams provided their rows.
 */
public class CollectingProjector implements Projector {

    private final AtomicInteger upstreamsRemaining;
    public List<Object[]> rows = new ArrayList<>();

    public CollectingProjector() {
        this.upstreamsRemaining = new AtomicInteger(0);
    }

    // TODO: further split Projector interface so that this projector doesn't have downstream / setDownstream
    @Override
    public void downstream(Projector downstream) {
    }

    @Override
    public Projector downstream() {
        return null;
    }

    @Override
    public void startProjection() {
        if (upstreamsRemaining.get() <= 0) {
            upstreamFinished();
        }
    }

    @Override
    public synchronized boolean setNextRow(Object... row) {
        rows.add(row);
        return true;
    }

    @Override
    public void registerUpstream(ProjectorUpstream upstream) {
        upstreamsRemaining.incrementAndGet();
    }

    @Override
    public void upstreamFinished() {
        upstreamsRemaining.decrementAndGet();
        // TODO: if upstreamsRemaining == 0
        //  set result future
    }

    @Override
    public Object[][] getRows() throws IllegalStateException {
        return rows.toArray(new Object[rows.size()][]);
    }

    @Override
    public Iterator<Object[]> iterator() {
        return rows.iterator();
    }
}
