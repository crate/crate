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

import io.crate.exceptions.CrateException;
import io.crate.exceptions.UnsupportedFeatureException;
import io.crate.operation.ProjectorUpstream;
import io.crate.operation.projectors.writer.Output;
import io.crate.operation.projectors.writer.OutputFile;
import io.crate.operation.projectors.writer.OutputS3;
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.common.settings.Settings;

import java.io.IOException;
import java.io.OutputStream;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

public class WriterProjector implements Projector {

    private static final byte NEW_LINE = (byte) '\n';

    private final URI uri;
    private Output output;
    private OutputStream outputStream;

    protected final AtomicInteger remainingUpstreams = new AtomicInteger();
    protected final AtomicLong counter = new AtomicLong();
    private final AtomicReference<Throwable> failure = new AtomicReference<>(null);

    private Projector downstream;

    public WriterProjector(String uri, Settings settings) {

        try {
            this.uri = new URI(uri);
        } catch (URISyntaxException e) {
            throw new CrateException(String.format("Invalid uri '%s'", uri), e);
        }
        if (this.uri.getScheme() == null || this.uri.getScheme().equals("file")) {
            this.output = new OutputFile(this.uri, settings);
        } else if (this.uri.getScheme().equalsIgnoreCase("s3")) {
            this.output = new OutputS3(this.uri, settings);
        } else {
            throw new UnsupportedFeatureException(String.format("Unknown scheme '%s'", this.uri.getScheme()));
        }
    }

    @Override
    public void startProjection() {
        counter.set(0);
        try {
            output.open();
        } catch (IOException e) {
            failure.set(new CrateException("Failed to open output", e));
            return;
        }
        outputStream = output.getOutputStream();
    }


    private void endProjection() {
        try {
            output.close();
        } catch (IOException e) {
            failure.set(new CrateException("Failed to close output", e));
        }
        if (downstream != null) {
            if (failure.get() == null){
                downstream.setNextRow(counter.get());
                downstream.upstreamFinished();
            } else {
                downstream.upstreamFailed(failure.get());
            }
        }
    }

    @Override
    public synchronized boolean setNextRow(Object... row) {
        if (failure.get()!=null){
            return false;
        }
        BytesRef value = (BytesRef) row[0];
        try {
            outputStream.write(value.bytes, value.offset, value.length);
            outputStream.write(NEW_LINE);
        } catch (IOException e) {
            failure.set(new CrateException("Failed to write row to output", e));
        }
        counter.incrementAndGet();
        return true;
    }

    @Override
    public void registerUpstream(ProjectorUpstream upstream) {
        remainingUpstreams.incrementAndGet();
    }

    @Override
    public void upstreamFinished() {
        if (remainingUpstreams.decrementAndGet() == 0) {
            endProjection();
        }
    }

    @Override
    public void upstreamFailed(Throwable throwable) {
        failure.set(throwable);
        upstreamFinished();
    }

    @Override
    public void downstream(Projector downstream) {
        this.downstream = downstream;
        downstream.registerUpstream(this);
    }

    @Override
    public Projector downstream() {
        return downstream;
    }
}
