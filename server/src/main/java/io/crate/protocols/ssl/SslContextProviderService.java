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

package io.crate.protocols.ssl;

import io.crate.common.unit.TimeValue;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.common.component.AbstractLifecycleComponent;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.inject.Singleton;
import org.elasticsearch.common.io.stream.InputStreamStreamInput;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.translog.BufferedChecksumStreamInput;
import org.elasticsearch.threadpool.Scheduler;
import org.elasticsearch.threadpool.ThreadPool;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.attribute.FileTime;
import java.util.ArrayList;
import java.util.List;

@Singleton
public class SslContextProviderService extends AbstractLifecycleComponent {

    private static final Logger LOGGER = LogManager.getLogger(SslContextProviderService.class);

    private final SslContextProvider sslContextProvider;
    private final Settings settings;
    private final ThreadPool threadPool;

    private Scheduler.Cancellable watchRoutine;

    @Inject
    public SslContextProviderService(Settings settings,
                                     ThreadPool threadPool,
                                     SslContextProvider sslContextProvider) {
        this.settings = settings;
        this.threadPool = threadPool;
        this.sslContextProvider = sslContextProvider;
    }

    @Override
    protected void doStart() {
        List<FingerPrint> filesToWatch = createFilesToWatch();
        if (filesToWatch.isEmpty()) {
            return;
        }
        TimeValue pollIntervalSetting = SslSettings.SSL_RESOURCE_POLL_INTERVAL.get(settings);
        watchRoutine = threadPool.scheduleWithFixedDelay(
            () -> pollForChanges(filesToWatch),
            pollIntervalSetting,
            ThreadPool.Names.GENERIC
        );
    }

    List<FingerPrint> createFilesToWatch() {
        ArrayList<FingerPrint> filesToWatch = new ArrayList<>();
        var keystorePath = SslSettings.SSL_KEYSTORE_FILEPATH.get(settings);
        if (!keystorePath.isEmpty()) {
            filesToWatch.add(FingerPrint.create(Paths.get(keystorePath)));
        }
        var trustStorePath = SslSettings.SSL_TRUSTSTORE_FILEPATH.get(settings);
        if (!trustStorePath.isEmpty()) {
            filesToWatch.add(FingerPrint.create(Paths.get(trustStorePath)));
        }
        return filesToWatch;
    }

    void pollForChanges(List<FingerPrint> fingerPrints) {
        for (FingerPrint fingerPrint : fingerPrints) {
            try {
                if (fingerPrint.didChange()) {
                    sslContextProvider.reloadSslContext();
                    return;
                }
            } catch (Exception e) {
                LOGGER.warn("Error trying to detect changes to SSL KEYSTORE/TRUSTSTORE", e);
                // Don't abort thread, retry on next poll-interval
            }
        }
    }

    @Override
    protected void doStop() {
        if (watchRoutine != null && !watchRoutine.isCancelled()) {
            watchRoutine.cancel();
            watchRoutine = null;
        }
    }

    @Override
    protected void doClose() {
    }

    static class FingerPrint {

        private final Path path;
        private FileTime lastModifiedTime;
        long checksum;

        public FingerPrint(Path path, FileTime lastModifiedTime, long checksum) {
            this.path = path;
            this.lastModifiedTime = lastModifiedTime;
            this.checksum = checksum;
        }

        public static FingerPrint create(Path path) {
            try (var in = new BufferedChecksumStreamInput(new InputStreamStreamInput(Files.newInputStream(path)), path.toString())) {
                in.skip(in.available());
                long checksum = in.getChecksum();
                FileTime lastModifiedTime = Files.getLastModifiedTime(path);
                return new FingerPrint(path, lastModifiedTime, checksum);
            } catch (IOException e) {
                throw new UncheckedIOException(e);
            }
        }

        public boolean didChange() throws IOException {
            FileTime lastModifiedTime = Files.getLastModifiedTime(path);
            boolean changed = false;
            if (!lastModifiedTime.equals(this.lastModifiedTime)) {
                this.lastModifiedTime = lastModifiedTime;
                changed = true;
            }
            try (var in = new BufferedChecksumStreamInput(new InputStreamStreamInput(Files.newInputStream(path)), path.toString())) {
                in.skip(in.available());
                long checksum = in.getChecksum();
                if (checksum != this.checksum) {
                    this.checksum = checksum;
                    changed = true;
                }
            }
            return changed;
        }
    }
}
