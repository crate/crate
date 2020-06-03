/*
 * This file is part of a module with proprietary Enterprise Features.
 *
 * Licensed to Crate.io Inc. ("Crate.io") under one or more contributor
 * license agreements.  See the NOTICE file distributed with this work for
 * additional information regarding copyright ownership.
 *
 * Unauthorized copying of this file, via any medium is strictly prohibited.
 *
 * To use this file, Crate.io must have given you permission to enable and
 * use such Enterprise Features and you must have a valid Enterprise or
 * Subscription Agreement with Crate.io.  If you enable or use the Enterprise
 * Features, you represent and warrant that you have a valid Enterprise or
 * Subscription Agreement with Crate.io.  Your use of the Enterprise Features
 * if governed by the terms and conditions of your Enterprise or Subscription
 * Agreement with Crate.io.
 */

package io.crate.protocols.ssl;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.common.component.AbstractLifecycleComponent;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.inject.Singleton;
import org.elasticsearch.common.io.stream.InputStreamStreamInput;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.TimeValue;
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
        ArrayList<FingerPrint> filesToWatch = new ArrayList<>();
        var keystorePath = SslConfigSettings.SSL_KEYSTORE_FILEPATH.setting().get(settings);
        if (!keystorePath.isEmpty()) {
            filesToWatch.add(FingerPrint.create(Paths.get(keystorePath)));
        }
        var trustStorePath = SslConfigSettings.SSL_TRUSTSTORE_FILEPATH.setting().get(settings);
        if (!trustStorePath.isEmpty()) {
            filesToWatch.add(FingerPrint.create(Paths.get(trustStorePath)));
        }
        if (filesToWatch.isEmpty()) {
            return;
        }
        TimeValue pollIntervalSetting = SslConfigSettings.SSL_RESOURCE_POLL_INTERVAL.setting().get(settings);
        watchRoutine = threadPool.scheduleWithFixedDelay(
            () -> pollForChanges(filesToWatch),
            pollIntervalSetting,
            ThreadPool.Names.GENERIC
        );
    }

    private void pollForChanges(List<FingerPrint> fingerPrints) {
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
        private long checksum;

        public FingerPrint(Path path, FileTime lastModifiedTime, long checksum) {
            this.path = path;
            this.lastModifiedTime = lastModifiedTime;
            this.checksum = checksum;
        }

        public static FingerPrint create(Path path) {
            try (var in = new BufferedChecksumStreamInput(new InputStreamStreamInput(Files.newInputStream(path)), path.toString())) {
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
