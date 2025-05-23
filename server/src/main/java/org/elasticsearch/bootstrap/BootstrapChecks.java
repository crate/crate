/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.elasticsearch.bootstrap;

import java.io.BufferedReader;
import java.io.IOException;
import java.lang.Runtime.Version;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Locale;
import java.util.Optional;
import java.util.function.Predicate;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.message.ParameterizedMessage;
import org.apache.lucene.util.Constants;
import org.elasticsearch.common.io.PathUtils;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.BoundTransportAddress;
import org.elasticsearch.common.transport.TransportAddress;
import org.elasticsearch.discovery.DiscoveryModule;
import org.elasticsearch.index.IndexModule;
import org.elasticsearch.monitor.jvm.JvmInfo;
import org.elasticsearch.monitor.process.ProcessProbe;
import org.elasticsearch.node.NodeValidationException;

import io.crate.common.SuppressForbidden;
import io.crate.ffi.Natives;

/**
 * We enforce bootstrap checks once a node has the transport protocol bound to a non-loopback interface or if the system property {@code
 * es.enforce.bootstrap.checks} is set to {@true}. In this case we assume the node is running in production and all bootstrap checks must
 * pass.
 */
final class BootstrapChecks {

    private BootstrapChecks() {
    }

    static final String ES_ENFORCE_BOOTSTRAP_CHECKS = "es.enforce.bootstrap.checks";

    /**
     * Executes the bootstrap checks if the node has the transport protocol bound to a non-loopback interface. If the system property
     * {@code es.enforce.bootstrap.checks} is set to {@code true} then the bootstrap checks will be enforced regardless of whether or not
     * the transport protocol is bound to a non-loopback interface.
     *
     * @param boundTransportAddress the node network bindings
     */
    static void check(final Settings settings,
                      final BoundTransportAddress boundTransportAddress,
                      List<BootstrapCheck> additionalChecks) throws NodeValidationException {
        final List<BootstrapCheck> builtInChecks = checks();
        final List<BootstrapCheck> combinedChecks = new ArrayList<>(builtInChecks);
        combinedChecks.addAll(additionalChecks);
        check(settings,
              enforceLimits(boundTransportAddress, DiscoveryModule.DISCOVERY_TYPE_SETTING.get(settings)),
              Collections.unmodifiableList(combinedChecks));
    }

    /**
     * Executes the provided checks and fails the node if {@code enforceLimits} is {@code true}, otherwise logs warnings. If the system
     * property {@code es.enforce.bootstrap.checks} is set to {@code true} then the bootstrap checks will be enforced regardless of whether
     * or not the transport protocol is bound to a non-loopback interface.
     *
     * @param enforceLimits {@code true} if the checks should be enforced or otherwise warned
     * @param checks        the checks to execute
     */
    static void check(
        final Settings settings,
        final boolean enforceLimits,
        final List<BootstrapCheck> checks) throws NodeValidationException {
        check(settings, enforceLimits, checks, LogManager.getLogger(BootstrapChecks.class));
    }

    /**
     * Executes the provided checks and fails the node if {@code enforceLimits} is {@code true}, otherwise logs warnings. If the system
     * property {@code es.enforce.bootstrap.checks }is set to {@code true} then the bootstrap checks will be enforced regardless of whether
     * or not the transport protocol is bound to a non-loopback interface.
     *
     * @param enforceLimits {@code true} if the checks should be enforced or otherwise warned
     * @param checks        the checks to execute
     * @param logger        the logger to
     */
    static void check(
            final Settings settings,
            final boolean enforceLimits,
            final List<BootstrapCheck> checks,
            final Logger logger) throws NodeValidationException {
        final List<String> errors = new ArrayList<>();
        final List<String> ignoredErrors = new ArrayList<>();

        final String esEnforceBootstrapChecks = System.getProperty(ES_ENFORCE_BOOTSTRAP_CHECKS);
        final boolean enforceBootstrapChecks;
        if (esEnforceBootstrapChecks == null) {
            enforceBootstrapChecks = false;
        } else if (Boolean.TRUE.toString().equals(esEnforceBootstrapChecks)) {
            enforceBootstrapChecks = true;
        } else {
            final String message =
                    String.format(
                            Locale.ROOT,
                            "[%s] must be [true] but was [%s]",
                            ES_ENFORCE_BOOTSTRAP_CHECKS,
                            esEnforceBootstrapChecks);
            throw new IllegalArgumentException(message);
        }

        if (enforceLimits) {
            logger.info("bound or publishing to a non-loopback address, enforcing bootstrap checks");
        } else if (enforceBootstrapChecks) {
            logger.info("explicitly enforcing bootstrap checks");
        }

        for (final BootstrapCheck check : checks) {
            final BootstrapCheck.BootstrapCheckResult result = check.check(settings);
            if (result.isFailure()) {
                if (!(enforceLimits || enforceBootstrapChecks) && !check.alwaysEnforce()) {
                    ignoredErrors.add(result.getMessage());
                } else {
                    errors.add(result.getMessage());
                }
            }
        }

        if (!ignoredErrors.isEmpty()) {
            ignoredErrors.forEach(error -> log(logger, error));
        }

        if (!errors.isEmpty()) {
            final List<String> messages = new ArrayList<>(1 + errors.size());
            messages.add("[" + errors.size() + "] bootstrap checks failed");
            for (int i = 0; i < errors.size(); i++) {
                messages.add("[" + (i + 1) + "]: " + errors.get(i));
            }
            final NodeValidationException ne = new NodeValidationException(String.join("\n", messages));
            errors.stream().map(IllegalStateException::new).forEach(ne::addSuppressed);
            throw ne;
        }
    }

    static void log(final Logger logger, final String error) {
        logger.warn(error);
    }

    /**
     * Tests if the checks should be enforced.
     *
     * @param boundTransportAddress the node network bindings
     * @param discoveryType the discovery type
     * @return {@code true} if the checks should be enforced
     */
    static boolean enforceLimits(final BoundTransportAddress boundTransportAddress, final String discoveryType) {
        final Predicate<TransportAddress> isLoopbackAddress = t -> t.address().getAddress().isLoopbackAddress();
        final boolean bound =
                !(Arrays.stream(boundTransportAddress.boundAddresses()).allMatch(isLoopbackAddress) &&
                isLoopbackAddress.test(boundTransportAddress.publishAddress()));
        return bound && !"single-node".equals(discoveryType);
    }

    // the list of checks to execute
    static List<BootstrapCheck> checks() {
        final List<BootstrapCheck> checks = new ArrayList<>();
        checks.add(new HeapSizeCheck());
        final FileDescriptorCheck fileDescriptorCheck
            = Constants.MAC_OS_X ? new OsXFileDescriptorCheck() : new FileDescriptorCheck();
        checks.add(fileDescriptorCheck);
        checks.add(new MlockallCheck());
        if (Constants.LINUX) {
            checks.add(new MaxNumberOfThreadsCheck());
        }
        if (Constants.LINUX || Constants.MAC_OS_X) {
            checks.add(new MaxSizeVirtualMemoryCheck());
        }
        if (Constants.LINUX || Constants.MAC_OS_X) {
            checks.add(new MaxFileSizeCheck());
        }
        if (Constants.LINUX) {
            checks.add(new MaxMapCountCheck());
        }
        checks.add(new ClientJvmCheck());
        checks.add(new UseSerialGCCheck());
        checks.add(new OnErrorCheck());
        checks.add(new OnOutOfMemoryErrorCheck());
        checks.add(new EarlyAccessCheck());
        return Collections.unmodifiableList(checks);
    }

    static class HeapSizeCheck implements BootstrapCheck {

        @Override
        public BootstrapCheckResult check(Settings settings) {
            final long initialHeapSize = getInitialHeapSize();
            final long maxHeapSize = getMaxHeapSize();
            if (initialHeapSize != 0 && maxHeapSize != 0 && initialHeapSize != maxHeapSize) {
                final String message = String.format(
                    Locale.ROOT,
                    "initial heap size [%d] not equal to maximum heap size [%d]; " +
                        "this can cause resize pauses and prevents mlockall from locking the entire heap. " +
                        "Set the heap size via the CRATE_HEAP_SIZE environment variable. " +
                        "See https://crate.io/docs/crate/howtos/en/latest/performance/memory.html",
                    getInitialHeapSize(),
                    getMaxHeapSize()
                );
                return BootstrapCheckResult.failure(message);
            } else {
                return BootstrapCheckResult.success();
            }
        }

        // visible for testing
        long getInitialHeapSize() {
            return JvmInfo.jvmInfo().getConfiguredInitialHeapSize();
        }

        // visible for testing
        long getMaxHeapSize() {
            return JvmInfo.jvmInfo().getConfiguredMaxHeapSize();
        }

    }

    static class OsXFileDescriptorCheck extends FileDescriptorCheck {

        OsXFileDescriptorCheck() {
            // see constant OPEN_MAX defined in
            // /usr/include/sys/syslimits.h on OS X and its use in JVM
            // initialization in int os:init_2(void) defined in the JVM
            // code for BSD (contains OS X)
            super(10240);
        }

    }

    static class FileDescriptorCheck implements BootstrapCheck {

        private final int limit;

        FileDescriptorCheck() {
            this(1 << 16);
        }

        protected FileDescriptorCheck(final int limit) {
            if (limit <= 0) {
                throw new IllegalArgumentException("limit must be positive but was [" + limit + "]");
            }
            this.limit = limit;
        }

        public final BootstrapCheckResult check(Settings settings) {
            final long maxFileDescriptorCount = getMaxFileDescriptorCount();
            if (maxFileDescriptorCount != -1 && maxFileDescriptorCount < limit) {
                final String message = String.format(
                        Locale.ROOT,
                        "max file descriptors [%d] for crate process is too low, increase to at least [%d]",
                        getMaxFileDescriptorCount(),
                        limit);
                return BootstrapCheckResult.failure(message);
            } else {
                return BootstrapCheckResult.success();
            }
        }

        long getMaxFileDescriptorCount() {
            return ProcessProbe.getInstance().getMaxFileDescriptorCount();
        }

    }

    static class MlockallCheck implements BootstrapCheck {

        @Override
        public BootstrapCheckResult check(Settings settings) {
            if (BootstrapSettings.MEMORY_LOCK_SETTING.get(settings) && !isMemoryLocked()) {
                return BootstrapCheckResult.failure("memory locking requested for crate process but memory is not locked");
            } else {
                return BootstrapCheckResult.success();
            }
        }

        boolean isMemoryLocked() {
            return Natives.isMemoryLocked();
        }
    }

    static class MaxNumberOfThreadsCheck implements BootstrapCheck {

        // this should be plenty for machines up to 256 cores
        private static final long MAX_NUMBER_OF_THREADS_THRESHOLD = 1 << 12;

        @Override
        public BootstrapCheckResult check(Settings settings) {
            long maxNumberOfThreads = Natives.getMaxNumberOfThreads();
            if (maxNumberOfThreads != -1 && maxNumberOfThreads < MAX_NUMBER_OF_THREADS_THRESHOLD) {
                final String message = String.format(
                        Locale.ROOT,
                        "max number of threads [%d] for user [%s] is too low, increase to at least [%d]",
                        maxNumberOfThreads,
                        System.getProperty("user.name"),
                        MAX_NUMBER_OF_THREADS_THRESHOLD);
                return BootstrapCheckResult.failure(message);
            } else {
                return BootstrapCheckResult.success();
            }
        }
    }

    static class MaxSizeVirtualMemoryCheck implements BootstrapCheck {

        @Override
        public BootstrapCheckResult check(Settings settings) {
            long maxSizeVirtualMemory = Natives.getMaxSizeVirtualMemory();
            if (maxSizeVirtualMemory != Long.MIN_VALUE && maxSizeVirtualMemory != Natives.Rlimits.INFINITY) {
                final String message = String.format(
                    Locale.ROOT,
                    "max size virtual memory [%d] for user [%s] is too low, increase to [unlimited]",
                    maxSizeVirtualMemory,
                    System.getProperty("user.name")
                );
                return BootstrapCheckResult.failure(message);
            } else {
                return BootstrapCheckResult.success();
            }
        }
    }

    /**
     * Bootstrap check that the maximum file size is unlimited (otherwise Elasticsearch could run in to an I/O exception writing files).
     */
    static class MaxFileSizeCheck implements BootstrapCheck {

        @Override
        public BootstrapCheckResult check(Settings settings) {
            final long maxFileSize = Natives.getMaxFileSize();
            if (maxFileSize != Long.MIN_VALUE && maxFileSize != Natives.Rlimits.INFINITY) {
                final String message = String.format(
                    Locale.ROOT,
                    "max file size [%d] for user [%s] is too low, increase to [unlimited]",
                    maxFileSize,
                    System.getProperty("user.name")
                );
                return BootstrapCheckResult.failure(message);
            } else {
                return BootstrapCheckResult.success();
            }
        }
    }

    static class MaxMapCountCheck implements BootstrapCheck {

        static final long LIMIT = 1 << 18;

        @Override
        public BootstrapCheckResult check(final Settings settings) {
            // we only enforce the check if a store is allowed to use mmap at all
            if (IndexModule.NODE_STORE_ALLOW_MMAP.getWithFallback(settings)) {
                long maxMapCount = getMaxMapCount();
                if (maxMapCount != -1 && maxMapCount < LIMIT) {
                    final String message = String.format(
                        Locale.ROOT,
                        "max virtual memory areas vm.max_map_count [%d] is too low, " +
                        "increase to at least [%d] by adding `vm.max_map_count = 262144` to `/etc/sysctl.conf` " +
                        "or invoking `sysctl -w vm.max_map_count=262144`",
                        maxMapCount,
                        LIMIT);
                    return BootstrapCheckResult.failure(message);
                } else {
                    return BootstrapCheckResult.success();
                }
            } else {
                return BootstrapCheckResult.success();
            }
        }

        // visible for testing
        long getMaxMapCount() {
            return getMaxMapCount(LogManager.getLogger(BootstrapChecks.class));
        }

        // visible for testing
        long getMaxMapCount(Logger logger) {
            final Path path = getProcSysVmMaxMapCountPath();
            try (BufferedReader bufferedReader = getBufferedReader(path)) {
                final String rawProcSysVmMaxMapCount = readProcSysVmMaxMapCount(bufferedReader);
                if (rawProcSysVmMaxMapCount != null) {
                    try {
                        return parseProcSysVmMaxMapCount(rawProcSysVmMaxMapCount);
                    } catch (final NumberFormatException e) {
                        logger.warn(() -> new ParameterizedMessage("unable to parse vm.max_map_count [{}]", rawProcSysVmMaxMapCount), e);
                    }
                }
            } catch (final IOException e) {
                logger.warn(() -> new ParameterizedMessage("I/O exception while trying to read [{}]", path), e);
            }
            return -1;
        }

        @SuppressForbidden(reason = "access /proc/sys/vm/max_map_count")
        private Path getProcSysVmMaxMapCountPath() {
            return PathUtils.get("/proc/sys/vm/max_map_count");
        }

        // visible for testing
        BufferedReader getBufferedReader(final Path path) throws IOException {
            return Files.newBufferedReader(path);
        }

        // visible for testing
        String readProcSysVmMaxMapCount(final BufferedReader bufferedReader) throws IOException {
            return bufferedReader.readLine();
        }

        // visible for testing
        long parseProcSysVmMaxMapCount(final String procSysVmMaxMapCount) throws NumberFormatException {
            return Long.parseLong(procSysVmMaxMapCount);
        }

    }

    static class ClientJvmCheck implements BootstrapCheck {

        @Override
        public BootstrapCheckResult check(Settings settings) {
            if (getVmName().toLowerCase(Locale.ROOT).contains("client")) {
                final String message = String.format(
                        Locale.ROOT,
                        "JVM is using the client VM [%s] but should be using a server VM for the best performance",
                        getVmName());
                return BootstrapCheckResult.failure(message);
            } else {
                return BootstrapCheckResult.success();
            }
        }

        // visible for testing
        String getVmName() {
            return JvmInfo.jvmInfo().getVmName();
        }

    }

    /**
     * Checks if the serial collector is in use. This collector is single-threaded and devastating
     * for performance and should not be used for a server application like Elasticsearch.
     */
    static class UseSerialGCCheck implements BootstrapCheck {

        @Override
        public BootstrapCheckResult check(Settings settings) {
            if (getUseSerialGC().equals("true")) {
                final String message = String.format(
                        Locale.ROOT,
                        "JVM is using the serial collector but should not be for the best performance; " +
                                "either it's the default for the VM [%s] or -XX:+UseSerialGC was explicitly specified",
                        JvmInfo.jvmInfo().getVmName());
                return BootstrapCheckResult.failure(message);
            } else {
                return BootstrapCheckResult.success();
            }
        }

        // visible for testing
        String getUseSerialGC() {
            return JvmInfo.jvmInfo().useSerialGC();
        }

    }

    abstract static class MightForkCheck implements BootstrapCheck {

        @Override
        public BootstrapCheckResult check(Settings settings) {
            if (mightFork()) {
                return BootstrapCheckResult.failure(message());
            } else {
                return BootstrapCheckResult.success();
            }
        }

        abstract String message();

        // visible for testing
        abstract boolean mightFork();

        @Override
        public final boolean alwaysEnforce() {
            return true;
        }

    }

    static class OnErrorCheck extends MightForkCheck {

        @Override
        boolean mightFork() {
            final String onError = onError();
            return onError != null && !onError.equals("");
        }

        // visible for testing
        String onError() {
            return JvmInfo.jvmInfo().onError();
        }

        @Override
        String message() {
            return String.format(
                Locale.ROOT,
                "OnError [%s] requires forking but is prevented by system call filters ([%s=true]);" +
                    " upgrade to at least Java 8u92 and use ExitOnOutOfMemoryError",
                onError(),
                false);
        }
    }

    static class OnOutOfMemoryErrorCheck extends MightForkCheck {

        @Override
        boolean mightFork() {
            final String onOutOfMemoryError = onOutOfMemoryError();
            return onOutOfMemoryError != null && !onOutOfMemoryError.equals("");
        }

        // visible for testing
        String onOutOfMemoryError() {
            return JvmInfo.jvmInfo().onOutOfMemoryError();
        }

        String message() {
            return String.format(
                Locale.ROOT,
                "OnOutOfMemoryError [%s] requires forking but is prevented by system call filters ([%s=true]);" +
                    " upgrade to at least Java 8u92 and use ExitOnOutOfMemoryError",
                onOutOfMemoryError(),
                false);
        }
    }

    /**
     * Bootstrap check for early-access builds from OpenJDK.
     */
    static class EarlyAccessCheck implements BootstrapCheck {

        @Override
        public BootstrapCheckResult check(Settings settings) {
            Version version = Runtime.version();
            Optional<String> pre = version.pre();
            if (pre.isPresent()) {
                final String message = String.format(
                        Locale.ROOT,
                        "Java version [%s] is an early-access build, only use release builds",
                        version);
                return BootstrapCheckResult.failure(message);
            } else {
                return BootstrapCheckResult.success();
            }
        }
    }
}
