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

package io.crate.ffi;

import java.lang.foreign.Arena;
import java.lang.foreign.FunctionDescriptor;
import java.lang.foreign.Linker;
import java.lang.foreign.MemoryLayout;
import java.lang.foreign.MemoryLayout.PathElement;
import java.lang.foreign.MemorySegment;
import java.lang.foreign.StructLayout;
import java.lang.foreign.SymbolLookup;
import java.lang.foreign.ValueLayout;
import java.lang.invoke.MethodHandle;
import java.lang.invoke.VarHandle;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.lucene.util.Constants;

import io.crate.common.exceptions.Exceptions;

public class Natives {

    private static final Logger LOGGER = LogManager.getLogger(Natives.class);

    /// Representation for C struct:
    ///
    /// ```c
    /// struct rlimit {
    ///     rlim_t rlim_cur;
    ///     rlim_t rlim_max;
    /// }
    /// ```
    record RLimit(long cur, long max) {
    }

    private static final Linker LINKER = Linker.nativeLinker();
    private static final SymbolLookup LOOKUP = LINKER.defaultLookup();

    // See https://docs.oracle.com/en/java/javase/24/core/checking-native-errors-using-errno.html
    private static final Linker.Option CCS = Linker.Option.captureCallState("errno");
    private static final StructLayout CAPTURE_STATE_LAYOUT = Linker.Option.captureStateLayout();
    private static final VarHandle ERRNO_HANDLE = CAPTURE_STATE_LAYOUT.varHandle(PathElement.groupElement("errno"));

    private static final int ENOMEM = 12;
    private static boolean LOCAL_MLOCKALL = false;

    public static class Rlimits {

        /// this is only valid on Linux and the value *is* different on OS X
        /// see /usr/include/sys/resource.h on OS X
        /// on Linux the resource RLIMIT_NPROC means *the number of threads*
        /// this is in opposition to BSD-derived OSes
        public static final int NPROC = 6;
        public static final int MEMLOCK = Constants.MAC_OS_X ? 6 : 8;
        public static final int AS = Constants.MAC_OS_X ? 5 : 9;
        public static final int FSIZE = 1;

        public static final long INFINITY = Constants.MAC_OS_X ? 9223372036854775807L : -1L;

        private static final FunctionDescriptor DESC = FunctionDescriptor.of(
            ValueLayout.JAVA_INT,
            ValueLayout.JAVA_INT,
            ValueLayout.ADDRESS
        );

        private static final MemorySegment ADDR = LOOKUP.find("getrlimit")
            .orElseThrow(() -> new UnsatisfiedLinkError("unresolved symbol: getrlimit"));

        private static final MethodHandle HANDLE = LINKER.downcallHandle(ADDR, DESC, CCS);
        private static final StructLayout LAYOUT = MemoryLayout.structLayout(
            ValueLayout.JAVA_LONG.withName("rlim_cur"),
            ValueLayout.JAVA_LONG.withName("rlim_max")
        ).withName("rlimit");


        /// Binding to getrlimit from sys/resource.h
        /// ```c
        /// int getrlimit(int resource, struct rlimit *rlp);
        /// ```
        public static RLimit getrlimit(int resource) {
            try (var arena = Arena.ofConfined()) {
                MemorySegment rlimit = arena.allocate(LAYOUT);
                MemorySegment captureState = arena.allocate(CAPTURE_STATE_LAYOUT);
                int ret = (int) HANDLE.invokeExact(captureState, resource, rlimit);
                if (ret == 0) {
                    long cur = rlimit.getAtIndex(ValueLayout.JAVA_LONG, 0);
                    long max = rlimit.getAtIndex(ValueLayout.JAVA_LONG, 1);
                    return new RLimit(cur, max);
                }
                int errno = (int) ERRNO_HANDLE.get(captureState, 0);
                throw new NativeError(errno, Stringh.strerror(errno));
            } catch (Throwable t) {
                throw Exceptions.toRuntimeException(t);
            }
        }
    }

    private static class Stringh {

        private static final FunctionDescriptor DESC = FunctionDescriptor.of(
            ValueLayout.ADDRESS,
            ValueLayout.JAVA_INT
        );
        private static final MemorySegment ADDR = LOOKUP.find("strerror")
            .orElseThrow(() -> new UnsatisfiedLinkError("unresolved symbol: strerror"));
        private static final MethodHandle HANDLE = LINKER.downcallHandle(ADDR, DESC);

        /// Binding for strerror from string.h:
        ///
        /// ```c
        /// char *strerror(int errnum);
        /// ```
        public static String strerror(int errno) {
            try {
                MemorySegment ptr = (MemorySegment) HANDLE.invokeExact(errno);
                return ptr.reinterpret(Long.MAX_VALUE).getString(0);
            } catch (Throwable t) {
                throw Exceptions.toRuntimeException(t);
            }
        }
    }

    private static class Mmanh {

        static final int MCL_CURRENT = 1;

        private static final FunctionDescriptor DESC = FunctionDescriptor.of(
            ValueLayout.JAVA_INT,
            ValueLayout.JAVA_INT
        );
        private static final MemorySegment ADDR = LOOKUP.find("mlockall")
            .orElseThrow(() -> new UnsatisfiedLinkError("unresolved symbol: strerror"));
        private static final MethodHandle HANDLE = LINKER.downcallHandle(ADDR, DESC, CCS);


        /// Binding for `mlockall` from sys/mman.h
        ///
        /// ```c
        /// int mlockall(int flags);
        /// ```
        public static int mlockall(int flags) {
            try (var arena = Arena.ofConfined()) {
                MemorySegment captureState = arena.allocate(CAPTURE_STATE_LAYOUT);
                int result = (int) HANDLE.invokeExact(captureState, flags);
                if (result == -1) {
                    int errno = (int) ERRNO_HANDLE.get(captureState, 0);
                    throw new NativeError(errno, Stringh.strerror(errno));
                }
                return result;
            } catch (Throwable t) {
                throw Exceptions.toRuntimeException(t);
            }
        }
    }

    private static class Unistdh {

        private static final FunctionDescriptor DESC = FunctionDescriptor.of(
            ValueLayout.JAVA_INT
        );
        private static final MemorySegment ADDR = LOOKUP.find("geteuid")
            .orElseThrow(() -> new UnsatisfiedLinkError("unresolved symbol: geteuid"));
        private static final MethodHandle HANDLE = LINKER.downcallHandle(ADDR, DESC);

        /// Binding for `geteuid` from unistd.h
        ///
        /// ```c
        /// uid_t geteuid(void);
        /// ```
        public static int geteuid() {
            try {
                int result = (int) HANDLE.invokeExact();
                return result;
            } catch (Throwable t) {
                throw Exceptions.toRuntimeException(t);
            }
        }
    }

    public static void tryMlockall() {
        int errno = Integer.MIN_VALUE;
        boolean rlimitSuccess = false;
        long softLimit = 0;
        long hardLimit = 0;
        try {
            Mmanh.mlockall(Mmanh.MCL_CURRENT);
            LOCAL_MLOCKALL = true;
        } catch (NativeError err) {
            LOGGER.warn("Unable to lock JVM Memory: error={}, reason={}", err.errno() , err.getMessage());
            LOGGER.warn("This can result in part of the JVM being swapped out.");
            errno = err.errno();
        }

        if (Constants.LINUX || Constants.MAC_OS_X) {
            // we only know RLIMIT_MEMLOCK for these two at the moment.
            try {
                RLimit rlimit = Rlimits.getrlimit(Rlimits.MEMLOCK);
                rlimitSuccess = true;
                softLimit = rlimit.cur();
                hardLimit = rlimit.max();
            } catch (NativeError err) {
                LOGGER.warn("Unable to retrieve resource limits: errno={} msg={}", err.errno(), err.getMessage());
            }
        }

        // mlockall failed for some reason
        if (errno == ENOMEM) {
            if (rlimitSuccess) {
                LOGGER.warn("Increase RLIMIT_MEMLOCK, soft limit: {}, hard limit: {}", rlimitToString(softLimit), rlimitToString(hardLimit));
                if (Constants.LINUX) {
                    // give specific instructions for the linux case to make it easy
                    String user = System.getProperty("user.name");
                    LOGGER.warn("These can be adjusted by modifying /etc/security/limits.conf, for example: \n" +
                                "\t# allow user '{}' mlockall\n" +
                                "\t{} soft memlock unlimited\n" +
                                "\t{} hard memlock unlimited",
                                user, user, user
                                );
                    LOGGER.warn("If you are logged in interactively, you will have to re-login for the new limits to take effect.");
                }
            } else {
                LOGGER.warn("Increase RLIMIT_MEMLOCK (ulimit).");
            }
        }
    }

    private static String rlimitToString(long value) {
        assert Constants.LINUX || Constants.MAC_OS_X;
        return value == Rlimits.INFINITY ? "unlimited" : Long.toUnsignedString(value);
    }

    public static long getMaxNumberOfThreads() {
        if (Constants.LINUX) {
            return Rlimits.getrlimit(Rlimits.NPROC).cur();
        }
        return -1;
    }

    private static long getCurRlimit(int resource) {
        if (Constants.LINUX || Constants.MAC_OS_X) {
            return Rlimits.getrlimit(resource).cur();
        }
        return -1;
    }

    public static long getMaxSizeVirtualMemory() {
        return getCurRlimit(Rlimits.AS);
    }

    public static long getMaxFileSize() {
        return getCurRlimit(Rlimits.FSIZE);
    }

    public static boolean definitelyRunningAsRoot() {
        if (Constants.WINDOWS) {
            return false; // don't know
        }
        try {
            return Unistdh.geteuid() == 0;
        } catch (RuntimeException t) {
            return false;
        }
    }

    /// @return true if [tryMlockall()] was able to lock memory
    public static boolean isMemoryLocked() {
        return LOCAL_MLOCKALL;
    }
}
