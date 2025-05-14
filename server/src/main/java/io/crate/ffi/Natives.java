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
import java.lang.foreign.MemorySegment;
import java.lang.foreign.StructLayout;
import java.lang.foreign.SymbolLookup;
import java.lang.foreign.ValueLayout;
import java.lang.invoke.MethodHandle;

import org.apache.lucene.util.Constants;
import org.elasticsearch.bootstrap.ConsoleCtrlHandler;

import io.crate.common.exceptions.Exceptions;

public class Natives {

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

    // private final Linker.Option ccs = Linker.Option.captureCallState("errno");
    // private final StructLayout capturedStateLayout = Linker.Option.captureStateLayout();
    // private final VarHandle errnoHandle = capturedStateLayout.varHandle(PathElement.groupElement("errno"));

    private static class Rlimits {

        /// this is only valid on Linux and the value *is* different on OS X
        /// see /usr/include/sys/resource.h on OS X
        /// on Linux the resource RLIMIT_NPROC means *the number of threads*
        /// this is in opposition to BSD-derived OSes
        public static final int NPROC = 6;

        private static final FunctionDescriptor DESC = FunctionDescriptor.of(
            ValueLayout.JAVA_INT,
            ValueLayout.JAVA_INT,
            ValueLayout.ADDRESS
        );

        private static final MemorySegment ADDR = LOOKUP.find("getrlimit")
            .orElseThrow(() -> new UnsatisfiedLinkError("unresolved symbol: getrlimit"));

        private static final MethodHandle HANDLE = LINKER.downcallHandle(ADDR, DESC);
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
                int ret = (int) HANDLE.invokeExact(resource, rlimit);
                if (ret == 0) {
                    return new RLimit(
                        rlimit.getAtIndex(ValueLayout.JAVA_LONG, 0),
                        rlimit.getAtIndex(ValueLayout.JAVA_LONG, 1)
                    );
                }
                throw new IllegalStateException();
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

        private static final FunctionDescriptor DESC = FunctionDescriptor.of(
            ValueLayout.JAVA_INT,
            ValueLayout.JAVA_INT
        );
        private static final MemorySegment ADDR = LOOKUP.find("mlockall")
            .orElseThrow(() -> new UnsatisfiedLinkError("unresolved symbol: strerror"));
        private static final MethodHandle HANDLE = LINKER.downcallHandle(ADDR, DESC);

        /// Binding for `mlockall` from sys/mman.h
        ///
        /// ```c
        /// int mlockall(int flags);
        /// ```
        public static int mlockall(int flags) {
            try {
                return (int) HANDLE.invokeExact(flags);
            } catch (Throwable t) {
                throw Exceptions.toRuntimeException(t);
            }
        }
    }

    public static void tryMlockall() {
    }

    public static long getMaxNumberOfThreads() {
        if (Constants.LINUX) {
            RLimit getrlimit = Rlimits.getrlimit(Rlimits.NPROC);
            return getrlimit.cur();
        }
        return -1;
    }

    public static boolean definitelyRunningAsRoot() {
        return false;
    }

    public static void tryVirtualLock() {
    }

    public static void addConsoleCtrlHandler(ConsoleCtrlHandler handler) {
    }

    public static void trySetMaxSizeVirtualMemory() {
    }

    public static void trySetMaxFileSize() {
    }

    public static boolean isMemoryLocked() {
        return true;
    }
}
