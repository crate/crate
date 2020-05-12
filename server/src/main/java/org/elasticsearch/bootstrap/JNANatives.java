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

import com.sun.jna.Native;
import com.sun.jna.Pointer;
import com.sun.jna.WString;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.lucene.util.Constants;
import org.elasticsearch.monitor.jvm.JvmInfo;

import static org.elasticsearch.bootstrap.JNAKernel32Library.SizeT;

/**
 * This class performs the actual work with JNA and library bindings to call native methods. It should only be used after
 * we are sure that the JNA classes are available to the JVM
 */
class JNANatives {

    /** no instantiation */
    private JNANatives() {
    }

    private static final Logger LOGGER = LogManager.getLogger(JNANatives.class);

    // Set to true, in case native mlockall call was successful
    static boolean LOCAL_MLOCKALL = false;

    // set to the maximum number of threads that can be created for
    // the user ID that owns the running CrateDB process
    static long MAX_NUMBER_OF_THREADS = -1;

    static long MAX_SIZE_VIRTUAL_MEMORY = Long.MIN_VALUE;

    static long MAX_FILE_SIZE = Long.MIN_VALUE;

    static void tryMlockall() {
        int errno = Integer.MIN_VALUE;
        String errMsg = null;
        boolean rlimitSuccess = false;
        long softLimit = 0;
        long hardLimit = 0;

        try {
            int result = JNACLibrary.mlockall(JNACLibrary.MCL_CURRENT);
            if (result == 0) {
                LOCAL_MLOCKALL = true;
                return;
            }

            errno = Native.getLastError();
            errMsg = JNACLibrary.strerror(errno);
            if (Constants.LINUX || Constants.MAC_OS_X) {
                // we only know RLIMIT_MEMLOCK for these two at the moment.
                JNACLibrary.Rlimit rlimit = new JNACLibrary.Rlimit();
                if (JNACLibrary.getrlimit(JNACLibrary.RLIMIT_MEMLOCK, rlimit) == 0) {
                    rlimitSuccess = true;
                    softLimit = rlimit.rlim_cur.longValue();
                    hardLimit = rlimit.rlim_max.longValue();
                } else {
                    LOGGER.warn("Unable to retrieve resource limits: {}", JNACLibrary.strerror(Native.getLastError()));
                }
            }
        } catch (UnsatisfiedLinkError e) {
            // this will have already been logged by CLibrary, no need to repeat it
            return;
        }

        // mlockall failed for some reason
        LOGGER.warn("Unable to lock JVM Memory: error={}, reason={}", errno , errMsg);
        LOGGER.warn("This can result in part of the JVM being swapped out.");
        if (errno == JNACLibrary.ENOMEM) {
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

    static void trySetMaxNumberOfThreads() {
        if (Constants.LINUX) {
            // this is only valid on Linux and the value *is* different on OS X
            // see /usr/include/sys/resource.h on OS X
            // on Linux the resource RLIMIT_NPROC means *the number of threads*
            // this is in opposition to BSD-derived OSes
            final int rlimit_nproc = 6;

            final JNACLibrary.Rlimit rlimit = new JNACLibrary.Rlimit();
            if (JNACLibrary.getrlimit(rlimit_nproc, rlimit) == 0) {
                MAX_NUMBER_OF_THREADS = rlimit.rlim_cur.longValue();
            } else {
                LOGGER.warn("unable to retrieve max number of threads [" + JNACLibrary.strerror(Native.getLastError()) + "]");
            }
        }
    }

    static void trySetMaxSizeVirtualMemory() {
        if (Constants.LINUX || Constants.MAC_OS_X) {
            final JNACLibrary.Rlimit rlimit = new JNACLibrary.Rlimit();
            if (JNACLibrary.getrlimit(JNACLibrary.RLIMIT_AS, rlimit) == 0) {
                MAX_SIZE_VIRTUAL_MEMORY = rlimit.rlim_cur.longValue();
            } else {
                LOGGER.warn("unable to retrieve max size virtual memory [" + JNACLibrary.strerror(Native.getLastError()) + "]");
            }
        }
    }

    static void trySetMaxFileSize() {
        if (Constants.LINUX || Constants.MAC_OS_X) {
            final JNACLibrary.Rlimit rlimit = new JNACLibrary.Rlimit();
            if (JNACLibrary.getrlimit(JNACLibrary.RLIMIT_FSIZE, rlimit) == 0) {
                MAX_FILE_SIZE = rlimit.rlim_cur.longValue();
            } else {
                LOGGER.warn("unable to retrieve max file size [" + JNACLibrary.strerror(Native.getLastError()) + "]");
            }
        }
    }

    private static String rlimitToString(long value) {
        assert Constants.LINUX || Constants.MAC_OS_X;
        if (value == JNACLibrary.RLIM_INFINITY) {
            return "unlimited";
        } else {
            return Long.toUnsignedString(value);
        }
    }

    /** Returns true if user is root, false if not, or if we don't know */
    static boolean definitelyRunningAsRoot() {
        if (Constants.WINDOWS) {
            return false; // don't know
        }
        try {
            return JNACLibrary.geteuid() == 0;
        } catch (UnsatisfiedLinkError e) {
            // this will have already been logged by Kernel32Library, no need to repeat it
            return false;
        }
    }

    static void tryVirtualLock() {
        JNAKernel32Library kernel = JNAKernel32Library.getInstance();
        Pointer process = null;
        try {
            process = kernel.GetCurrentProcess();
            // By default, Windows limits the number of pages that can be locked.
            // Thus, we need to first increase the working set size of the JVM by
            // the amount of memory we wish to lock, plus a small overhead (1MB).
            SizeT size = new SizeT(JvmInfo.jvmInfo().getMem().getHeapInit().getBytes() + (1024 * 1024));
            if (!kernel.SetProcessWorkingSetSize(process, size, size)) {
                LOGGER.warn("Unable to lock JVM memory. Failed to set working set size. Error code {}", Native.getLastError());
            } else {
                JNAKernel32Library.MemoryBasicInformation memInfo = new JNAKernel32Library.MemoryBasicInformation();
                long address = 0;
                while (kernel.VirtualQueryEx(process, new Pointer(address), memInfo, memInfo.size()) != 0) {
                    boolean lockable = memInfo.State.longValue() == JNAKernel32Library.MEM_COMMIT
                            && (memInfo.Protect.longValue() & JNAKernel32Library.PAGE_NOACCESS) != JNAKernel32Library.PAGE_NOACCESS
                            && (memInfo.Protect.longValue() & JNAKernel32Library.PAGE_GUARD) != JNAKernel32Library.PAGE_GUARD;
                    if (lockable) {
                        kernel.VirtualLock(memInfo.BaseAddress, new SizeT(memInfo.RegionSize.longValue()));
                    }
                    // Move to the next region
                    address += memInfo.RegionSize.longValue();
                }
                LOCAL_MLOCKALL = true;
            }
        } catch (UnsatisfiedLinkError e) {
            // this will have already been logged by Kernel32Library, no need to repeat it
        } finally {
            if (process != null) {
                kernel.CloseHandle(process);
            }
        }
    }

    /**
     * Retrieves the short path form of the specified path.
     *
     * @param path the path
     * @return the short path name (or the original path if getting the short path name fails for any reason)
     */
    static String getShortPathName(String path) {
        assert Constants.WINDOWS;
        try {
            final WString longPath = new WString("\\\\?\\" + path);
            // first we get the length of the buffer needed
            final int length = JNAKernel32Library.getInstance().GetShortPathNameW(longPath, null, 0);
            if (length == 0) {
                LOGGER.warn("failed to get short path name: {}", Native.getLastError());
                return path;
            }
            final char[] shortPath = new char[length];
            // knowing the length of the buffer, now we get the short name
            if (JNAKernel32Library.getInstance().GetShortPathNameW(longPath, shortPath, length) > 0) {
                return Native.toString(shortPath);
            } else {
                LOGGER.warn("failed to get short path name: {}", Native.getLastError());
                return path;
            }
        } catch (final UnsatisfiedLinkError e) {
            return path;
        }
    }

    static void addConsoleCtrlHandler(ConsoleCtrlHandler handler) {
        // The console Ctrl handler is necessary on Windows platforms only.
        if (Constants.WINDOWS) {
            try {
                boolean result = JNAKernel32Library.getInstance().addConsoleCtrlHandler(handler);
                if (result) {
                    LOGGER.debug("console ctrl handler correctly set");
                } else {
                    LOGGER.warn("unknown error {} when adding console ctrl handler", Native.getLastError());
                }
            } catch (UnsatisfiedLinkError e) {
                // this will have already been logged by Kernel32Library, no need to repeat it
            }
        }
    }
}
