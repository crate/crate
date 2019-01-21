/*
 * Licensed to Crate under one or more contributor license agreements.
 * See the NOTICE file distributed with this work for additional
 * information regarding copyright ownership.  Crate licenses this file
 * to you under the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.  You may
 * obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
 * implied.  See the License for the specific language governing
 * permissions and limitations under the License.
 *
 * However, if you have executed another commercial license agreement
 * with Crate these terms will supersede the license and you may use the
 * software solely pursuant to the terms of the relevant commercial
 * agreement.
 */

package io.crate.monitor;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Charsets;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import org.apache.logging.log4j.Logger;
import org.apache.lucene.util.Constants;
import org.apache.logging.log4j.LogManager;

import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.io.InputStreamReader;
import java.nio.file.Files;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.function.BiConsumer;
import java.util.function.Consumer;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

/**
 * A class that provides system information as similarly as possible as it was returned by Sigar.
 * https://github.com/hyperic/sigar/blob/ad47dc3b494e9293d1f087aebb099bdba832de5e/include/sigar.h#L937-L946
 *
 * The main usage of this class is to gather kernel information that is sent via the UDC ping.
 * This is usually done by calling {@link SysInfo#gather()}.
 * Additionally it contains {@link #getSystemUptime()} to obtain the system uptime for Linux, Windows and macOS.
 */
public class SysInfo {

    private String arch = "";
    private String description = "";
    private String machine = "";
    private String name = "";
    private String patchLevel = "";
    private String vendor = "";
    private String vendorCodeName = "";
    private String vendorName = "";
    private String vendorVersion = "";
    private String version = "";

    public String arch() {
        return arch;
    }

    public String description() {
        return description;
    }

    public String machine() {
        return machine;
    }

    public String name() {
        return name;
    }

    public String patchLevel() {
        return patchLevel;
    }

    public String vendor() {
        return vendor;
    }

    public String vendorCodeName() {
        return vendorCodeName;
    }

    public String vendorName() {
        return vendorName;
    }

    public String vendorVersion() {
        return vendorVersion;
    }

    public String version() {
        return version;
    }

    private static final Logger LOGGER = LogManager.getLogger(SysInfo.class);


    private static final SysInfo INSTANCE = new SysInfo.Builder()
        .withName(Constants.OS_NAME)
        .withVersion(Constants.OS_VERSION)
        .withArch(Constants.OS_ARCH)
        .gather();

    /**
     * A builder to gather system information based on the Java system properties "os.name", "os.version" and "os.arch".
     * On Linux, providing not the actual OS's name, version and architecture may lead to incorrect results,
     * because it also depends on certain system files!
     *
     * The builder should not be used from outside of the outer class, except for testing.
     * It is used once in the {@link SysInfo} class to build {@link SysInfo#INSTANCE}.
     */
    static class Builder {

        private static final String RH_ENTERPRISE = "Red Hat Enterprise Linux";
        private static final String[] RH_OPENSOURCE = new String[] {"CentOS", "Scientific Linux"};

        /**
         * MacOS releases:
         * https://en.wikipedia.org/wiki/MacOS#Release_history
         */
        private static final Map<Integer, String> MACOS_VERSIONS = ImmutableMap.<Integer, String>builder()
            .put(0, "Cheetah") // 10.0
            .put(1, "Puma")
            .put(2, "Jaguar")
            .put(3, "Panther")
            .put(4, "Tiger")
            .put(5, "Leopard")
            .put(6, "Snow Leopard")
            .put(7, "Lion")
            .put(8, "Mountain Lion")
            .put(9, "Mavericks")
            .put(10, "Yosemite")
            .put(11, "El Capitan")
            .put(12, "Sierra")
            .put(13, "High Sierra") // 10.13
            .build();

        private static final Map<Integer, String> DARWIN_VERSIONS = ImmutableMap.<Integer, String>builder()
            .put(5, "Puma") // 10.0
            .put(6, "Jaguar")
            .put(7, "Panther")
            .put(8, "Tiger")
            .put(9, "Leopard")
            .put(10, "Snow Leopard")
            .put(11, "Lion")
            .put(12, "Mountain Lion")
            .put(13, "Mavericks")
            .put(14, "Yosemite")
            .put(15, "El Capitan")
            .put(16, "Sierra")
            .put(17, "High Sierra") // 10.13
            .build();

        /**
         * Taken from
         * https://github.com/hyperic/sigar/blob/master/src/os/linux/linux_sigar.c#L2733
         */
        private final List<LinuxVendorInfo> LINUX_VENDORS = ImmutableList.<LinuxVendorInfo>builder()
            .add(new LinuxVendorInfo("Fedora", "/etc/fedora-release", this::parseGenericVendorFile))
            .add(new LinuxVendorInfo("SuSE", "/etc/SuSE-release", this::parseGenericVendorFile))
            .add(new LinuxVendorInfo("Gentoo", "/etc/gentoo-release", this::parseGenericVendorFile))
            .add(new LinuxVendorInfo("Slackware", "/etc/slackware-release", this::parseGenericVendorFile))
            .add(new LinuxVendorInfo("Mandrake", "/etc/mandrake-release", this::parseGenericVendorFile))
            .add(new LinuxVendorInfo("VMware", "/proc/vmware/version", this::parseGenericVendorFile))
            .add(new LinuxVendorInfo("XenSource", "/etc/xensource-inventory", this::parseXenVendorFile))
            .add(new LinuxVendorInfo("Red Hat", "/etc/redhat-release", this::parseRedHatVendorFile))
            .add(new LinuxVendorInfo("lsb", "/etc/lsb-release", this::parseLsbVendorFile))
            .add(new LinuxVendorInfo("Debian", "/etc/debian_version", this::parseGenericVendorFile))
            .build();

        private class LinuxVendorInfo {
            final String name;
            final File releaseFile;
            final BiConsumer<SysInfo, File> parseFunction;

            private LinuxVendorInfo(String name, String releaseFile, BiConsumer<SysInfo, File> parseFunction) {
                this.name = name;
                this.releaseFile = new File(releaseFile);
                this.parseFunction = parseFunction;
            }
        }

        private String name = "";
        private String version = "";
        private String arch = "";

        Builder withName(String osName) {
            this.name = osName;
            return this;
        }

        Builder withVersion(String osVersion) {
            this.version = osVersion;
            return this;
        }

        Builder withArch(String osArch) {
            this.arch = osArch;
            return this;
        }

        SysInfo gather() {
            SysInfo sysinfo = new SysInfo();
            sysinfo.arch = arch;
            sysinfo.machine = getCanonicalArchitecture(arch);
            sysinfo.version = version;

            if (name.startsWith("Windows")) {
                gatherWindowsInfo(sysinfo, name);
            } else if (name.startsWith("Mac") || name.startsWith("Darwin")) {
                gatherMacOsInfo(sysinfo, name, version);
            } else if (name.startsWith("Linux") || name.startsWith("SunOS")) {
                gatherLinuxInfo(sysinfo, name);
            }
            return sysinfo;
        }

        /**
         * Windows versions
         * https://msdn.microsoft.com/en-us/library/windows/desktop/ms724832(v=vs.85).aspx
         *
         * vendorCodename is not yet implemented
         * https://en.wikipedia.org/wiki/List_of_Microsoft_codenames
         */
        private static void gatherWindowsInfo(SysInfo sysinfo, String osName) {
            sysinfo.name = "Win32";
            sysinfo.vendor = "Microsoft";
            sysinfo.vendorName = osName;
            sysinfo.vendorVersion = osName.substring(8); // "Windows ".length()
            sysinfo.description = String.format(Locale.ROOT, "%s %s", sysinfo.vendor, sysinfo.vendorName);
        }

        /**
         * Taken from
         * https://github.com/hyperic/sigar/blob/ad47dc3b494e9293d1f087aebb099bdba832de5e/src/os/darwin/darwin_sigar.c#L3614
         */
        private static void gatherMacOsInfo(SysInfo sysinfo, String osName, String osVersion) {
            String[] versions = osVersion.split("\\.");
            if (osName.startsWith("Mac")) {
                sysinfo.vendorCodeName = MACOS_VERSIONS.getOrDefault(Integer.parseInt(versions[1]), "Unknown");
            } else if (osName.startsWith("Darwin")) {
                sysinfo.vendorCodeName = DARWIN_VERSIONS.getOrDefault(Integer.parseInt(versions[0]), "Unknown");
            }

            sysinfo.name = "MacOSX";
            sysinfo.vendor = "Apple";
            sysinfo.vendorName = "Mac OS X";
            sysinfo.vendorVersion = versions[0] + "." + versions[1];
            sysinfo.description = String.format(Locale.ROOT, "%s (%s)", sysinfo.vendorName, sysinfo.vendorCodeName);
        }

        /**
         * Taken from
         * https://github.com/hyperic/sigar/blob/master/src/os/linux/linux_sigar.c#L2747
         */
        private void gatherLinuxInfo(SysInfo sysinfo, String osName) {
            sysinfo.name = "Linux";
            sysinfo.vendorName = osName;
            sysinfo.patchLevel = "unknown";

            LinuxVendorInfo vendorInfo = null;
            for (LinuxVendorInfo vi: LINUX_VENDORS) {
                if (vi.releaseFile.exists()) {
                    vendorInfo = vi;
                    break;
                }
            }

            if (vendorInfo != null) {
                sysinfo.vendor = vendorInfo.name;
                vendorInfo.parseFunction.accept(sysinfo, vendorInfo.releaseFile);
            } else {
                sysinfo.vendor = "Unknown";
            }

            if (sysinfo.description.isEmpty()) {
                sysinfo.description = String.format(Locale.ROOT, "%s %s", sysinfo.vendor, sysinfo.vendorVersion);
            }
        }

        /**
         * Taken from
         * https://github.com/hyperic/sigar/blob/master/src/os/linux/linux_sigar.c#L2585
         */
        private void parseGenericVendorFile(SysInfo sysinfo, File releaseFile) {
            // file contains only single line
            consumeFileGracefully(releaseFile, line -> parseGenericVendorLine(sysinfo, line));
        }

        @VisibleForTesting
        void parseGenericVendorLine(SysInfo sysinfo, String line) {
            if (line.isEmpty()) {
                return;
            }
            int start;
            int len = 0;
            char c;
            char[] sequence = line.toCharArray();
            for (int i = 0; i < sequence.length; i++) {
                c = sequence[i];
                while (Character.isWhitespace(c)) {
                    c = sequence[++i];
                }
                if (!Character.isDigit(c)) {
                    continue;
                }
                start = i;
                while (Character.isDigit(c) || c == 46) { // ".".charAt(0) = 46
                    ++len;
                    if (++i < sequence.length) {
                        c = sequence[i];
                    } else {
                        break;
                    }
                }
                if (len > 0) {
                    sysinfo.vendorVersion = line.substring(start, start + len);
                    return;
                }
            }
        }

        /**
         * Taken from
         * https://github.com/hyperic/sigar/blob/master/src/os/linux/linux_sigar.c#L2717
         */
        private void parseXenVendorFile(SysInfo sysinfo, File releaseFile) {
            consumeFileGracefully(releaseFile, line -> {
                String[] kv = parseKeyValuePair(line);
                switch (kv[0]) {
                    case("KERNEL_VERSION"):
                        sysinfo.version = kv[1];
                        break;
                    case("PRODUCT_VERSION"):
                        sysinfo.vendorVersion = kv[1];
                        break;
                    default:
                        // ignore key
                }
            });
            sysinfo.description = String.format(Locale.ROOT, "XenServer %s", sysinfo.vendorVersion);
        }

        /**
         * Taken from
         * https://github.com/hyperic/sigar/blob/master/src/os/linux/linux_sigar.c#L2615
         *
         * $ cat /etc/redhat-release
         * CentOS Linux release 7.4.1708 (Core)
         */
        private void parseRedHatVendorFile(SysInfo sysinfo, File releaseFile) {
            parseGenericVendorFile(sysinfo, releaseFile);
            consumeFileGracefully(releaseFile, line -> parseRedHatVendorLine(sysinfo, line));
        }

        @VisibleForTesting
        void parseRedHatVendorLine(SysInfo sysinfo, String line) {
            if (line.isEmpty()) {
                return;
            }
            int start = line.indexOf("(") + 1;
            int end = line.lastIndexOf(")");
            if (end - start > 0) {
                sysinfo.vendorCodeName = line.substring(start, end);
            }
            if (line.startsWith(RH_ENTERPRISE)) {
                sysinfo.vendorVersion = "Enterprise Linux " + sysinfo.vendorVersion.substring(0, 1);
            } else {
                for (String vendor : RH_OPENSOURCE) {
                    if (line.startsWith(vendor)) {
                        sysinfo.vendor = vendor;
                    }
                }
            }
        }

        /**
         * Taken from
         * https://github.com/hyperic/sigar/blob/master/src/os/linux/linux_sigar.c#L2701
         */
        private void parseLsbVendorFile(SysInfo sysinfo, File releaseFile) {
            consumeFileGracefully(releaseFile, line -> {
                String[] kv = parseKeyValuePair(line);
                switch (kv[0]) {
                    case("DISTRIB_ID"):
                        sysinfo.vendor = kv[1];
                        break;
                    case("DISTRIB_RELEASE"):
                        sysinfo.vendorVersion = kv[1];
                        break;
                    case("DISTRIB_CODENAME"):
                        sysinfo.vendorCodeName = kv[1];
                        break;
                    default:
                        // ignore key
                }
            });
        }

        private static void consumeFileGracefully(File fn, Consumer<String> consumer) {
            if (fn.exists()) {
                try {
                    Files.readAllLines(fn.toPath()).forEach(consumer);
                } catch (IOException e) {
                    LOGGER.debug("Failed to read '{}': {}", fn.getAbsolutePath(), e.getMessage());
                }
            }
        }

        @VisibleForTesting
        static String[] parseKeyValuePair(String line) {
            String[] kv = line.split("=");
            if (kv.length == 2) {
                if (kv[1].startsWith("\"")) {
                    kv[1] = kv[1].substring(1);
                }
                if (kv[1].endsWith("\"")) {
                    kv[1] = kv[1].substring(0, kv[1].length() - 1);
                }
                return kv;
            } else if (kv.length == 1) {
                return new String[]{kv[0], ""};
            }
            return new String[]{"", ""};
        }

        /**
         * Taken from {@link com.sun.jna.Platform}
         */
        private static String getCanonicalArchitecture(String osArch) {
            String arch = osArch.toLowerCase(Locale.ROOT).trim();
            if ("powerpc".equals(arch)) {
                arch = "ppc";
            } else if ("powerpc64".equals(arch)) {
                arch = "ppc64";
            } else if ("i386".equals(arch) || "i686".equals(arch)) {
                arch = "x86";
            } else if ("x86_64".equals(arch) || "amd64".equals(arch)) {
                arch = "x86_64";
            }
            // Work around OpenJDK mis-reporting os.arch
            // https://bugs.openjdk.java.net/browse/JDK-8073139
            if ("ppc64".equals(arch) && "little".equals(System.getProperty("sun.cpu.endian"))) {
                arch = "ppc64le";
            }
            return arch;
        }

    }

    /**
     * This is the main method to obtain the SysInfo data structure.
     */
    public static SysInfo gather() {
        return INSTANCE;
    }

    @VisibleForTesting
    static List<String> sysCall(String[] cmd, String defaultValue) {
        ProcessBuilder pb = new ProcessBuilder(cmd);
        try {
            Process p = pb.start();
            try (InputStreamReader in = new InputStreamReader(p.getInputStream(), Charsets.UTF_8)) {
                try (BufferedReader reader = new BufferedReader(in)) {
                    return reader.lines().collect(Collectors.toList());
                }
            }
        } catch (IOException e) {
            LOGGER.debug("Failed to execute process: {}", e.getMessage());
            return ImmutableList.of(defaultValue);
        }
    }

    /**
     * Retrieve system uptime in milliseconds
     * https://en.wikipedia.org/wiki/Uptime
     */
    static long getSystemUptime() {
        long uptime = -1L;
        if (Constants.WINDOWS) {
            List<String> lines = SysInfo.sysCall(new String[]{"net", "stats", "srv"}, "");
            for (String line : lines) {
                if (line.startsWith("Statistics since")) {
                    SimpleDateFormat format = new SimpleDateFormat("'Statistics since' MM/dd/yyyy hh:mm:ss a", Locale.ROOT);
                    try {
                        Date bootTime = format.parse(line);
                        return System.currentTimeMillis() - bootTime.getTime();
                    } catch (ParseException e) {
                        LOGGER.debug("Failed to parse uptime: {}", e.getMessage());
                    }
                }
            }
        } else if (Constants.LINUX) {
            File procUptime = new File("/proc/uptime");
            if (procUptime.exists()) {
                try {
                    List<String> lines = Files.readAllLines(procUptime.toPath());
                    if (!lines.isEmpty()) {
                        String[] parts = lines.get(0).split(" ");
                        if (parts.length == 2) {
                            double uptimeMillis = Float.parseFloat(parts[1]) * 1000.0;
                            return (long) uptimeMillis;
                        }
                    }
                } catch (IOException e) {
                    LOGGER.debug("Failed to read '{}': {}", procUptime.getAbsolutePath(), e.getMessage());
                }
            }
        } else if (Constants.MAC_OS_X) {
            Pattern pattern = Pattern.compile("kern.boottime: \\{ sec = (\\d+), usec = (\\d+) \\} .*");
            List<String> lines = SysInfo.sysCall(new String[]{"sysctl", "kern.boottime"}, "");
            for (String line : lines) {
                Matcher matcher = pattern.matcher(line);
                if (matcher.matches()) {
                    return Long.parseLong(matcher.group(1)) * 1000L;
                }
            }
        }
        return uptime;
    }

}
