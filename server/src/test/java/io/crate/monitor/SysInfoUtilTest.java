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

package io.crate.monitor;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.List;

import org.elasticsearch.test.ESTestCase;
import org.junit.Test;

public class SysInfoUtilTest extends ESTestCase {

    private static final SysInfo.Builder SYSINFO_BUILDER = new SysInfo.Builder();
    private static final String[] X86_64 = new String[] {"x86_64", "amd64", "x64"};

    private void assertVendorVersionFromGenericLine(String line, String expectedVersionString) {
        SysInfo sysInfo = new SysInfo();
        SYSINFO_BUILDER.parseGenericVendorLine(sysInfo, line);
        assertThat(expectedVersionString).isEqualTo(sysInfo.vendorVersion());
    }

    @Test
    public void testParseKeyValue() {
        assertThat(SysInfo.Builder.parseKeyValuePair("KEY=\"val\"ue\"")).isEqualTo(new String[]{"KEY", "val\"ue"});
        assertThat(SysInfo.Builder.parseKeyValuePair("KEY=\"value\"")).isEqualTo(new String[]{"KEY", "value"});
        assertThat(SysInfo.Builder.parseKeyValuePair("KEY=value")).isEqualTo(new String[]{"KEY", "value"});
        assertThat(SysInfo.Builder.parseKeyValuePair("KEY=")).isEqualTo(new String[]{"KEY", ""});
        assertThat(SysInfo.Builder.parseKeyValuePair("KEY")).isEqualTo(new String[]{"KEY", ""});
        assertThat(SysInfo.Builder.parseKeyValuePair("")).isEqualTo(new String[]{"", ""});
    }

    @Test
    public void testWindows() {
        SysInfo sysInfo = new SysInfo.Builder()
            .withName("Windows 10")
            .withVersion("10.0")
            .withArch("x86_64")
            .gather();
        assertThat(sysInfo.arch()).isEqualTo("x86_64");
        assertThat(sysInfo.description()).isEqualTo("Microsoft Windows 10");
        assertThat(sysInfo.machine()).isIn(X86_64);
        assertThat(sysInfo.name()).isEqualTo("Win32");
        assertThat(sysInfo.patchLevel()).isEqualTo("");
        assertThat(sysInfo.vendor()).isEqualTo("Microsoft");
        assertThat(sysInfo.vendorCodeName()).isEqualTo("");
        assertThat(sysInfo.vendorName()).isEqualTo("Windows 10");
        assertThat(sysInfo.vendorVersion()).isEqualTo("10");
        assertThat(sysInfo.version()).isEqualTo("10.0");
    }

    @Test
    public void testMacOS() {
        SysInfo sysInfo = new SysInfo.Builder()
            .withName("Mac OS")
            .withVersion("10.12.6")
            .withArch("x86_64")
            .gather();
        assertThat(sysInfo.arch()).isEqualTo("x86_64");
        assertThat(sysInfo.description()).isEqualTo("Mac OS X (Sierra)");
        assertThat(sysInfo.machine()).isIn(X86_64);
        assertThat(sysInfo.name()).isEqualTo("MacOSX");
        assertThat(sysInfo.patchLevel()).isEqualTo("");
        assertThat(sysInfo.vendor()).isEqualTo("Apple");
        assertThat(sysInfo.vendorCodeName()).isEqualTo("Sierra");
        assertThat(sysInfo.vendorName()).isEqualTo("Mac OS X");
        assertThat(sysInfo.vendorVersion()).isEqualTo("10.12");
        assertThat(sysInfo.version()).isEqualTo("10.12.6");
    }

    @Test
    public void testDarwin() {
        SysInfo sysInfo = new SysInfo.Builder()
            .withName("Darwin")
            .withVersion("16.6.0")
            .withArch("x86_64")
            .gather();
        assertThat(sysInfo.arch()).isEqualTo("x86_64");
        assertThat(sysInfo.description()).isEqualTo("Mac OS X (Sierra)");
        assertThat(sysInfo.machine()).isIn(X86_64);
        assertThat(sysInfo.name()).isEqualTo("MacOSX");
        assertThat(sysInfo.patchLevel()).isEqualTo("");
        assertThat(sysInfo.vendor()).isEqualTo("Apple");
        assertThat(sysInfo.vendorCodeName()).isEqualTo("Sierra");
        assertThat(sysInfo.vendorName()).isEqualTo("Mac OS X");
        assertThat(sysInfo.vendorVersion()).isEqualTo("16.6");
        assertThat(sysInfo.version()).isEqualTo("16.6.0");
    }

    @Test
    public void testParseRedHatVendorCentOs() {
        SysInfo sysInfo = new SysInfo();
        String release = "CentOS Linux release 7.4.1708 (Core)";
        SYSINFO_BUILDER.parseRedHatVendorLine(sysInfo, release);
        assertThat(sysInfo.vendor()).isEqualTo("CentOS");
        assertThat(sysInfo.vendorCodeName()).isEqualTo("Core");
    }

    @Test
    public void testParseRedHatVendorRhel() {
        SysInfo sysInfo = new SysInfo();
        String release = "Red Hat Enterprise Linux Server release 6.7 (Santiago)";
        SYSINFO_BUILDER.parseGenericVendorLine(sysInfo, release);
        assertThat(sysInfo.vendorVersion()).isEqualTo("6.7"); // required for parseRedHatVendorLine()
        SYSINFO_BUILDER.parseRedHatVendorLine(sysInfo, release);
        assertThat(sysInfo.vendorVersion()).isEqualTo("Enterprise Linux 6");
        assertThat(sysInfo.vendorCodeName()).isEqualTo("Santiago");
    }

    @Test
    public void testGenericVendor() {
        assertVendorVersionFromGenericLine("", "");
        assertVendorVersionFromGenericLine("8", "8");
        assertVendorVersionFromGenericLine("8.10", "8.10");
        assertVendorVersionFromGenericLine("8.10.1", "8.10.1");
        assertVendorVersionFromGenericLine("buster/sid", "");
        assertVendorVersionFromGenericLine("jessie", "");
        assertVendorVersionFromGenericLine("jessie 8.10", "8.10");
        assertVendorVersionFromGenericLine("9.1 stretch", "9.1");
        assertVendorVersionFromGenericLine("9.1.x", "9.1.");
    }

    @Test
    public void testFailedSysCall() {
        // by default sys call filter is enabled, so any Runtime.getRuntime().exec(...) will fail
        List<String> result = SysInfo.sysCall(new String[]{"undefined"}, "default");
        assertThat(result.getFirst()).isEqualTo("default");
    }
}
