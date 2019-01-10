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

package org.elasticsearch.discovery.zen;

import org.apache.logging.log4j.message.ParameterizedMessage;
import org.elasticsearch.common.component.AbstractComponent;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.TransportAddress;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * An implementation of {@link UnicastHostsProvider} that reads hosts/ports
 * from {@link #UNICAST_HOSTS_FILE}.
 *
 * Each unicast host/port that is part of the discovery process must be listed on
 * a separate line.  If the port is left off an entry, a default port of 9300 is
 * assumed.  An example unicast hosts file could read:
 *
 * 67.81.244.10
 * 67.81.244.11:9305
 * 67.81.244.15:9400
 */
public class FileBasedUnicastHostsProvider extends AbstractComponent implements UnicastHostsProvider {

    public static final String UNICAST_HOSTS_FILE = "unicast_hosts.txt";

    private final Path unicastHostsFilePath;
    private final Path legacyUnicastHostsFilePath;

    public FileBasedUnicastHostsProvider(Settings settings, Path configFile) {
        super(settings);
        this.unicastHostsFilePath = configFile.resolve(UNICAST_HOSTS_FILE);
        this.legacyUnicastHostsFilePath = configFile.resolve("discovery-file").resolve(UNICAST_HOSTS_FILE);
    }

    private List<String> getHostsList() {
        if (Files.exists(unicastHostsFilePath)) {
            return readFileContents(unicastHostsFilePath);
        }

        if (Files.exists(legacyUnicastHostsFilePath)) {
            deprecationLogger.deprecated("Found dynamic hosts list at [{}] but this path is deprecated. This list should be at [{}] " +
                "instead. Support for the deprecated path will be removed in future.", legacyUnicastHostsFilePath, unicastHostsFilePath);
            return readFileContents(legacyUnicastHostsFilePath);
        }

        logger.warn("expected, but did not find, a dynamic hosts list at [{}]", unicastHostsFilePath);

        return Collections.emptyList();
    }

    private List<String> readFileContents(Path path) {
        try (Stream<String> lines = Files.lines(path)) {
            return lines.filter(line -> line.startsWith("#") == false) // lines starting with `#` are comments
                .collect(Collectors.toList());
        } catch (IOException e) {
            logger.warn(() -> new ParameterizedMessage("failed to read file [{}]", unicastHostsFilePath), e);
            return Collections.emptyList();
        }
    }

    @Override
    public List<TransportAddress> buildDynamicHosts(HostsResolver hostsResolver) {
        final List<TransportAddress> transportAddresses = hostsResolver.resolveHosts(getHostsList(), 1);
        logger.debug("seed addresses: {}", transportAddresses);
        return transportAddresses;
    }
}
