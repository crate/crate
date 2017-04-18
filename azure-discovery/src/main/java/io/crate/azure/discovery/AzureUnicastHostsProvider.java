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

package io.crate.azure.discovery;

import com.microsoft.azure.management.network.NetworkResourceProviderClient;
import com.microsoft.azure.management.network.models.*;
import io.crate.azure.management.AzureComputeService;
import io.crate.azure.management.AzureComputeService.Discovery;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.Version;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.common.component.AbstractComponent;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.network.NetworkAddress;
import org.elasticsearch.common.network.NetworkService;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.TransportAddress;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.util.SingleObjectCache;
import org.elasticsearch.discovery.zen.UnicastHostsProvider;
import org.elasticsearch.transport.TransportService;

import java.io.IOException;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;

public class AzureUnicastHostsProvider extends AbstractComponent implements UnicastHostsProvider {

    public static enum HostType {
        PRIVATE_IP("private_ip"),
        PUBLIC_IP("public_ip");

        private String type;

        private HostType(String type) {
            this.type = type;
        }

        public static HostType fromString(String type) {
            for (HostType hostType : values()) {
                if (hostType.type.equalsIgnoreCase(type)) {
                    return hostType;
                }
            }
            return null;
        }
    }

    private final AzureComputeService azureComputeService;
    private final TransportService transportService;
    private final NetworkService networkService;
    private final TimeValue refreshInterval;

    private DiscoNodeCache cache;

    private final String resourceGroup;
    private final HostType hostType;
    private final String discoveryMethod;

    @Inject
    public AzureUnicastHostsProvider(Settings settings,
                                     AzureComputeService azureComputeService,
                                     TransportService transportService,
                                     NetworkService networkService) {
        super(settings);
        this.azureComputeService = azureComputeService;
        this.transportService = transportService;
        this.networkService = networkService;

        refreshInterval = Discovery.REFRESH.get(settings);
        resourceGroup = AzureComputeService.Management.RESOURCE_GROUP_NAME.get(settings);
        hostType = HostType.fromString(Discovery.HOST_TYPE.get(settings));
        discoveryMethod = Discovery.DISCOVERY_METHOD.get(settings);
    }

    /**
     * We build the list of Nodes from Azure Management API
     * List of discovery nodes is cached.
     * The cache time can be controlled using `cloud.azure.refresh_interval` setting.
     */
    @Override
    public List<DiscoveryNode> buildDynamicNodes() {
        if (cache == null) {
            cache = new DiscoNodeCache(refreshInterval, Collections.<DiscoveryNode>emptyList());
        }
        return cache.getOrRefresh();
    }

    private class DiscoNodeCache extends SingleObjectCache<List<DiscoveryNode>> {

        protected DiscoNodeCache(TimeValue refreshInterval, List<DiscoveryNode> initialValue) {
            super(refreshInterval, initialValue);
        }

        @Override
        protected List<DiscoveryNode> refresh() {
            ArrayList<DiscoveryNode> nodes = new ArrayList<>();
            InetAddress ipAddress;
            try {
                ipAddress = networkService.resolvePublishHostAddresses(null);
                logger.trace("ip of current node: [{}]", ipAddress);
            } catch (IOException e) {
                // We can't find the publish host address... Hmmm. Too bad :-(
                logger.warn("Cannot find publish host address/ip", e);
                return Collections.emptyList();
            }

            // In other case, it should be the right deployment so we can add it to the list of instances
            NetworkResourceProviderClient client = azureComputeService.networkResourceClient();
            if (client == null) {
                return Collections.emptyList();
            }

            try {
                final HashMap<String, String> networkNameOfCurrentHost = retrieveNetInfo(client, resourceGroup, NetworkAddress.format(ipAddress));

                if (networkNameOfCurrentHost.size() == 0) {
                    logger.error("Could not find vnet or subnet of current host");
                    return Collections.emptyList();
                }

                List<String> ipAddresses = listIPAddresses(client, resourceGroup, networkNameOfCurrentHost.get(AzureDiscovery.VNET),
                    networkNameOfCurrentHost.get(AzureDiscovery.SUBNET), discoveryMethod, hostType, logger);
                for (String networkAddress : ipAddresses) {
                    // limit to 1 port per address
                    TransportAddress[] addresses = transportService.addressesFromString(networkAddress, 1);
                    for (TransportAddress address : addresses) {
                        logger.trace("adding {}, transport_address {}", networkAddress, address);
                        nodes.add(new DiscoveryNode(
                            "#cloud-" + networkAddress, address, Version.CURRENT.minimumCompatibilityVersion()));
                    }
                }
            } catch (UnknownHostException e) {
                logger.error("Error occurred in getting hostname");
            } catch (Exception e) {
                logger.error(e.getMessage());
            }

            logger.debug("{} node(s) added", nodes.size());
            return nodes;
        }
    }

    public static HashMap<String, String> retrieveNetInfo(NetworkResourceProviderClient networkResourceProviderClient,
                                                          String rgName,
                                                          String ipAddress) throws Exception {

        HashMap<String, String> networkNames = new HashMap<>();
        ArrayList<VirtualNetwork> virtualNetworks = networkResourceProviderClient
            .getVirtualNetworksOperations()
            .list(rgName)
            .getVirtualNetworks();
        for (VirtualNetwork vn : virtualNetworks) {
            ArrayList<Subnet> subnets = vn.getSubnets();
            for (Subnet subnet : subnets) {
                for (ResourceId resourceId : subnet.getIpConfigurations()) {
                    String[] nicURI = resourceId.getId().split("/");
                    NetworkInterface nic = networkResourceProviderClient.getNetworkInterfacesOperations().get(rgName, nicURI[
                        nicURI.length - 3]).getNetworkInterface();
                    ArrayList<NetworkInterfaceIpConfiguration> ips = nic.getIpConfigurations();

                    // find public ip address
                    for (NetworkInterfaceIpConfiguration ipConfiguration : ips) {
                        if (ipAddress.equals(ipConfiguration.getPrivateIpAddress())) {
                            networkNames.put(AzureDiscovery.VNET, vn.getName());
                            networkNames.put(AzureDiscovery.SUBNET, subnet.getName());
                            break;
                        }

                    }
                }
            }
        }

        return networkNames;
    }

    public static List<String> listIPAddresses(NetworkResourceProviderClient networkResourceProviderClient,
                                               String rgName,
                                               String vnetName,
                                               String subnetName,
                                               String discoveryMethod,
                                               HostType hostType,
                                               Logger logger) {

        List<String> ipList = new ArrayList<>();
        List<ResourceId> ipConfigurations = new ArrayList<>();

        try {
            List<Subnet> subnets = networkResourceProviderClient.getVirtualNetworksOperations().get(rgName, vnetName).getVirtualNetwork().getSubnets();
            if (discoveryMethod.equalsIgnoreCase(AzureDiscovery.VNET)) {
                for (Subnet subnet : subnets) {
                    ipConfigurations.addAll(subnet.getIpConfigurations());
                }
            } else {
                for (Subnet subnet : subnets) {
                    if (subnet.getName().equalsIgnoreCase(subnetName)) {
                        ipConfigurations.addAll(subnet.getIpConfigurations());
                    }
                }

            }

            for (ResourceId resourceId : ipConfigurations) {
                String[] nicURI = resourceId.getId().split("/");
                NetworkInterface nic = networkResourceProviderClient.getNetworkInterfacesOperations().get(rgName, nicURI[
                    nicURI.length - 3]).getNetworkInterface();
                ArrayList<NetworkInterfaceIpConfiguration> ips = nic.getIpConfigurations();

                // find public ip address
                for (NetworkInterfaceIpConfiguration ipConfiguration : ips) {

                    String networkAddress = null;
                    // Let's detect if we want to use public or private IP
                    switch (hostType) {
                        case PRIVATE_IP:
                            InetAddress privateIp = InetAddress.getByName(ipConfiguration.getPrivateIpAddress());

                            if (privateIp != null) {
                                networkAddress = NetworkAddress.format(privateIp);
                            } else {
                                logger.trace("no private ip provided. ignoring [{}]...", nic.getName());
                            }
                            break;
                        case PUBLIC_IP:
                            if (ipConfiguration.getPublicIpAddress() != null) {
                                String[] pipID = ipConfiguration.getPublicIpAddress().getId().split("/");
                                PublicIpAddress pip = networkResourceProviderClient.getPublicIpAddressesOperations()
                                    .get(rgName, pipID[pipID.length - 1]).getPublicIpAddress();

                                networkAddress = NetworkAddress.format(InetAddress.getByName(pip.getIpAddress()));
                            }

                            if (networkAddress == null) {
                                logger.trace("no public ip provided. ignoring [{}]...", nic.getName());
                            }
                            break;
                    }

                    if (networkAddress == null) {
                        // We have a bad parameter here or not enough information from azure
                        logger.warn("no network address found. ignoring [{}]...", nic.getName());
                        continue;
                    } else {
                        ipList.add(networkAddress);
                    }

                }
            }
        } catch (Exception e) {
            logger.error("Could not retrieve IP addresses for unicast host list", e);
        }
        return ipList;
    }
}
