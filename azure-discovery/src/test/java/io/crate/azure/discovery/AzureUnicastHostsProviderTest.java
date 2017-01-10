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

import com.microsoft.azure.management.network.*;
import com.microsoft.azure.management.network.models.*;
import com.microsoft.windowsazure.exception.ServiceException;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.common.logging.Loggers;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.CollectionUtils;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class AzureUnicastHostsProviderTest {

    private static final String rgName = "my_resourcegroup";
    private static final String vnetName = "myVnet";
    private static final String subnetname = "mySubnet2";
    private Logger logger;

    private NetworkResourceProviderClient providerClient = mock(NetworkResourceProviderClientImpl.class);

    @Before
    public void setUp() throws IOException, ServiceException {
        logger = Loggers.getLogger(this.getClass(), Settings.EMPTY, new String[0]);

        ResourceId resourceId = new ResourceId();
        resourceId.setId("/subscriptions/xx/resourceGroups/my_resourcegroup/providers/Microsoft.Network/networkInterfaces/nic_dummy/ipConfigurations/Nic-IP-config");

        Subnet subnet = new Subnet();
        subnet.setIpConfigurations(CollectionUtils.asArrayList(resourceId));
        subnet.setName("mySubnet");

        VirtualNetworkOperations virtualNetworkOperations = mock(VirtualNetworkOperationsImpl.class);
        VirtualNetworkGetResponse virtualNetworkGetResponse = mock(VirtualNetworkGetResponse.class);
        final NetworkInterfaceOperations networkInterfaceOperations = mock(NetworkInterfaceOperationsImpl.class);
        NetworkInterfaceGetResponse networkInterfaceGetResponse = mock(NetworkInterfaceGetResponse.class);

        NetworkInterfaceIpConfiguration ipConfiguration = new NetworkInterfaceIpConfiguration();
        ipConfiguration.setPrivateIpAddress("10.0.0.4");

        NetworkInterface nic = new NetworkInterface();
        nic.setName("nic_dummy");
        nic.setIpConfigurations(CollectionUtils.asArrayList(ipConfiguration));

        VirtualNetwork virtualNetwork = new VirtualNetwork();
        virtualNetwork.setSubnets(CollectionUtils.asArrayList(subnet));

        when(virtualNetworkGetResponse.getVirtualNetwork()).thenReturn(virtualNetwork);
        when(providerClient.getVirtualNetworksOperations()).thenReturn(virtualNetworkOperations);
        when(virtualNetworkOperations.get(rgName, vnetName)).thenReturn(virtualNetworkGetResponse);

        when(providerClient.getNetworkInterfacesOperations()).thenReturn(networkInterfaceOperations);
        when(networkInterfaceOperations.get(rgName, "nic_dummy")).thenReturn(networkInterfaceGetResponse);
        when(networkInterfaceGetResponse.getNetworkInterface()).thenReturn(nic);
    }

    @Test
    public void testSingleSubnet() throws IOException, ServiceException {
        List<String> networkAddresses = AzureUnicastHostsProvider.listIPAddresses(providerClient, rgName, vnetName, "", "vnet",
            AzureUnicastHostsProvider.HostType.PRIVATE_IP, logger);
        assertEquals(networkAddresses.size(), 1);
        assertEquals(networkAddresses.get(0), "10.0.0.4");

        List<String> networkAddresses2 = AzureUnicastHostsProvider.listIPAddresses(providerClient, rgName, vnetName, "", "vnet",
            AzureUnicastHostsProvider.HostType.PUBLIC_IP, logger);
        assertEquals(networkAddresses2.size(), 0);
    }

    @Test
    public void testMultipleSubnet() throws IOException, ServiceException {
        ResourceId resourceId2 = new ResourceId();
        resourceId2.setId("/subscriptions/xx/resourceGroups/my_resourcegroup/providers/Microsoft.Network/networkInterfaces/nic_dummy2/ipConfigurations/Nic-IP-config");

        ResourceId resourceId3 = new ResourceId();
        resourceId3.setId("/subscriptions/xx/resourceGroups/my_resourcegroup/providers/Microsoft.Network/publicIPAddresses/ip_public1");

        Subnet subnet2 = new Subnet();
        subnet2.setIpConfigurations(CollectionUtils.asArrayList(resourceId2));
        subnet2.setName("mySubnet2");

        NetworkInterfaceGetResponse networkInterfaceGetResponse2 = mock(NetworkInterfaceGetResponse.class);
        PublicIpAddressOperations publicIpAddressOperations = mock(PublicIpAddressOperationsImpl.class);
        PublicIpAddressGetResponse publicIpAddressGetResponse = mock(PublicIpAddressGetResponse.class);

        NetworkInterfaceIpConfiguration ipConfiguration2 = new NetworkInterfaceIpConfiguration();
        ipConfiguration2.setPrivateIpAddress("10.0.0.5");

        ipConfiguration2.setPublicIpAddress(resourceId3);

        PublicIpAddress publicIpAddress = new PublicIpAddress();
        publicIpAddress.setIpAddress("33.33.33.33");

        NetworkInterface nic2 = new NetworkInterface();
        nic2.setName("nic_dummy2");
        nic2.setIpConfigurations(CollectionUtils.asArrayList(ipConfiguration2));

        providerClient.getVirtualNetworksOperations().get(rgName, vnetName).getVirtualNetwork().getSubnets().add(subnet2);

        when(providerClient.getNetworkInterfacesOperations().get(rgName, "nic_dummy2")).thenReturn(networkInterfaceGetResponse2);
        when(networkInterfaceGetResponse2.getNetworkInterface()).thenReturn(nic2);

        when(providerClient.getPublicIpAddressesOperations()).thenReturn(publicIpAddressOperations);
        when(publicIpAddressOperations.get(rgName, "ip_public1")).thenReturn(publicIpAddressGetResponse);
        when(publicIpAddressGetResponse.getPublicIpAddress()).thenReturn(publicIpAddress);

        List<String> networkAddresses = AzureUnicastHostsProvider.listIPAddresses(providerClient, rgName, vnetName, subnetname, "subnet",
            AzureUnicastHostsProvider.HostType.PRIVATE_IP, logger);
        assertEquals(networkAddresses.size(), 1);
        assertEquals(networkAddresses.get(0), "10.0.0.5");

        List<String> networkAddresses2 = AzureUnicastHostsProvider.listIPAddresses(providerClient, rgName, vnetName, subnetname, "vnet",
            AzureUnicastHostsProvider.HostType.PRIVATE_IP, logger);
        assertEquals(networkAddresses2.size(), 2);
        assertEquals(networkAddresses2.contains("10.0.0.5"), true);
        assertEquals(networkAddresses2.contains("10.0.0.4"), true);

        List<String> networkAddresses3 = AzureUnicastHostsProvider.listIPAddresses(providerClient, rgName, vnetName, subnetname, "vnet",
            AzureUnicastHostsProvider.HostType.PUBLIC_IP, logger);
        assertEquals(networkAddresses3.size(), 1);
        assertEquals(networkAddresses3.contains("33.33.33.33"), true);
        assertEquals(networkAddresses3.contains("10.0.0.5"), false);
        assertEquals(networkAddresses3.contains("10.0.0.4"), false);

    }

}
