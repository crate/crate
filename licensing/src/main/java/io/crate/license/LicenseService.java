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

package io.crate.license;

import io.crate.license.exception.InvalidLicenseException;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.cluster.ClusterChangedEvent;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.ClusterStateListener;
import org.elasticsearch.cluster.node.DiscoveryNodes;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.Nullable;
import org.elasticsearch.common.component.AbstractLifecycleComponent;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.gateway.Gateway;
import org.elasticsearch.gateway.GatewayService;

import java.io.IOException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import static io.crate.license.LicenseExpiryNotification.EXPIRED;
import static io.crate.license.LicenseExpiryNotification.MODERATE;
import static io.crate.license.LicenseExpiryNotification.SEVERE;
import static io.crate.license.LicenseKey.SELF_GENERATED;
import static io.crate.license.LicenseKey.decodeLicense;
import static io.crate.license.SelfGeneratedLicense.decryptLicenseContent;

public class LicenseService extends AbstractLifecycleComponent implements ClusterStateListener, Gateway.GatewayStateRecoveredListener {

    private final TransportSetLicenseAction transportSetLicenseAction;
    private final ClusterService clusterService;

    private AtomicReference<DecryptedLicenseData> currentLicense = new AtomicReference<>();

    @Inject
    public LicenseService(Settings settings,
                          TransportSetLicenseAction transportSetLicenseAction,
                          ClusterService clusterService) {
        super(settings);
        this.transportSetLicenseAction = transportSetLicenseAction;
        this.clusterService = clusterService;
    }

    public void registerLicense(final LicenseKey licenseKey,
                                final ActionListener<SetLicenseResponse> listener) {
        if (verifyLicense(licenseKey)) {
            transportSetLicenseAction.execute(new SetLicenseRequest(licenseKey), listener);
        } else {
            listener.onFailure(new InvalidLicenseException("Unable to register the provided license key"));
        }
    }

    /**
     * Encrypts the provided license data and creates a #{@link LicenseKey}
     */
    LicenseKey createLicenseKey(int licenseType, int version, DecryptedLicenseData decryptedLicenseData) {
        byte[] encryptedContent;
        if (licenseType == SELF_GENERATED) {
            encryptedContent = SelfGeneratedLicense.encryptLicenseContent(decryptedLicenseData.formatLicenseData());
        } else {
            throw new UnsupportedOperationException("Only self generated licenses are supported.");
        }
        return LicenseKey.createLicenseKey(licenseType, version, encryptedContent);
    }

    boolean verifyLicense(LicenseKey licenseKey) {
        try {
            DecodedLicense decodedLicense = decodeLicense(licenseKey);
            return !isLicenseExpired(licenseData(decodedLicense));
        } catch (IOException e) {
            return false;
        }
    }

    @Nullable
    public LicenseExpiryNotification getLicenseExpiryNotification(DecryptedLicenseData decryptedLicenseData) {
        long millisToExpiration = decryptedLicenseData.millisToExpiration();
        if (millisToExpiration < 0) {
            return EXPIRED;
        } else if (millisToExpiration < TimeUnit.DAYS.toMillis(1)) {
            return SEVERE;
        } else if (millisToExpiration < TimeUnit.DAYS.toMillis(15)) {
            return MODERATE;
        }
        return null;
    }

    private boolean isLicenseExpired(DecryptedLicenseData decryptedLicenseData) {
        if (decryptedLicenseData == null) {
            return false;
        }
        return System.currentTimeMillis() > decryptedLicenseData.expirationDateInMs();
    }

    DecryptedLicenseData licenseData(DecodedLicense decodedLicense) throws IOException {
        if (decodedLicense.type() == LicenseKey.SELF_GENERATED) {
            return decryptLicenseContent(decodedLicense.encryptedContent());
        } else {
            throw new UnsupportedOperationException("Only self generated licenses are supported.");
        }
    }

    @Nullable
    public DecryptedLicenseData currentLicense() {
        return currentLicense.get();
    }

    @Override
    protected void doStart() {
        clusterService.addListener(this);
    }

    private LicenseKey getLicenseMetadata(ClusterState clusterState) {
        return clusterState.getMetaData().custom(LicenseKey.WRITEABLE_TYPE);
    }

    private void registerSelfGeneratedLicense(ClusterState clusterState) {
        DiscoveryNodes nodes = clusterState.getNodes();
        if (nodes != null) {
            if (nodes.isLocalNodeElectedMaster()) {
                DecryptedLicenseData licenseData = new DecryptedLicenseData(Long.MAX_VALUE, clusterState.getClusterName().value());
                LicenseKey licenseKey = createLicenseKey(
                    LicenseKey.SELF_GENERATED,
                    LicenseKey.VERSION,
                    licenseData
                );
                registerLicense(licenseKey,
                    new ActionListener<SetLicenseResponse>() {

                        @Override
                        public void onResponse(SetLicenseResponse setLicenseResponse) {
                        }

                        @Override
                        public void onFailure(Exception e) {
                            logger.error("Unable to register license", e);
                        }
                    });
            }
        }
    }

    @Override
    protected void doStop() {
    }

    @Override
    protected void doClose() {
        clusterService.removeListener(this);
        currentLicense.set(null);
    }

    @Override
    public void clusterChanged(ClusterChangedEvent event) {
        ClusterState currentState = event.state();

        if (currentState.blocks().hasGlobalBlock(GatewayService.STATE_NOT_RECOVERED_BLOCK)) {
            return;
        }

        LicenseKey previousLicenseKey = getLicenseMetadata(event.previousState());
        LicenseKey newLicenseKey = getLicenseMetadata(currentState);

        if (previousLicenseKey == null && newLicenseKey == null) {
            registerSelfGeneratedLicense(currentState);
            return;
        }

        if (newLicenseKey != null && !newLicenseKey.equals(previousLicenseKey)) {
            try {
                DecryptedLicenseData decryptedLicenseData = licenseData(decodeLicense(newLicenseKey));
                LicenseExpiryNotification expiryNotification = getLicenseExpiryNotification(decryptedLicenseData);

                if (expiryNotification != null) {
                    long millisToExpiration = decryptedLicenseData.millisToExpiration();
                    if (expiryNotification.equals(LicenseExpiryNotification.EXPIRED)) {
                        logger.error(expiryNotification.notificationMessage(millisToExpiration));
                    } else if (expiryNotification.equals(LicenseExpiryNotification.SEVERE)) {
                        logger.error(expiryNotification.notificationMessage(millisToExpiration));
                    } else if (expiryNotification.equals(LicenseExpiryNotification.MODERATE)) {
                        logger.warn(expiryNotification.notificationMessage(millisToExpiration));
                    }
                }

                this.currentLicense.set(decryptedLicenseData);
            } catch (IOException e) {
                logger.error("Received invalid license. Unable to read the license data.");
            }
        }
    }

    @Override
    public void onSuccess(ClusterState build) {
    }

    @Override
    public void onFailure(String s) {
    }
}
