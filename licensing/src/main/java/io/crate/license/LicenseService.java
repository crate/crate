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
import io.crate.settings.SharedSettings;
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
import org.elasticsearch.gateway.GatewayService;

import java.io.IOException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import static io.crate.license.LicenseExpiryNotification.EXPIRED;
import static io.crate.license.LicenseExpiryNotification.MODERATE;
import static io.crate.license.LicenseExpiryNotification.SEVERE;
import static io.crate.license.LicenseKey.LicenseType;
import static io.crate.license.LicenseKey.decodeLicense;


/**
 * The service responsible for the license generation and verification.
 *
 *
 * There are two types of licenses (see {@link LicenseType}):
 * - A {@link LicenseType#TRIAL} license, which is generated by the system if no license already exists
 * - A paid {@link LicenseType#ENTERPRISE} license which can be set with the `set license` statement
 *
 * The produced {@link LicenseKey} is the encoded information of the {@link DecodedLicense}:
 * {@code licenseType}
 * {@code version}
 * {@code encryptedContent}
 *
 * The encryptedContent is the encrypted representation of the {@link DecryptedLicenseData}:
 * {@code ExpirationDateInMs}
 * {@code issuedTo}
 *
 * For {@link LicenseType#TRIAL} licenses, we use symmetric Cryptography,
 * while for {@link LicenseType#ENTERPRISE} licenses, asymmetric.
 *
 * */
public class LicenseService extends AbstractLifecycleComponent implements ClusterStateListener {

    private final TransportSetLicenseAction transportSetLicenseAction;
    private final ClusterService clusterService;
    private final boolean enterpriseEnabled;

    private AtomicReference<DecryptedLicenseData> currentLicense = new AtomicReference<>();

    @Inject
    public LicenseService(Settings settings,
                          TransportSetLicenseAction transportSetLicenseAction,
                          ClusterService clusterService) {
        super(settings);
        enterpriseEnabled = SharedSettings.ENTERPRISE_LICENSE_SETTING.setting().get(settings);
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

    static DecryptedLicenseData licenseData(DecodedLicense decodedLicense) throws IOException {
        byte[] decryptedContent = decryptLicenseContent(decodedLicense.type(), decodedLicense.encryptedContent());
        return DecryptedLicenseData.fromFormattedLicenseData(decryptedContent);
    }

    static boolean verifyLicense(LicenseKey licenseKey) {
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

    static boolean isLicenseExpired(@Nullable DecryptedLicenseData decryptedLicenseData) {
        return decryptedLicenseData != null && decryptedLicenseData.isExpired();
    }

    public boolean hasValidLicense() {
        if (enterpriseEnabled == false) {
            return true;
        }
        return !isLicenseExpired(currentLicense());
    }

    @Nullable
    public DecryptedLicenseData currentLicense() {
        return currentLicense.get();
    }

    @Override
    protected void doStart() {
        if (enterpriseEnabled) {
            clusterService.addListener(this);
        }
    }

    private LicenseKey getLicenseMetadata(ClusterState clusterState) {
        return clusterState.getMetaData().custom(LicenseKey.WRITEABLE_TYPE);
    }

    private void registerTrialLicense(ClusterState clusterState) {
        DiscoveryNodes nodes = clusterState.getNodes();
        if (nodes != null) {
            if (nodes.isLocalNodeElectedMaster()) {
                long trialLicenseExpirationDateMillis = System.currentTimeMillis() + TimeUnit.DAYS.toMillis(30);
                DecryptedLicenseData licenseData = new DecryptedLicenseData(
                    trialLicenseExpirationDateMillis, clusterState.getClusterName().value());
                LicenseKey licenseKey = TrialLicense.createLicenseKey(
                    LicenseKey.VERSION, licenseData);
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

    private static byte[] decryptLicenseContent(LicenseType type, byte[] encryptedContent) {
        switch (type) {
            case TRIAL:
                return TrialLicense.decrypt(encryptedContent);

            case ENTERPRISE:
                return EnterpriseLicense.decrypt(encryptedContent);

            default:
                throw new AssertionError("Invalid license type: " + type);
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
            registerTrialLicense(currentState);
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
}
