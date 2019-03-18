/*
 * This file is part of a module with proprietary Enterprise Features.
 *
 * Licensed to Crate.io Inc. ("Crate.io") under one or more contributor
 * license agreements.  See the NOTICE file distributed with this work for
 * additional information regarding copyright ownership.
 *
 * Unauthorized copying of this file, via any medium is strictly prohibited.
 *
 * To use this file, Crate.io must have given you permission to enable and
 * use such Enterprise Features and you must have a valid Enterprise or
 * Subscription Agreement with Crate.io.  If you enable or use the Enterprise
 * Features, you represent and warrant that you have a valid Enterprise or
 * Subscription Agreement with Crate.io.  Your use of the Enterprise Features
 * if governed by the terms and conditions of your Enterprise or Subscription
 * Agreement with Crate.io.
 */

package io.crate.license;

import com.google.common.annotations.VisibleForTesting;
import io.crate.action.FutureActionListener;
import io.crate.license.exception.InvalidLicenseException;
import io.crate.settings.SharedSettings;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.action.support.master.AcknowledgedResponse;
import org.elasticsearch.cluster.ClusterChangedEvent;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.ClusterStateListener;
import org.elasticsearch.cluster.node.DiscoveryNodes;
import org.elasticsearch.common.Nullable;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.BoundTransportAddress;
import org.elasticsearch.common.transport.TransportAddress;
import org.elasticsearch.gateway.GatewayService;
import org.elasticsearch.transport.TransportService;

import java.io.IOException;
import java.time.Instant;
import java.util.Arrays;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Predicate;

import static io.crate.license.License.Type;
import static io.crate.license.LicenseKey.decode;


/**
 * The service responsible for the license generation and verification.
 *
 *
 * There are two types of licenses (see {@link Type}):
 * - A {@link Type#TRIAL} license, which is generated by the system if no license already exists
 * - A paid {@link Type#ENTERPRISE} license which can be set with the `set license` statement
 *
 * The produced {@link LicenseKey} is the encoded information of the {@link License}:
 * {@code type}
 * {@code version}
 * {@code encryptedContent}
 *
 * The encryptedContent is the encrypted representation of the {@link LicenseData}:
 * {@code ExpirationDateInMs}
 * {@code issuedTo}
 * {@code maxNumberOfNodes}
 *
 * For {@link Type#TRIAL} licenses, we use symmetric Cryptography,
 * while for {@link Type#ENTERPRISE} licenses, asymmetric.
 *
 */
public class EnterpriseLicenseService implements LicenseService, ClusterStateListener {

    static final long UNLIMITED_EXPIRY_DATE_IN_MS = Long.MAX_VALUE;
    static final int MAX_NODES_FOR_V1_LICENSES = 10;
    private static final int MAX_NODES_FOR_V2_LICENSES = 3;

    private final Logger logger;
    private final TransportService transportService;
    private final TransportSetLicenseAction transportSetLicenseAction;
    private final boolean enterpriseEnabled;

    /**
     * true if the instance is bound to localhost (=development mode), otherwise false.
     * null if the boundAddress isn't initialized yet.
     */
    private Boolean isBoundToLocalhost = null;

    private final AtomicReference<LicenseData> currentLicense = new AtomicReference<>();
    private final AtomicBoolean isMaxNumberOfNodesExceeded = new AtomicBoolean(false);

    @Inject
    public EnterpriseLicenseService(Settings settings,
                                    TransportService transportService,
                                    TransportSetLicenseAction transportSetLicenseAction) {
        enterpriseEnabled = SharedSettings.ENTERPRISE_LICENSE_SETTING.setting().get(settings);
        this.transportService = transportService;
        this.transportSetLicenseAction = transportSetLicenseAction;
        this.logger = LogManager.getLogger(getClass());
    }

    public CompletableFuture<Long> registerLicense(final String licenseKey) {
        return registerLicense(new LicenseKey(licenseKey));
    }

    private CompletableFuture<Long> registerLicense(final LicenseKey licenseKey) {
        FutureActionListener<AcknowledgedResponse, Long> listener = new FutureActionListener<>(r -> 1L);
        if (verifyLicense(licenseKey)) {
            transportSetLicenseAction.execute(new SetLicenseRequest(licenseKey), listener);
        } else {
            listener.onFailure(new InvalidLicenseException("Unable to register the provided license key"));
        }
        return listener;
    }

    static boolean verifyLicense(LicenseKey licenseKey) {
        try {
            License decodedLicense = decode(licenseKey);
            return !isLicenseExpired(decodedLicense.licenseData());
        } catch (IOException e) {
            return false;
        }
    }

    private static boolean isLicenseExpired(@Nullable LicenseData licenseData) {
        return licenseData != null && licenseData.isExpired();
    }

    @Override
    public LicenseState getLicenseState() {
        // We consider an instance that is bound to loopback as a development instance and by-pass the license expiration.
        // This makes it easier for us to run our tests.
        if (enterpriseEnabled == false || boundToLocalhost()) {
            return LicenseState.VALID;
        }
        if (isMaxNumberOfNodesExceeded()) {
            return LicenseState.MAX_NODES_VIOLATED;
        }
        if (isLicenseExpired(currentLicense())) {
            return LicenseState.EXPIRED;
        }
        return LicenseState.VALID;
    }

    private boolean boundToLocalhost() {
        if (isBoundToLocalhost == null) {
            // boundAddress is initialized in LifecycleComponent.start;
            // We guard here against changes in the initialization order of the components
            BoundTransportAddress boundTransportAddress = transportService.boundAddress();
            if (boundTransportAddress == null) {
                return false;
            }
            Predicate<TransportAddress> isLoopbackAddress = t -> t.address().getAddress().isLoopbackAddress();
            isBoundToLocalhost = Arrays.stream(boundTransportAddress.boundAddresses()).allMatch(isLoopbackAddress)
                                 && isLoopbackAddress.test(boundTransportAddress.publishAddress());
        }
        return isBoundToLocalhost;
    }

    @Override
    @Nullable
    public LicenseData currentLicense() {
        return currentLicense.get();
    }

    @Override
    @Nullable
    public ClusterStateListener clusterStateListener() {
        return this;
    }

    boolean isMaxNumberOfNodesExceeded() {
        return isMaxNumberOfNodesExceeded.get();
    }

    private LicenseKey getLicenseMetadata(ClusterState clusterState) {
        return clusterState.getMetaData().custom(LicenseKey.WRITEABLE_TYPE);
    }

    private void registerTrialLicense(ClusterState clusterState) {
        DiscoveryNodes nodes = clusterState.getNodes();
        if (nodes != null) {
            if (nodes.isLocalNodeElectedMaster()) {
                LicenseData licenseData = new LicenseData(
                    UNLIMITED_EXPIRY_DATE_IN_MS,
                    "Trial-" + clusterState.getClusterName().value(),
                    MAX_NODES_FOR_V2_LICENSES
                );
                LicenseKey licenseKey = TrialLicense.createLicenseKey(LicenseKey.VERSION, licenseData);
                registerLicense(licenseKey).whenComplete((ignored, t) -> {
                    if (t != null) {
                        logger.error("Unable to register license", t);
                    }
                });
            }
        }
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
        } else if (newLicenseKey != null && !newLicenseKey.equals(previousLicenseKey)) {
            try {
                LicenseData licenseData = decode(newLicenseKey).licenseData();
                if (previousLicenseKey == null) {
                    long expiryDateInMs = licenseData.expiryDateInMs();
                    logger.info("License loaded. issuedTo={} maxNodes={} expiration={}",
                        licenseData.issuedTo(),
                        licenseData.maxNumberOfNodes(),
                        expiryDateInMs == Long.MAX_VALUE ? "None" : Instant.ofEpochMilli(expiryDateInMs));
                }
                LicenseExpiryNotification expiryNotification = LicenseExpiryNotification.of(licenseData);

                if (expiryNotification != null) {
                    long millisToExpiration = licenseData.millisToExpiration();
                    if (expiryNotification.equals(LicenseExpiryNotification.EXPIRED)) {
                        logger.error(expiryNotification.notificationMessage(millisToExpiration));
                    } else if (expiryNotification.equals(LicenseExpiryNotification.SEVERE)) {
                        logger.error(expiryNotification.notificationMessage(millisToExpiration));
                    } else if (expiryNotification.equals(LicenseExpiryNotification.MODERATE)) {
                        logger.warn(expiryNotification.notificationMessage(millisToExpiration));
                    }
                }

                this.currentLicense.set(licenseData);
            } catch (IOException e) {
                logger.error("Received invalid license. Unable to read the license data.");
            }
        }
        onUpdatedLicense(currentState, currentLicense());
    }

    @VisibleForTesting
    void onUpdatedLicense(ClusterState clusterState,
                          @Nullable LicenseData currentLicense) {
        if (currentLicense == null) {
            // no license is registered yet
            isMaxNumberOfNodesExceeded.set(false);
        } else {
            isMaxNumberOfNodesExceeded.set(clusterState.nodes().getSize() > currentLicense.maxNumberOfNodes());
        }
    }
}
