/**
 * Copyright (c) 2015, Joyent, Inc. All rights reserved.
 */
package com.joyent.manta.config;

import java.io.File;

/**
 * {@link ConfigContext} implementation that outputs nothing but the default
 * values for all of the configuration settings.
 *
 * @author <a href="https://github.com/dekobon">Elijah Zupancic</a>
 */
public class DefaultsConfigContext implements ConfigContext {
    /**
     * The default Manta service endpoint.
     */
    public static final String DEFAULT_MANTA_URL = "https://us-east.manta.joyent.com";

    /**
     * The default timeout for accessing the Manta service.
     */
    public static final int DEFAULT_HTTP_TIMEOUT = 20 * 1000;

    /**
     * We assume the default rsa key in the user's home directory.
     */
    public static final String MANTA_KEY_PATH;

    static {
        // Don't even bother setting a default key path if it doesn't exist
        String defaultKeyPath = String.format("%s/.ssh/id_rsa",
                System.getProperty("user.home"));
        File privateKeyFile = new File(defaultKeyPath);

        if (privateKeyFile.exists() && privateKeyFile.canRead()) {
            MANTA_KEY_PATH = defaultKeyPath;
        } else {
            MANTA_KEY_PATH = null;
        }
    }

    /**
     * Creates a new instance with all of the defaults assigned to the beans
     * defined in {@link ConfigContext}.
     */
    public DefaultsConfigContext() {
    }

    @Override
    public String getMantaURL() {
        return DEFAULT_MANTA_URL;
    }

    @Override
    public String getMantaUser() {
        return null;
    }

    @Override
    public String getMantaKeyId() {
        return null;
    }

    @Override
    public String getMantaKeyPath() {
        return MANTA_KEY_PATH;
    }

    @Override
    public Integer getTimeout() {
        return DEFAULT_HTTP_TIMEOUT;
    }
}