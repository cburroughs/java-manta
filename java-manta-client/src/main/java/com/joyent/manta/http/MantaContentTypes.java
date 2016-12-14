/*
 * Copyright (c) 2016, Joyent, Inc. All rights reserved.
 */
package com.joyent.manta.http;

/**
 * Enum defining custom content-types used only by Manta.
 *
 * @author <a href="https://github.com/dekobon">Elijah Zupancic</a>
 * @since 3.0.0
 */
public enum MantaContentTypes {
    /**
     * The content-type used to represent Manta directory resources in http requests.
     */
    DIRECTORY_LIST("application/json; type=directory"),

    /**
     * The content-type used to represent Manta link resources.
     */
    SNAPLINK("application/json; type=link");

    /**
     * Plain-text representation of the enum's content-type.
     */
    private final String contentType;

    /**
     * Creates a new instance with the specified content-type.
     *
     * @param contentType content-type to initialize
     */
    MantaContentTypes(final String contentType) {
        this.contentType = contentType;
    }

    /**
     * @return the <a href="https://tools.ietf.org/html/rfc2616#page-124">RFC 2616 content-type</a>
     */
    public String getContentType() {
        return this.contentType;
    }

    @Override
    public String toString() {
        return getContentType();
    }
}