package com.joyent.manta.util;

import org.apache.commons.lang3.Validate;

import javax.crypto.Mac;
import java.io.IOException;
import java.io.OutputStream;

/**
 * {@link OutputStream} implementation that progressively implements a
 * HMAC as data is written.
 *
 * @author <a href="https://github.com/dekobon">Elijah Zupancic</a>
 * @since 3.0.0
 */
public class HmacOutputStream extends OutputStream {
    /**
     * HMAC instance used to generate HMAC for wrapped stream.
     */
    private final Mac hmac;

    /**
     * Underlying stream being wrapped.
     */
    private final OutputStream chained;

    /**
     * Creates a new instance using the specified HMAC instance and wrapping
     * the specified stream.
     *
     * @param hmac HMAC instance that has been initialized
     * @param chained stream being wrapped
     */
    public HmacOutputStream(final Mac hmac, final OutputStream chained) {
        Validate.notNull(hmac, "HMAC instance must not be null");
        Validate.notNull(chained, "OutputStream must not be null");

        this.hmac = hmac;
        this.chained = chained;
    }

    @Override
    public void write(final int b) throws IOException {
        hmac.update((byte)b);
        chained.write(b);
    }

    @Override
    public void write(final byte[] b) throws IOException {
        hmac.update(b);
        chained.write(b);
    }

    @Override
    public void write(final byte[] b, final int off, final int len) throws IOException {
        hmac.update(b, off, len);
        chained.write(b, off, len);
    }

    @Override
    public void flush() throws IOException {
        chained.flush();
    }

    @Override
    public void close() throws IOException {
        chained.close();
    }
}