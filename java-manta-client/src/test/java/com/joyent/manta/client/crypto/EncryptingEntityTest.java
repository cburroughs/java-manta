package com.joyent.manta.client.crypto;

import com.joyent.manta.http.entity.ExposedStringEntity;
import com.joyent.manta.http.entity.MantaInputStreamEntity;
import org.apache.commons.codec.Charsets;
import org.apache.commons.io.FileUtils;
import org.apache.commons.io.IOUtils;
import org.apache.http.HttpEntity;
import org.testng.Assert;
import org.testng.annotations.Test;

import javax.crypto.AEADBadTagException;
import javax.crypto.Cipher;
import javax.crypto.CipherInputStream;
import javax.crypto.SecretKey;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.net.URL;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.charset.Charset;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.security.SecureRandom;
import java.util.Arrays;
import java.util.function.Predicate;

import static java.nio.file.StandardOpenOption.READ;
import static java.nio.file.StandardOpenOption.WRITE;

@Test
public class EncryptingEntityTest {
    private final byte[] keyBytes;

    {
        try {
            keyBytes = "FFFFFFFBD96783C6C91E2222".getBytes("US-ASCII");
        } catch (UnsupportedEncodingException e) {
            throw new AssertionError(e.getMessage());
        }
    }

    /* AES-GCM-NoPadding Tests */

    public void canEncryptAndDecryptToAndFromFileInAesGcm() throws Exception {
        verifyEncryptionWorksRoundTrip(keyBytes, AesGcmCipherDetails.INSTANCE);
    }

    public void canEncryptAndDecryptToAndFromFileWithManySizesInAesGcm() throws Exception {
        canEncryptAndDecryptToAndFromFileWithManySizes(AesGcmCipherDetails.INSTANCE);
    }

    public void canCountBytesFromStreamWithUnknownLengthInAesGcm() throws Exception {
        canCountBytesFromStreamWithUnknownLength(AesGcmCipherDetails.INSTANCE);
    }

    @Test(expectedExceptions = AEADBadTagException.class)
    public void canEncryptAndDecryptToAndFromFileInAesGcmAndThrowWhenCiphertextIsAltered()
            throws Exception {
        SupportedCipherDetails cipherDetails = AesGcmCipherDetails.INSTANCE;
        final ClassLoader classLoader = Thread.currentThread().getContextClassLoader();
        URL resource = classLoader.getResource("com/joyent/manta/client/crypto/EncryptingEntityTest.class");
        Path path = Paths.get(resource.toURI());
        long size = path.toFile().length();

        MantaInputStreamEntity entity = new MantaInputStreamEntity(resource.openStream(),
                size);

        SecretKey key = SecretKeyUtils.loadKey(keyBytes, cipherDetails);

        EncryptingEntity encryptingEntity = new EncryptingEntity(key,
                cipherDetails, entity, new SecureRandom());

        File file = File.createTempFile("ciphertext-", ".data");
        FileUtils.forceDeleteOnExit(file);

        try (FileOutputStream out = new FileOutputStream(file)) {
            encryptingEntity.writeTo(out);
        }

        Assert.assertEquals(file.length(), encryptingEntity.getContentLength());

        try (FileChannel fc = (FileChannel.open(file.toPath(), READ, WRITE))) {
            fc.position(2);
            ByteBuffer buff = ByteBuffer.wrap(new byte[] { 20, 20 });
            fc.write(buff);
        }

        byte[] iv = encryptingEntity.getCipher().getIV();
        Cipher cipher = cipherDetails.getCipher();
        cipher.init(Cipher.DECRYPT_MODE, key, cipherDetails.getEncryptionParameterSpec(iv));

        try (FileInputStream in = new FileInputStream(file);
             CipherInputStream cin = new CipherInputStream(in, cipher)) {
            IOUtils.toByteArray(cin);
        } catch (IOException e) {
            Throwable cause = e.getCause();

            if (cause instanceof AEADBadTagException) {
                throw (AEADBadTagException)cause;
            } else {
                throw e;
            }
        }
    }

    /* AES-CTR-NoPadding Tests */

    public void canEncryptAndDecryptToAndFromFileInAesCtr() throws Exception {
        verifyEncryptionWorksRoundTrip(keyBytes, AesCtrCipherDetails.INSTANCE);
    }

    public void canEncryptAndDecryptToAndFromFileWithManySizesInAesCtr() throws Exception {
        canEncryptAndDecryptToAndFromFileWithManySizes(AesCtrCipherDetails.INSTANCE);
    }

    public void canCountBytesFromStreamWithUnknownLengthInAesCtr() throws Exception {
        canCountBytesFromStreamWithUnknownLength(AesCtrCipherDetails.INSTANCE);
    }

    /* AES-CBC-PKCS5Padding Tests */

    public void canEncryptAndDecryptToAndFromFileInAesCbc() throws Exception {
        verifyEncryptionWorksRoundTrip(keyBytes, AesCbcCipherDetails.INSTANCE);
    }

    public void canEncryptAndDecryptToAndFromFileWithManySizesInAesCbc() throws Exception {
        canEncryptAndDecryptToAndFromFileWithManySizes(AesCbcCipherDetails.INSTANCE);
    }

    public void canCountBytesFromStreamWithUnknownLengthInAesCbc() throws Exception {
        canCountBytesFromStreamWithUnknownLength(AesCbcCipherDetails.INSTANCE);
    }

    /* Test helper methods */

    private void canCountBytesFromStreamWithUnknownLength(SupportedCipherDetails cipherDetails)
            throws Exception {
        final ClassLoader classLoader = Thread.currentThread().getContextClassLoader();
        URL resource = classLoader.getResource("com/joyent/manta/client/crypto/EncryptingEntityTest.class");
        Path path = Paths.get(resource.toURI());
        long size = path.toFile().length();

        MantaInputStreamEntity entity = new MantaInputStreamEntity(resource.openStream());

        Assert.assertEquals(entity.getContentLength(), -1L,
                "Content length should be set to unknown value");

        verifyEncryptionWorksRoundTrip(keyBytes, cipherDetails,
                entity, (actualBytes) -> {
                    Assert.assertEquals(actualBytes.length, size,
                            "Incorrect number of bytes counted");
                    return true;
                });
    }

    private void canEncryptAndDecryptToAndFromFileWithManySizes(SupportedCipherDetails cipherDetails)
            throws Exception {
        final Charset charset = Charsets.US_ASCII;
        final int maxLength = 1025;

        for (int i = 0; i < maxLength; i++) {
            final char[] chars = new char[i];
            Arrays.fill(chars, 'z');
            final String expectedString = String.valueOf(chars);

            ExposedStringEntity stringEntity = new ExposedStringEntity(
                    expectedString, charset);

            verifyEncryptionWorksRoundTrip(keyBytes, cipherDetails,
                    stringEntity, (actualBytes) -> {
                        final String actual = new String(actualBytes, charset);
                        Assert.assertEquals(actual, expectedString,
                                "Plaintext doesn't match decrypted value");
                        return true;
                    });
        }
    }

    private static void verifyEncryptionWorksRoundTrip(
            byte[] keyBytes, SupportedCipherDetails cipherDetails) throws Exception {
        final Charset charset = Charsets.US_ASCII;
        final String expectedString = "012345678901245601234567890124";
        ExposedStringEntity stringEntity = new ExposedStringEntity(
                expectedString, charset);

        verifyEncryptionWorksRoundTrip(keyBytes, cipherDetails, stringEntity,
                (actualBytes) -> {
            final String actual = new String(actualBytes, charset);
            Assert.assertEquals(actual, expectedString,
                    "Plaintext doesn't match decrypted value");
            return true;
        });
    }

    private static void verifyEncryptionWorksRoundTrip(byte[] keyBytes,
                                                       SupportedCipherDetails cipherDetails,
                                                       HttpEntity entity,
                                                       Predicate<byte[]> validator)
            throws Exception {
        SecretKey key = SecretKeyUtils.loadKey(keyBytes, cipherDetails);

        EncryptingEntity encryptingEntity = new EncryptingEntity(key,
                cipherDetails, entity, new SecureRandom());

        File file = File.createTempFile("ciphertext-", ".data");
        FileUtils.forceDeleteOnExit(file);

        try (FileOutputStream out = new FileOutputStream(file)) {
            encryptingEntity.writeTo(out);
        }

        Assert.assertEquals(file.length(), encryptingEntity.getContentLength());

        byte[] iv = encryptingEntity.getCipher().getIV();
        Cipher cipher = cipherDetails.getCipher();
        cipher.init(Cipher.DECRYPT_MODE, key, cipherDetails.getEncryptionParameterSpec(iv));

        try (FileInputStream in = new FileInputStream(file);
             CipherInputStream cin = new CipherInputStream(in, cipher)) {
            final byte[] actualBytes = IOUtils.toByteArray(cin);

            Assert.assertTrue(validator.test(actualBytes),
                    "Entity validation failed");
        }
    }
}
