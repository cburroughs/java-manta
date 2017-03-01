/*
 * Copyright (c) 2016-2017, Joyent, Inc. All rights reserved.
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */
package com.joyent.manta.benchmark;

import com.joyent.manta.client.MantaClient;
import com.joyent.manta.client.multipart.EncryptedServerSideMultipartManager;
import com.joyent.manta.client.multipart.MantaMultipartUploadPart;
import com.joyent.manta.client.multipart.ServerSideMultipartUpload;
import com.joyent.manta.client.multipart.EncryptedMultipartUpload;
import com.joyent.manta.config.ConfigContext;
import com.joyent.manta.config.SettableConfigContext;
import com.joyent.manta.config.ChainedConfigContext;
import com.joyent.manta.config.DefaultsConfigContext;
import com.joyent.manta.client.crypto.SupportedCiphersLookupMap;
import com.joyent.manta.config.MapConfigContext;
import com.joyent.manta.config.SystemSettingsConfigContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import com.joyent.manta.client.crypto.SupportedCipherDetails;
import com.joyent.manta.client.crypto.SecretKeyUtils;

import java.util.ArrayList;
import java.util.List;
import java.util.Arrays;
import java.util.UUID;
import java.util.concurrent.ThreadLocalRandom;
import javax.crypto.SecretKey;
import java.util.Base64;

public final class CseBenchmark {

    private static final Logger LOG = LoggerFactory.getLogger(CseBenchmark.class);

    private static int getIntProp(String name, int def) {
        String val = System.getProperty("com.joyent.manta.benchmark.CseBenchmark." + name);
        if (val != null) {
            return Integer.valueOf(val);
        } else {
            return def;
        }
    }
    
    // TODO: Is there a  non-sucky way to make these properties with defaults?
    private static final int NUM_THREADS = getIntProp("NUM_THREADS", 1);
    private static final int OBJECT_UPLOADS_PER_THEAD = getIntProp("OBJECT_UPLOADS_PER_THEAD", 1);
    private static final int PARTS_PER_OBJECT = getIntProp("PARTS_PER_OBJECT", 2);
    private static final int PART_SIZE_BYTES = getIntProp("PART_SIZE_BYTES", 5242880);
    // TODO
    private static final boolean USE_MPU = true;


    private final UUID testRunId = UUID.randomUUID();
    private MantaClient client;
    private final String testDirectory;

    public CseBenchmark() {
        SettableConfigContext config = new ChainedConfigContext(new DefaultsConfigContext(),
                                                        new SystemSettingsConfigContext(),
                                                        new MapConfigContext(System.getProperties()));
        config.setClientEncryptionEnabled(true);
        config.setEncryptionKeyId("ephemeral-benchmark-key");
        SupportedCipherDetails cipherDetails = SupportedCiphersLookupMap.INSTANCE.get("AES256/CTR/NoPadding");
        config.setEncryptionAlgorithm(cipherDetails.getCipherId());
        SecretKey key = SecretKeyUtils.generate(cipherDetails);
        config.setEncryptionPrivateKeyBytes(key.getEncoded());
        System.out.printf("Unique secret key used for test (base64):\n%s\n",
                          Base64.getEncoder().encodeToString(key.getEncoded()));

        client = new MantaClient(config);
        testDirectory = String.format("%s/stor/java-manta-benchmark/CseBenchmark/%s",
                                      config.getMantaHomeDirectory(), testRunId);
        
    }

    public void execute() throws Exception {
        client.putDirectory(testDirectory, true);
        LOG.info("dir {} created", testDirectory);
        Thread[] threads = new Thread[NUM_THREADS];
        for (int i = 0; i < NUM_THREADS; i++) {
            threads[i] = new BenchThread(i);
        }
        for (int i = 0; i < NUM_THREADS; i++) {
            threads[i].start();
        }
        for (int i = 0; i < NUM_THREADS; i++) {
            threads[i].join();
            LOG.info("Thread {} complete", i);
        }
    }

    private class BenchThread extends Thread {
        private int completedUploads = 0;
        private int threadId;
        private ThreadLocalRandom random;
        private EncryptedServerSideMultipartManager multipartManager;

        public BenchThread(int threadId) {
            this.threadId = threadId;
            random = ThreadLocalRandom.current();
            multipartManager = new EncryptedServerSideMultipartManager(client);
        }

        @Override
        public void run() {
            while (completedUploads < OBJECT_UPLOADS_PER_THEAD) {
                try {
                    uploadObject();
                } catch (Exception e) {
                    LOG.error("uh oh", e);
                }
                completedUploads++;
            }
        }

        private void uploadObject() throws Exception {
            long startTime = System.currentTimeMillis();
            List<MantaMultipartUploadPart> parts = new ArrayList<>();
            String objectPath = testDirectory + String.format("/%d-%d", threadId, completedUploads);
            
            EncryptedMultipartUpload<ServerSideMultipartUpload> upload = multipartManager.initiateUpload(objectPath);
            for (int i=1; i <= PARTS_PER_OBJECT; i++) {
                MantaMultipartUploadPart part = multipartManager.uploadPart(upload, i, nextBytes(PART_SIZE_BYTES));
                parts.add(part);
            }
            multipartManager.complete(upload, parts);
            long endTime = System.currentTimeMillis();
            // TODO: csv
            LOG.info("upload complete {} {} {}", threadId, completedUploads, endTime-startTime); 
        }

        private byte[] nextBytes(int count) {
            final byte[] result = new byte[count];
            random.nextBytes(result);
            return result;
        }

    }


    public static void main(final String[] argv) throws Exception {
        CseBenchmark benchmark = new CseBenchmark();
        benchmark.execute();
    }
        


}
