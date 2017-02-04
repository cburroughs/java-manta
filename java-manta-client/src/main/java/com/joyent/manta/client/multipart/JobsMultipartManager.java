package com.joyent.manta.client.multipart;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.uuid.Generators;
import com.joyent.manta.client.MantaClient;
import com.joyent.manta.http.MantaHttpHeaders;
import com.joyent.manta.client.jobs.MantaJob;
import com.joyent.manta.client.jobs.MantaJobBuilder;
import com.joyent.manta.client.jobs.MantaJobPhase;
import com.joyent.manta.client.MantaMetadata;
import com.joyent.manta.client.MantaObject;
import com.joyent.manta.client.MantaObjectMapper;
import com.joyent.manta.client.MantaObjectResponse;
import com.joyent.manta.util.MantaUtils;
import com.joyent.manta.exception.MantaClientHttpResponseException;
import com.joyent.manta.exception.MantaException;
import com.joyent.manta.exception.MantaIOException;
import com.joyent.manta.exception.MantaMultipartException;
import org.apache.commons.lang3.Validate;
import org.apache.http.HttpStatus;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.Serializable;
import java.nio.file.Paths;
import java.time.Duration;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Function;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

import static com.joyent.manta.client.MantaClient.SEPARATOR;

/**
 * Class providing a jobs-based implementation multipart uploads to Manta.
 *
 * @author <a href="https://github.com/dekobon">Elijah Zupancic</a>
 * @since 2.5.0
 */
public class JobsMultipartManager extends AbstractMultipartManager
        <MantaMultipartUpload, MantaMultipartUploadPart> {
    /**
     * Logger instance.
     */
    private static final Logger LOGGER = LoggerFactory.getLogger(JobsMultipartManager.class);

    /**
     * Temporary storage directory on Manta for multipart data. This is a
     * randomly chosen UUID used so that we don't have directory naming
     * conflicts.
     */
    static final String MULTIPART_DIRECTORY =
            "stor/.multipart-6439b444-9041-11e6-9be2-9f622f483d01";

    /**
     * Metadata file containing information about final multipart file.
     */
    static final String METADATA_FILE = "metadata.json";

    /**
     * Number of seconds to poll Manta to see if a job is complete.
     */
    private static final long DEFAULT_SECONDS_TO_POLL = 5L;

    /**
     * Number of times to check to see if a multipart transfer has completed.
     */
    private static final int NUMBER_OF_TIMES_TO_POLL = 20;

    /**
     * Reference to {@link MantaClient} Manta client object providing access to
     * Manta.
     */
    protected final MantaClient mantaClient;

    /**
     * Full path on Manta to the upload directory.
     */
    private final String resolvedMultipartUploadDirectory;

    /**
     * Format for naming Manta jobs.
     */
    private static final String JOB_NAME_FORMAT = "multipart-%s";

    /**
     * Key name for retrieving job id from metadata.
     */
    static final String JOB_ID_METADATA_KEY = "m-multipart-job-id";

    /**
     * Key name for retrieving upload id from final object's metadata.
     */
    static final String UPLOAD_ID_METADATA_KEY = "m-multipart-upload-id";

    /**
     * Creates a new instance backed by the specified {@link MantaClient}.
     * @param mantaClient Manta client instance to use to communicate with server
     */
    public JobsMultipartManager(final MantaClient mantaClient) {
        super();

        Validate.notNull(mantaClient, "Manta client object must not be null");

        this.mantaClient = mantaClient;

        this.resolvedMultipartUploadDirectory =
                mantaClient.getContext().getMantaHomeDirectory()
                + SEPARATOR + MULTIPART_DIRECTORY;
    }

    @Override
    public Stream<MantaMultipartUpload> listInProgress() throws IOException {
        final List<Exception> exceptions = new ArrayList<>();

        /* This nesting structure is unfortunate, but an artifact of us needing
         * to close the stream when we have finished processing. */
        try (Stream<MantaObject> multipartDirList = mantaClient
                    .listObjects(this.resolvedMultipartUploadDirectory)
                    .filter(MantaObject::isDirectory)) {

            final Stream<MantaMultipartUpload> stream = multipartDirList
                    .map(object -> {
                        String idString = MantaUtils.lastItemInPath(object.getPath());
                        UUID id = UUID.fromString(idString);

                        try {
                            MultipartMetadata mantaMetadata = downloadMultipartMetadata(id);
                            return new MantaMultipartUpload(id, mantaMetadata.getPath());
                        } catch (MantaClientHttpResponseException e) {
                            if (e.getStatusCode() == HttpStatus.SC_NOT_FOUND) {
                                return null;
                            } else {
                                exceptions.add(e);
                                return null;
                            }
                        } catch (IOException | RuntimeException e) {
                            exceptions.add(e);
                            return null;
                        }
                    })
                    /* We explicitly filter out items that stopped existing when we
                     * went to get the multipart metadata because we encountered a
                     * race condition. */
                    .filter(Objects::nonNull);

            if (exceptions.isEmpty()) {
                return stream;
            }
        // This catches an exception on the initial listObjects call
        } catch (MantaClientHttpResponseException e) {
            if (e.getStatusCode() == HttpStatus.SC_NOT_FOUND) {
                return Stream.empty();
            } else {
                throw e;
            }
        }

        final MantaIOException aggregateException = new MantaIOException(
                "Problem(s) listing multipart uploads in progress");

        MantaUtils.attachExceptionsToContext(aggregateException,
                exceptions);

        throw aggregateException;
    }

    @Override
    public MantaMultipartUpload initiateUpload(final String path) throws IOException {
        return initiateUpload(path, null, null);
    }

    @Override
    public MantaMultipartUpload initiateUpload(final String path,
                                               final MantaMetadata mantaMetadata) throws IOException {
        return initiateUpload(path, mantaMetadata, null);
    }

    @Override
    public MantaMultipartUpload initiateUpload(final String path,
                                               final MantaMetadata mantaMetadata,
                                               final MantaHttpHeaders httpHeaders) throws IOException {
        final UUID uploadId = Generators.timeBasedGenerator().generate();

        if (LOGGER.isDebugEnabled()) {
            LOGGER.debug("Creating a new multipart upload [{}] for {}",
                    uploadId, path);
        }

        final String uploadDir = multipartUploadDir(uploadId);
        mantaClient.putDirectory(uploadDir, true);

        final String metadataPath = uploadDir + METADATA_FILE;

        final MultipartMetadata metadata = new MultipartMetadata()
                .setPath(path)
                .setObjectMetadata(mantaMetadata);

        if (httpHeaders != null) {
            metadata.setContentType(httpHeaders.getContentType());
        }

        final byte[] metadataBytes = MantaObjectMapper.INSTANCE.writeValueAsBytes(metadata);

        LOGGER.debug("Writing metadata to: {}", metadataPath);
        mantaClient.put(metadataPath, metadataBytes);

        return new MantaMultipartUpload(uploadId, path);
    }

    /**
     * Uploads a single part of a multipart upload.
     *
     * @param id multipart upload id
     * @param partNumber part number to identify relative location in final file
     * @param contents String contents to be written in UTF-8
     * @return multipart single part object
     * @throws IOException thrown if there is a problem connecting to Manta
     */
    @Override
    public MantaMultipartUploadPart uploadPart(final MantaMultipartUpload upload,
                                               final int partNumber,
                                               final String contents)
            throws IOException {
        final InputStream is = new ByteArrayInputStream(contents.getBytes("UTF-8"));
        return uploadPart(upload, partNumber, is);
    }


    /**
     * Uploads a single part of a multipart upload.
     *
     * @param id multipart upload id
     * @param partNumber part number to identify relative location in final file
     * @param bytes byte array containing data of the part to be uploaded
     * @return multipart single part object
     * @throws IOException thrown if there is a problem connecting to Manta
     */
    @Override
    public MantaMultipartUploadPart uploadPart(final MantaMultipartUpload upload,
                                               final int partNumber,
                                               final byte[] bytes)
            throws IOException {
        ByteArrayInputStream is = new ByteArrayInputStream(bytes);
        return uploadPart(upload, partNumber, bytes);
    }


    /**
     * Uploads a single part of a multipart upload.
     *
     * @param id multipart upload id
     * @param partNumber part number to identify relative location in final file
     * @param file file containing data of the part to be uploaded
     * @return multipart single part object
     * @throws IOException thrown if there is a problem connecting to Manta
     */
    @Override
    public MantaMultipartUploadPart uploadPart(final MantaMultipartUpload upload,
                                               final int partNumber,
                                               final File file)
            throws IOException {
        Validate.notNull(file, "File must not be null");

        if (!file.exists()) {
            String msg = String.format("File doesn't exist: %s",
                    file.getPath());
            throw new FileNotFoundException(msg);
        }

        if (!file.canRead()) {
            String msg = String.format("Can't access file for read: %s",
                    file.getPath());
            throw new IOException(msg);
        }
        return uploadPart(upload, partNumber, new FileInputStream(file));
    }


    /**
     * Uploads a single part of a multipart upload.
     *
     * @param id multipart upload id
     * @param partNumber part number to identify relative location in final file
     * @param inputStream stream providing data for part to be uploaded
     * @return multipart single part object
     * @throws IOException thrown if there is a problem connecting to Manta
     */
    @Override
    public MantaMultipartUploadPart uploadPart(final MantaMultipartUpload upload,
                                               final int partNumber,
                                               final InputStream inputStream)
            throws IOException {
        Validate.notNull(upload, "Multipart upload object must not be null");
        final String path = multipartPath(upload.getId(), partNumber);
        final MantaObjectResponse response = mantaClient.put(path, inputStream);

        return new MantaMultipartUploadPart(response);
    }


    /**
     * Retrieves information about a single part of a multipart upload.
     *
     * @param id multipart upload id
     * @param partNumber part number to identify relative location in final file
     * @return multipart single part object
     * @throws IOException thrown if there is a problem connecting to Manta
     */
    @Override
    public MantaMultipartUploadPart getPart(final MantaMultipartUpload upload,
                                            final int partNumber)
            throws IOException {
        Validate.notNull(upload, "Multipart upload object must not be null");
        final String path = multipartPath(upload.getId(), partNumber);
        final MantaObjectResponse response = mantaClient.head(path);

        return new MantaMultipartUploadPart(response);

    }

    /**
     * Retrieves the state of a given Manta multipart upload.
     *
     * @param uploadId multipart upload id
     * @return enum representing the state / status of the multipart upload
     * @throws IOException thrown if there is a problem connecting to Manta
     */
    @Override
    public MantaMultipartStatus getStatus(final MantaMultipartUpload upload)
            throws IOException {
        Validate.notNull(upload, "Multipart upload object must not be null");

        return getStatus(upload, null);
    }

    /**
     * Retrieves the state of a given Manta multipart upload.
     *
     * @param uploadId multipart upload id
     * @param jobId Manta job id used to concatenate multipart parts
     * @return enum representing the state / status of the multipart upload
     * @throws IOException thrown if there is a problem connecting to Manta
     */
    private MantaMultipartStatus getStatus(final MantaMultipartUpload upload,
                                           final UUID jobId) throws IOException {
        Validate.notNull(upload, "Multipart upload id must not be null");

        final String dir = multipartUploadDir(upload.getId());
        final MantaObjectResponse response;
        final MantaJob job;

        try {
            response = mantaClient.head(dir);
        } catch (MantaClientHttpResponseException e) {
            if (e.getStatusCode() == HttpStatus.SC_NOT_FOUND) {
                if (jobId == null) {
                    job = findJob(upload);
                } else {
                    job = mantaClient.getJob(jobId);
                }

                if (job == null) {
                    return MantaMultipartStatus.UNKNOWN;
                } else if (job.getCancelled() != null && job.getCancelled()) {
                    return MantaMultipartStatus.ABORTED;
                } else if (job.getState().equals("done")) {
                    return MantaMultipartStatus.COMPLETED;
                } else if (job.getState().equals("running")) {
                    return MantaMultipartStatus.COMMITTING;
                } else {
                    MantaException mioe = new MantaException("Unexpected job state");
                    mioe.setContextValue("job_state", job.getState());
                    mioe.setContextValue("job_id", job.getId().toString());
                    mioe.setContextValue("multipart_id", upload.getId());
                    mioe.setContextValue("multipart_upload_dir", dir);

                    throw mioe;
                }
            }

            throw e;
        }

        if (!response.isDirectory()) {
            MantaMultipartException e = new MantaMultipartException(
                    "Remote path was a file and not a directory as expected");
            e.setContextValue("multipart_upload_dir", dir);
            throw e;
        }

        if (jobId == null) {
            job = findJob(upload);
        } else {
            job = mantaClient.getJob(jobId);
        }

        if (job == null) {
            return MantaMultipartStatus.CREATED;
        }

            /* If we still have the directory associated with the multipart
             * upload AND we are in the state of Cancelled. */
        if (job.getCancelled()) {
            return MantaMultipartStatus.ABORTING;
        }

        final String state = job.getState();

            /* If we still have the directory associated with the multipart
             * upload AND we have the job id, we are in a state where the
             * job hasn't finished clearing out the data files. */
        if (state.equals("done") || state.equals("running") || state.equals("queued")) {
            return MantaMultipartStatus.COMMITTING;
        } else {
            return MantaMultipartStatus.UNKNOWN;
        }
    }

    /**
     * Lists the parts that have already been uploaded.
     *
     * @param id multipart upload id
     * @return stream of parts identified by integer part number
     * @throws IOException thrown if there is a problem connecting to Manta
     */
    @Override
    public Stream<MantaMultipartUploadPart> listParts(final MantaMultipartUpload upload)
            throws IOException {
        Validate.notNull(upload, "Multipart upload object must not be null");
        final String dir = multipartUploadDir(upload.getId());

        return mantaClient.listObjects(dir)
                .filter(value -> !Paths.get(value.getPath())
                        .getFileName().toString().equals(METADATA_FILE))
                .map(MantaMultipartUploadPart::new);
    }

    /**
     * Validates that there is no part missing from the sequence.
     *
     * @param id multipart upload id
     * @throws IOException thrown if there is a problem connecting to Manta
     * @throws MantaMultipartException thrown went part numbers aren't sequential
     */
    public void validateThatThereAreSequentialPartNumbers(final MantaMultipartUpload upload)
            throws IOException, MantaMultipartException {
        Validate.notNull(upload, "Multipart transaction id must not be null");

        listParts(upload)
            .sorted()
            .map(MantaMultipartUploadPart::getPartNumber)
            .reduce(1, (memo, value) -> {
                if (!memo.equals(value)) {
                    MantaMultipartException e = new MantaMultipartException(
                            "Missing part of multipart upload");
                    e.setContextValue("missing_part", memo);
                    throw e;
                }

                return memo + 1;
            });
    }

    /**
     * Aborts a multipart transfer.
     *
     * @param id multipart upload id
     * @throws IOException thrown if there is a problem connecting to Manta
     */
    @Override
    public void abort(final MantaMultipartUpload upload) throws IOException {
        Validate.notNull(upload, "Multipart upload object must not be null");

        final String dir = multipartUploadDir(upload.getId());

        final MantaJob job = findJob(upload);

        LOGGER.debug("Aborting multipart upload [{}]", upload.getId());

        if (job != null && (job.getState().equals("running")
                || job.getState().equals("queued"))) {
            LOGGER.debug("Aborting multipart upload [{}] backing job [{}]", upload.getId(), job);
            mantaClient.cancelJob(job.getId());
        }

        LOGGER.debug("Deleting multipart upload data from: {}", dir);
        mantaClient.deleteRecursive(dir);
    }

    @Override
    public void complete(final MantaMultipartUpload upload,
                         final Iterable<? extends MantaMultipartUploadTuple> parts)
            throws IOException {
        Validate.notNull(upload, "Multipart upload object must not be null");

        try (Stream<? extends MantaMultipartUploadTuple> stream =
                     StreamSupport.stream(parts.spliterator(), false)) {
            complete(upload, stream, null);
        }
    }

    /**
     * Completes a multipart transfer by assembling the parts on Manta.
     * This is an asynchronous operation and you will need to call
     * {@link #waitForCompletion(MantaMultipartUpload, Duration, int, Function)}
     * to block until the operation completes.
     *
     * @param id multipart upload id
     * @param parts iterable of multipart part objects
     * @throws IOException thrown if there is a problem connecting to Manta
     */
    @Override
    public void complete(final MantaMultipartUpload upload,
                         final Stream<? extends MantaMultipartUploadTuple> partsStream)
            throws IOException {
        Validate.notNull(upload, "Multipart upload object must not be null");

        complete(upload, partsStream, null);
    }

    /**
     * Completes a multipart transfer by assembling the parts on Manta.
     * This is an asynchronous operation and you will need to call
     * {@link #waitForCompletion(MantaMultipartUpload, Duration, int, Function)}
     * to block until the operation completes.
     *
     * @param id multipart upload id
     * @param partsStream stream of multipart part objects
     * @throws IOException thrown if there is a problem connecting to Manta
     */
    public void complete(final MantaMultipartUpload upload,
                         final Stream<? extends MantaMultipartUploadTuple> partsStream,
                         final MantaMetadata extraMetadata)
            throws IOException {
        Validate.notNull(upload, "Upload  must not be null");

        LOGGER.debug("Completing multipart upload [{}]", upload.getId());

        final String uploadDir = multipartUploadDir(upload.getId());
        final MultipartMetadata metadata = downloadMultipartMetadata(upload.getId());

        final Map<String, MantaMultipartUploadPart> listing = new HashMap<>();
        try (Stream<MantaMultipartUploadPart> listStream = listParts(upload)
                .limit(MAX_PARTS)) {
            listStream.forEach(p -> listing.put(p.getEtag(), p));
        }

        final String path = metadata.getPath();

        final StringBuilder jobExecText = new StringBuilder(
                "set -o pipefail; mget -q ");

        List<MantaMultipartUploadTuple> missingTuples = new ArrayList<>();

        final AtomicInteger count = new AtomicInteger(0);
        partsStream.sorted().distinct().forEach(part -> {
            final int i = count.incrementAndGet();

            if (i > MAX_PARTS) {
                String msg = String.format("Too many multipart parts specified [%d]. "
                        + "The maximum number of parts is %d", MAX_PARTS, count.get());
                throw new IllegalArgumentException(msg);
            }

            // Catch and log any gaps in part numbers
            if (i != part.getPartNumber()) {
                missingTuples.add(new MantaMultipartUploadTuple(i, "N/A"));
            } else {
                final MantaMultipartUploadPart o = listing.get(part.getEtag());

                if (o != null) {
                    jobExecText.append(o.getObjectPath()).append(" ");
                } else {
                    missingTuples.add(part);
                }
            }
        });

        // FIXME
        // if (!missingTuples.isEmpty()) {
        //     final MantaMultipartException e = new MantaMultipartException(
        //             "Multipart part(s) specified couldn't be found");

        //     int missingCount = 0;
        //     for (MantaMultipartUploadTuple missingPart : missingTuples) {
        //         String key = String.format("missing_part_%d", ++missingCount);
        //         e.setContextValue(key, missingPart.toString());
        //     }

        //     throw e;
        // }

        final String headerFormat = "\"%s: %s\" ";

        jobExecText.append("| mput ")
                   .append("-H ")
                   .append(String.format(headerFormat, UPLOAD_ID_METADATA_KEY, upload.getId()))
                   .append("-H ")
                   .append(String.format(headerFormat, JOB_ID_METADATA_KEY, "$MANTA_JOB_ID"))
                   .append("-q ");

        if (metadata.getContentType() != null) {
            jobExecText.append("-H 'Content-Type: ")
                       .append(metadata.getContentType())
                       .append("' ");
        }

        MantaMetadata objectMetadata = metadata.getObjectMetadata();
        if (objectMetadata != null && extraMetadata != null) {
            objectMetadata.putAll(extraMetadata);
        } else if (objectMetadata == null && extraMetadata != null) {
            objectMetadata = extraMetadata;
        }

        if (objectMetadata != null) {
            Set<Map.Entry<String, String>> entries = objectMetadata.entrySet();

            for (Map.Entry<String, String> entry : entries) {
                jobExecText.append("-H '")
                           .append(entry.getKey()).append(": ")
                           .append(entry.getValue())
                           .append("' ");
            }
        }
        jobExecText.append(path);

        final MantaJobPhase concatPhase = new MantaJobPhase()
                .setType("reduce")
                .setExec(jobExecText.toString());

        final MantaJobPhase cleanupPhase = new MantaJobPhase()
                .setType("reduce")
                .setExec("mrm -r " + uploadDir);

        MantaJobBuilder.Run run = mantaClient.jobBuilder()
                .newJob(String.format(JOB_NAME_FORMAT, upload.getId()))
                .addPhase(concatPhase)
                .addPhase(cleanupPhase)
                .run();

        // We write the job id to Metadata object so that we can query it easily
        writeJobIdToMetadata(upload.getId(), run.getId());

        if (LOGGER.isDebugEnabled()) {
            LOGGER.debug("Created job for concatenating parts: {}",
                    run.getId());
        }
    }

    /**
     * Downloads the serialized metadata object from Manta and deserializes it.
     *
     * @param id multipart upload id
     * @return metadata object
     * @throws IOException thrown if there is a problem connecting to Manta
     */
    protected JobsMultipartManager.MultipartMetadata downloadMultipartMetadata(final UUID id)
            throws IOException {
        final String uploadDir = multipartUploadDir(id);
        final String metadataPath = uploadDir + METADATA_FILE;

        LOGGER.debug("Reading metadata from: {}", metadataPath);
        try (InputStream in = mantaClient.getAsInputStream(metadataPath)) {
            return MantaObjectMapper.INSTANCE.readValue(in,
                    JobsMultipartManager.MultipartMetadata.class);
        }
    }

    /**
     * Writes the multipart job id to the metadata object's Manta metadata.
     *
     * @param uploadId multipart upload id
     * @param jobId Manta job id used to concatenate multipart parts
     * @throws IOException thrown if there is a problem connecting to Manta
     */
    protected void writeJobIdToMetadata(final UUID uploadId, final UUID jobId)
            throws IOException {
        Validate.notNull(uploadId, "Multipart upload id must not be null");
        Validate.notNull(jobId, "Job id must not be null");

        final String uploadDir = multipartUploadDir(uploadId);
        final String metadataPath = uploadDir + METADATA_FILE;

        LOGGER.debug("Writing job id [{}] to: {}", jobId, metadataPath);

        MantaMetadata metadata = new MantaMetadata();
        metadata.put(JOB_ID_METADATA_KEY, jobId.toString());
        mantaClient.putMetadata(metadataPath, metadata);
    }

    /**
     * Writes the multipart job id to the metadata object's Manta metadata.
     *
     * @param uploadId multipart upload id
     * @throws IOException thrown if there is a problem connecting to Manta
     * @return Manta job id used to concatenate multipart parts
     */
    protected UUID getJobIdFromMetadata(final UUID uploadId)
            throws IOException {
        Validate.notNull(uploadId, "Multipart upload id must not be null");

        final String uploadDir = multipartUploadDir(uploadId);
        final String metadataPath = uploadDir + METADATA_FILE;

        try {
            MantaObjectResponse response = mantaClient.head(metadataPath);
            String uuidAsString = response.getMetadata().get(JOB_ID_METADATA_KEY);

            if (uuidAsString == null) {
                return null;
            }

            return UUID.fromString(uuidAsString);
        } catch (MantaClientHttpResponseException e) {
            if (e.getStatusCode() == HttpStatus.SC_NOT_FOUND) {
                return null;
            }

            throw e;
        }
    }

    /**
     * Waits for a multipart upload to complete. Polling every 5 seconds.
     *
     * @param <R> Return type for executeWhenTimesToPollExceeded
     * @param upload multipart upload object
     * @param executeWhenTimesToPollExceeded lambda executed when timesToPoll has been exceeded
     * @return null when under poll timeout, otherwise returns return value of executeWhenTimesToPollExceeded
     * @throws IOException thrown if there is a problem connecting to Manta
     */
    public <R> R waitForCompletion(final MantaMultipartUpload upload,
                                   final Function<UUID, R> executeWhenTimesToPollExceeded)
            throws IOException {
        Validate.notNull(upload, "Multipart upload object must not be null");

        return waitForCompletion(upload, Duration.ofSeconds(DEFAULT_SECONDS_TO_POLL),
                NUMBER_OF_TIMES_TO_POLL, executeWhenTimesToPollExceeded);

    }

    /**
     * Waits for a multipart upload to complete. Polling for set interval.
     *
     * @param <R> Return type for executeWhenTimesToPollExceeded
     * @param id multipart upload id
     * @param pingInterval interval to poll
     * @param timesToPoll number of times to poll Manta to check for completion
     * @param executeWhenTimesToPollExceeded lambda executed when timesToPoll has been exceeded
     * @return null when under poll timeout, otherwise returns return value of executeWhenTimesToPollExceeded
     * @throws IOException thrown if there is a problem connecting to Manta
     */
    public <R> R waitForCompletion(final MantaMultipartUpload upload, final Duration pingInterval,
                                   final int timesToPoll,
                                   final Function<UUID, R> executeWhenTimesToPollExceeded)
            throws IOException {
        if (timesToPoll <= 0) {
            String msg = String.format("times to poll should be set to a value greater than 1. "
                    + "Actual value: %d", timesToPoll);
            throw new IllegalArgumentException(msg);
        }

        final String dir = multipartUploadDir(upload.getId());
        final MantaJob job = findJob(upload);

        if (job == null) {
            String msg = "Unable for find job associated with multipart upload. "
                    + "Was complete() run for upload or was it run so long ago "
                    + "that we no longer have a record for it?";
            MantaMultipartException e = new MantaMultipartException(msg);
            e.setContextValue("upload_id", upload.getId().toString());
            e.setContextValue("upload_directory", dir);
            e.setContextValue("job_id", job.getId().toString());

            throw e;
        }

        final long waitMillis = pingInterval.toMillis();

        int timesPolled;

        /* We ping the upload directory and wait for it to be deleted because
         * there is the chance for a race condition when the job attempts to
         * delete the upload directory, but isn't finished. */
        for (timesPolled = 0; timesPolled < timesToPoll; timesPolled++) {
            try {
                final MantaMultipartStatus status = getStatus(upload, job.getId());

                // We do a check preemptively because we shouldn't sleep unless we need to
                if (status.equals(MantaMultipartStatus.COMPLETED)) {
                    return null;
                }

                if (status.equals(MantaMultipartStatus.ABORTED)) {
                    String msg = "Manta job backing multipart upload was aborted. "
                            + "This upload was unable to be completed.";
                    MantaMultipartException e = new MantaMultipartException(msg);
                    e.setContextValue("upload_id", upload.getId().toString());
                    e.setContextValue("upload_directory", dir);
                    e.setContextValue("job_id", job.getId().toString());

                    throw e;
                }

                if (status.equals(MantaMultipartStatus.UNKNOWN)) {
                    String msg = "Manta job backing multipart upload was is in "
                            + "a unknown state. Typically this means that we "
                            + "are unable to get the status of the job backing "
                            + "the multipart upload.";
                    MantaMultipartException e = new MantaMultipartException(msg);
                    e.setContextValue("upload_id", upload.getId().toString());
                    e.setContextValue("upload_directory", dir);
                    e.setContextValue("job_id", job.getId().toString());

                    throw e;
                }

                // Don't bother to sleep if we won't be doing a check
                if (timesPolled < timesToPoll + 1) {
                    if (LOGGER.isDebugEnabled()) {
                        LOGGER.debug("Waiting for [{}] ms for upload [{}] to complete "
                                     + "(try {} of {})", waitMillis, upload.getId(), timesPolled + 1,
                                timesToPoll);
                    }

                    Thread.sleep(waitMillis);
                }
            } catch (InterruptedException e) {
                /* We assume the client has written logic for when the polling operation
                 * doesn't complete within the time period as expected and we also make
                 * the assumption that that behavior would be acceptable when the thread
                 * has been interrupted. */
                return executeWhenTimesToPollExceeded.apply(upload.getId());
            }
        }

        if (timesPolled >= timesToPoll) {
            return executeWhenTimesToPollExceeded.apply(upload.getId());
        }

        return null;
    }

    /**
     * Builds the full remote path for a part of a multipart upload.
     *
     * @param id multipart upload id
     * @param partNumber part number to identify relative location in final file
     * @return temporary path on Manta to store part
     */
    String multipartPath(final UUID id, final int partNumber) {
        validatePartNumber(partNumber);
        final String dir = multipartUploadDir(id);
        return String.format("%s%d", dir, partNumber);
    }

    /**
     * Finds the directory in which to upload parts into.
     *
     * @param id multipart transaction id
     * @return temporary Manta directory in which to upload parts
     */
    String multipartUploadDir(final UUID id) {
        Validate.notNull(id, "Multipart transaction id must not be null");

        return this.resolvedMultipartUploadDirectory
                + SEPARATOR + id.toString() + SEPARATOR;
    }

    /**
     * Validates that the given part number is specified correctly.
     *
     * @param partNumber integer part number value
     * @throws IllegalArgumentException if partNumber is less than 1 or greater than MULTIPART_DIRECTORY
     */
    static void validatePartNumber(final int partNumber) {
        if (partNumber <= 0) {
            throw new IllegalArgumentException("Negative or zero part numbers are not valid");
        }

        if (partNumber > MAX_PARTS) {
            final String msg = String.format("Part number of [%d] exceeds maximum parts (%d)",
                    partNumber, MAX_PARTS);
            throw new IllegalArgumentException(msg);
        }
    }

    /**
     * Returns the Manta job used to concatenate multiple file parts.
     *
     * @param id multipart upload id
     * @return Manta job object or null if not found
     * @throws IOException thrown if there is a problem connecting to Manta
     */
    MantaJob findJob(final MantaMultipartUpload upload) throws IOException {
        final UUID jobId = getJobIdFromMetadata(upload.getId());

        if (jobId == null) {
            LOGGER.debug("Unable to get job id from metadata directory. Now trying job listing.");
            try (Stream<MantaJob> jobs = mantaClient.getJobsByName(String.format(JOB_NAME_FORMAT, upload.getId()))) {
                return jobs.findFirst().orElse(null);
            }
        }

        return mantaClient.getJob(jobId);
    }

    /**
     * Inner class used only with the jobs-based multipart implementation for
     * storing header and metadata information.
     */
    @JsonInclude
    static class MultipartMetadata implements Serializable {
        private static final long serialVersionUID = -4410867990710890357L;

        /**
         * Path to final object on Manta.
         */
        @JsonProperty
        private String path;

        /**
         * Metadata of final object.
         */
        @JsonProperty("object_metadata")
        private HashMap<String, String> objectMetadata;

        /**
         * HTTP content type to write to the final object.
         */
        @JsonProperty("content_type")
        private String contentType;

        /**
         * Creates a new instance.
         */
        MultipartMetadata() {
        }

        String getPath() {
            return path;
        }

        /**
         * Sets the path to the final object on Manta.
         *
         * @param path remote Manta path
         * @return this instance
         */
        MultipartMetadata setPath(final String path) {
            this.path = path;
            return this;
        }

        /**
         * Gets the metadata associated with the final Manta object.
         *
         * @return new instance of {@link MantaMetadata} with data populated
         */
        MantaMetadata getObjectMetadata() {
            if (this.objectMetadata == null) {
                return null;
            }

            return new MantaMetadata(this.objectMetadata);
        }

        /**
         * Sets the metadata to be written to the final object on Manta.
         *
         * @param objectMetadata metadata to write
         * @return this instance
         */
        MultipartMetadata setObjectMetadata(final MantaMetadata objectMetadata) {
            if (objectMetadata != null) {
                this.objectMetadata = new HashMap<>(objectMetadata);
            } else {
                this.objectMetadata = null;
            }

            return this;
        }

        String getContentType() {
            return contentType;
        }

        /**
         * Sets http headers to write to the final object on Manta. Actually,
         * we only consume Content-Type for now.
         *
         * @param contentType HTTP content type to set for the object
         * @return this instance
         */
        @SuppressWarnings("UnusedReturnValue")
        MultipartMetadata setContentType(final String contentType) {
            this.contentType = contentType;
            return this;
        }
    }
}