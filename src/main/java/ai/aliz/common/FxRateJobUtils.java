/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package ai.aliz.common;

import com.google.api.client.googleapis.json.GoogleJsonResponseException;
import com.google.api.client.googleapis.services.AbstractGoogleClientRequest;
import com.google.api.client.http.HttpRequestInitializer;
import com.google.api.services.bigquery.Bigquery;
import com.google.api.services.bigquery.Bigquery.Datasets;
import com.google.api.services.bigquery.Bigquery.Tables;
import com.google.api.services.bigquery.model.*;
import com.google.api.services.pubsub.Pubsub;
import com.google.auth.Credentials;
import com.google.auth.http.HttpCredentialsAdapter;
import com.google.cloud.hadoop.util.ChainingHttpRequestInitializer;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import com.google.common.util.concurrent.Uninterruptibles;
import org.apache.beam.sdk.PipelineResult;
import org.apache.beam.sdk.extensions.gcp.auth.NullCredentialInitializer;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryOptions;
import org.apache.beam.sdk.io.gcp.pubsub.PubsubOptions;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.util.*;
import org.joda.time.Duration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Collection;
import java.util.List;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import static ai.aliz.common.FxRateConstants.SC_NOT_FOUND;

/**
 * The utility class that sets up and tears down external resources,
 * and cancels the streaming pipelines once the program terminates.
 *
 * <p>It is used to run Beam examples.
 */
public class FxRateJobUtils {

    private static final Logger LOG = LoggerFactory.getLogger(FxRateJobUtils.class);

    /**
     * \p{L} denotes the category of Unicode letters,
     * so this pattern will match on everything that is not a letter.
     *
     * <p>It is used for tokenizing strings in the wordcount examples.
     */

    private final PipelineOptions options;
    private Bigquery bigQueryClient = null;
    private Pubsub pubsubClient = null;
    private Set<PipelineResult> pipelinesToCancel = Sets.newHashSet();
    private List<String> pendingMessages = Lists.newArrayList();

    /**
     * Do resources and runner options setup.
     */
    public FxRateJobUtils(final PipelineOptions options) {
        this.options = options;
    }

    /**
     * Sets up external resources that are required by the example,
     * such as Pub/Sub topics and BigQuery tables.
     *
     * @throws IOException if there is a problem setting up the resources
     */
    public void setup() throws IOException {
        final Sleeper sleeper = Sleeper.DEFAULT;
        final BackOff backOff =
                FluentBackoff.DEFAULT
                        .withMaxRetries(3).withInitialBackoff(Duration.millis(200)).backoff();
        Throwable lastException = null;
        try {
            while (true) {
                try {
                    setupBigQueryTable();
                    return;
                } catch (GoogleJsonResponseException e) {
                    lastException = e;
                }
                if (!BackOffUtils.next(sleeper, backOff))
                    break;
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            // Ignore InterruptedException

        }
        throw new RuntimeException(lastException);
    }


    /**
     * Sets up the BigQuery table with the given schema.
     *
     * <p>If the table already exists, the schema has to match the given one. Otherwise, the example
     * will throw a RuntimeException. If the table doesn't exist, a new table with the given schema
     * will be created.
     *
     * @throws IOException if there is a problem setting up the BigQuery table
     */
    public void setupBigQueryTable() throws IOException {
        final FxRateBigQueryTableOptions bigQueryTableOptions = options.as(FxRateBigQueryTableOptions.class);
        if (bigQueryTableOptions.getBigQueryDataset() != null && bigQueryTableOptions.getBigQueryTable() != null && bigQueryTableOptions.getBigQuerySchema() != null) {

            pendingMessages.add("Setting Up BigQuery Table");

            setupBigQueryTable(bigQueryTableOptions.getProject(),
                    bigQueryTableOptions.getBigQueryDataset(),
                    bigQueryTableOptions.getBigQueryTable(),
                    bigQueryTableOptions.getBigQuerySchema());

            pendingMessages.add(String.format("Table Set For: %s : %s.%s",
                    bigQueryTableOptions.getProject(), bigQueryTableOptions.getBigQueryDataset(), bigQueryTableOptions.getBigQueryTable()));
        }
    }

    /**
     * Tears down external resources that can be deleted upon the example's completion.
     */
    private void tearDown() {

        pendingMessages.add("Tearing Down");

        final FxPubSubTopicAndSubscriptionOptions fxPubSubTopicAndSubscriptionOptions = options.as(FxPubSubTopicAndSubscriptionOptions.class);

        if (!fxPubSubTopicAndSubscriptionOptions.getPubsubTopic().isEmpty()) {
            try {
                deletePubSubTopic(fxPubSubTopicAndSubscriptionOptions.getPubsubTopic());
                pendingMessages.add(String.format("The Pub/Sub topic has been deleted: %s", fxPubSubTopicAndSubscriptionOptions.getPubsubTopic()));
            } catch (IOException e) {
                pendingMessages.add(String.format("Failed to delete the Pub/Sub topic : %s", fxPubSubTopicAndSubscriptionOptions.getPubsubTopic()));
            }
            if (!fxPubSubTopicAndSubscriptionOptions.getPubsubSubscription().isEmpty()) {
                try {
                    deletePubSubSubscription(fxPubSubTopicAndSubscriptionOptions.getPubsubSubscription());
                    pendingMessages.add(String.format("The Pub/Sub subscription has been deleted: %s", fxPubSubTopicAndSubscriptionOptions.getPubsubSubscription()));
                } catch (IOException e) {
                    pendingMessages.add(String.format("Failed to delete the Pub/Sub subscription : %s", fxPubSubTopicAndSubscriptionOptions.getPubsubSubscription()));
                }
            }
        }

        final FxRateBigQueryTableOptions bigQueryTableOptions = options.as(FxRateBigQueryTableOptions.class);
        if (bigQueryTableOptions.getBigQueryDataset() != null && bigQueryTableOptions.getBigQueryTable() != null && bigQueryTableOptions.getBigQuerySchema() != null) {
            pendingMessages.add(String.format("The BigQuery table might contain the example's output, and it is not deleted automatically: %s : %s.%s", bigQueryTableOptions.getProject(), bigQueryTableOptions.getBigQueryDataset(), bigQueryTableOptions.getBigQueryTable()));
            pendingMessages.add("Please go to the Developers Console to delete it manually. Otherwise, you may be charged for its usage.");
        }

    }

    /**
     * Returns a BigQuery client builder using the specified {@link BigQueryOptions}.
     */
    private Bigquery.Builder newBigQueryClient(BigQueryOptions options) {
        return new Bigquery.Builder(Transport.getTransport(), Transport.getJsonFactory(),
                chainHttpRequestInitializer(options.getGcpCredential(), new RetryHttpRequestInitializer(ImmutableList.of(SC_NOT_FOUND))))
                .setApplicationName(options.getAppName())
                .setGoogleClientRequestInitializer(options.getGoogleApiTrace());
    }

    /**
     * Returns a Pubsub client builder using the specified {@link PubsubOptions}.
     */
    private Pubsub.Builder newPubsubClient(final PubsubOptions options) {
        return new Pubsub.Builder(
                Transport.getTransport(),
                Transport.getJsonFactory(),
                chainHttpRequestInitializer(options.getGcpCredential(), new RetryHttpRequestInitializer(ImmutableList.of(SC_NOT_FOUND))))
                .setRootUrl(options.getPubsubRootUrl()).setApplicationName(options.getAppName()).setGoogleClientRequestInitializer(options.getGoogleApiTrace());
    }

    /**
     * @param credential
     * @param httpRequestInitializer
     * @return
     */
    private HttpRequestInitializer chainHttpRequestInitializer(final Credentials credential, final HttpRequestInitializer httpRequestInitializer) {
        if (credential == null) {
            return new ChainingHttpRequestInitializer(new NullCredentialInitializer(), httpRequestInitializer);
        } else {
            return new ChainingHttpRequestInitializer(new HttpCredentialsAdapter(credential), httpRequestInitializer);
        }
    }

    /**
     * @param projectId
     * @param datasetId
     * @param tableId
     * @param schema
     * @throws IOException
     */
    private void setupBigQueryTable(final String projectId, final String datasetId, final String tableId, final TableSchema schema) throws IOException {

        if (bigQueryClient == null) {
            bigQueryClient = newBigQueryClient(options.as(BigQueryOptions.class)).build();
        }

        final Datasets datasetService = bigQueryClient.datasets();

        if (executeNullIfNotFound(datasetService.get(projectId, datasetId)) == null) {
            final Dataset newDataset = new Dataset().setDatasetReference(new DatasetReference().setProjectId(projectId).setDatasetId(datasetId));
            datasetService.insert(projectId, newDataset).execute();
        }

        final Tables tableService = bigQueryClient.tables();
        final Table table = executeNullIfNotFound(tableService.get(projectId, datasetId, tableId));

        if (table == null) {
            final Table newTable = new Table().setSchema(schema).setTableReference(new TableReference().setProjectId(projectId).setDatasetId(datasetId).setTableId(tableId));
            tableService.insert(projectId, datasetId, newTable).execute();
        } else if (!table.getSchema().equals(schema)) {
            throw new RuntimeException(String.format("Table exists and schemas do not match, expecting: %s, actual: %s", schema.toPrettyString(), table.getSchema().toPrettyString()));
        }
    }

    /**
     * Deletes the Google Cloud Pub/Sub topic.
     *
     * @throws IOException if there is a problem deleting the Pub/Sub topic
     */
    private void deletePubSubTopic(final String topic) throws IOException {
        if (pubsubClient == null) {
            pubsubClient = newPubsubClient(options.as(PubsubOptions.class)).build();
        }
        if (executeNullIfNotFound(pubsubClient.projects().topics().get(topic)) != null) {
            pubsubClient.projects().topics().delete(topic).execute();
        }
    }

    /**
     * Deletes the Google Cloud Pub/Sub subscription.
     *
     * @throws IOException if there is a problem deleting the Pub/Sub subscription
     */
    private void deletePubSubSubscription(final String subscription) throws IOException {
        if (pubsubClient == null) {
            pubsubClient = newPubsubClient(options.as(PubsubOptions.class)).build();
        }
        if (executeNullIfNotFound(pubsubClient.projects().subscriptions().get(subscription)) != null) {
            pubsubClient.projects().subscriptions().delete(subscription).execute();
        }
    }

    /**
     * Waits for the pipeline to finish and cancels it before the program exists.
     */
    public void waitToFinish(final PipelineResult result) {
        pipelinesToCancel.add(result);
        if (!options.as(FxRateOptions.class).getKeepJobsRunning()) {
            addShutdownHook(pipelinesToCancel);
        }

        try {
            result.waitUntilFinish();
        } catch (UnsupportedOperationException e) {
            tearDown();
            printPendingMessages();
        } catch (Exception e) {
            throw new RuntimeException("Failed to wait the pipeline until finish: " + result);
        }
    }

    /**
     * @param pipelineResults
     */
    private void addShutdownHook(final Collection<PipelineResult> pipelineResults) {
        Runtime.getRuntime()
                .addShutdownHook(
                        new Thread(
                                () -> {
                                    tearDown();
                                    printPendingMessages();
                                    for (PipelineResult pipelineResult : pipelineResults) {
                                        try {
                                            pipelineResult.cancel();
                                        } catch (IOException e) {
                                            LOG.info("Failed to cancel the job.");
                                            LOG.info(e.getMessage());
                                        }
                                    }

                                    for (PipelineResult pipelineResult : pipelineResults) {
                                        boolean cancellationVerified = false;
                                        for (int retryAttempts = 6; retryAttempts > 0; retryAttempts--) {
                                            if (pipelineResult.getState().isTerminal()) {
                                                cancellationVerified = true;
                                                break;
                                            } else {
                                                LOG.info(
                                                        "The example pipeline is still running. Verifying the cancellation.");
                                            }
                                            Uninterruptibles.sleepUninterruptibly(10, TimeUnit.SECONDS);
                                        }
                                        if (!cancellationVerified) {
                                            LOG.info(
                                                    "Failed to verify the cancellation for job: " + pipelineResult);
                                        }
                                    }
                                }));
    }

    /**
     *
     */
    protected void printPendingMessages() {
        LOG.info("--------------------------------------------");
        for (String message : pendingMessages) {
            System.out.println(message);
        }
        LOG.info("--------------------------------------------");
    }

    /**
     * @param request
     * @param <T>
     * @return
     * @throws IOException
     */
    protected <T> T executeNullIfNotFound(final AbstractGoogleClientRequest<T> request) throws IOException {
        try {
            return request.execute();
        } catch (GoogleJsonResponseException e) {
            if (e.getStatusCode() == SC_NOT_FOUND) {
                return null;
            } else {
                throw e;
            }
        }
    }

}
