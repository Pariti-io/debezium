/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.server.pubsub;

import java.io.IOException;
import java.util.*;
import java.util.concurrent.ExecutionException;

import javax.annotation.PostConstruct;
import javax.annotation.PreDestroy;
import javax.enterprise.context.Dependent;
import javax.enterprise.inject.Instance;
import javax.inject.Inject;
import javax.inject.Named;

import com.google.cloud.pubsub.v1.TopicAdminClient;
import com.google.cloud.pubsub.v1.TopicAdminSettings;
import com.google.pubsub.v1.Topic;
import io.grpc.Status;
import org.eclipse.microprofile.config.Config;
import org.eclipse.microprofile.config.ConfigProvider;
import org.eclipse.microprofile.config.inject.ConfigProperty;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.threeten.bp.Duration;

import com.google.api.core.ApiFuture;
import com.google.api.core.ApiFutures;
import com.google.api.gax.batching.BatchingSettings;
import com.google.api.gax.batching.FlowControlSettings;
import com.google.api.gax.batching.FlowController;
import com.google.api.gax.core.CredentialsProvider;
import com.google.api.gax.core.NoCredentialsProvider;
import com.google.api.gax.grpc.GrpcTransportChannel;
import com.google.api.gax.retrying.RetrySettings;
import com.google.api.gax.rpc.FixedTransportChannelProvider;
import com.google.api.gax.rpc.TransportChannelProvider;
import com.google.cloud.ServiceOptions;
import com.google.cloud.pubsub.v1.Publisher;
import com.google.protobuf.ByteString;
import com.google.pubsub.v1.ProjectTopicName;
import com.google.pubsub.v1.PubsubMessage;

import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;

import io.debezium.DebeziumException;
import io.debezium.engine.ChangeEvent;
import io.debezium.engine.DebeziumEngine;
import io.debezium.engine.DebeziumEngine.RecordCommitter;
import io.debezium.server.BaseChangeConsumer;
import io.debezium.server.CustomConsumerBuilder;

/**
 * Implementation of the consumer that delivers the messages into Google Pub/Sub destination.
 *
 * @author Jiri Pechanec
 */
@Named("pubsub")
@Dependent
public class PubSubChangeConsumer extends BaseChangeConsumer implements DebeziumEngine.ChangeConsumer<ChangeEvent<Object, Object>> {

    private static final Logger LOGGER = LoggerFactory.getLogger(PubSubChangeConsumer.class);

    private static final String PROP_PREFIX = "debezium.sink.pubsub.";
    private static final String PROP_PROJECT_ID = PROP_PREFIX + "project.id";
    private static final String EMULATOR_HOSTPORT_ID = PROP_PREFIX + "emulator.hostport";
    private static final String AUTO_CREATE_TOPIC_ID = PROP_PREFIX + "topics.autocreate";

    public interface PublisherBuilder {
        Publisher get(ProjectTopicName topicName);
    }

    private String projectId;

    private final Map<String, Publisher> publishers = new HashMap<>();
    private ManagedChannel channel;
    private TopicAdminClient topicClient;
    private PublisherBuilder publisherBuilder;

    @ConfigProperty(name = PROP_PREFIX + "ordering.enabled", defaultValue = "true")
    boolean orderingEnabled;

    @ConfigProperty(name = PROP_PREFIX + "null.key", defaultValue = "default")
    String nullKey;

    @ConfigProperty(name = PROP_PREFIX + "batch.delay.threshold.ms", defaultValue = "100")
    Integer maxDelayThresholdMs;

    @ConfigProperty(name = PROP_PREFIX + "batch.element.count.threshold", defaultValue = "100")
    Long maxBufferSize;

    @ConfigProperty(name = PROP_PREFIX + "batch.request.byte.threshold", defaultValue = "10000000")
    Long maxBufferBytes;

    @ConfigProperty(name = PROP_PREFIX + "flowcontrol.enabled", defaultValue = "false")
    boolean flowControlEnabled;

    @ConfigProperty(name = PROP_PREFIX + "flowcontrol.max.outstanding.messages", defaultValue = "9223372036854775807")
    Long maxOutstandingMessages;

    @ConfigProperty(name = PROP_PREFIX + "flowcontrol.max.outstanding.bytes", defaultValue = "9223372036854775807")
    Long maxOutstandingRequestBytes;

    @ConfigProperty(name = PROP_PREFIX + "retry.total.timeout.ms", defaultValue = "60000")
    Integer maxTotalTimeoutMs;

    @ConfigProperty(name = PROP_PREFIX + "retry.max.rpc.timeout.ms", defaultValue = "10000")
    Integer maxRequestTimeoutMs;

    @ConfigProperty(name = PROP_PREFIX + "retry.initial.delay.ms", defaultValue = "5")
    Integer initialRetryDelay;

    @ConfigProperty(name = PROP_PREFIX + "retry.delay.multiplier", defaultValue = "2.0")
    Double retryDelayMultiplier;

    @ConfigProperty(name = PROP_PREFIX + "retry.max.delay.ms", defaultValue = "9223372036854775807")
    Long maxRetryDelay;

    @ConfigProperty(name = PROP_PREFIX + "retry.initial.rpc.timeout.ms", defaultValue = "10000")
    Integer initialRpcTimeout;

    @ConfigProperty(name = PROP_PREFIX + "retry.rpc.timeout.multiplier", defaultValue = "2.0")
    Double rpcTimeoutMultiplier;

    @Inject
    @CustomConsumerBuilder
    Instance<PublisherBuilder> customPublisherBuilder;

    @PostConstruct
    void connect() {
        final Config config = ConfigProvider.getConfig();
        projectId = config.getOptionalValue(PROP_PROJECT_ID, String.class).orElse(ServiceOptions.getDefaultProjectId());
        final Optional<String> emulatorHostPort = config.getOptionalValue(EMULATOR_HOSTPORT_ID, String.class);

        if (customPublisherBuilder.isResolvable()) {
            publisherBuilder = customPublisherBuilder.get();
            LOGGER.info("Obtained custom configured PublisherBuilder '{}'", customPublisherBuilder);
            return;
        }

        if (emulatorHostPort.isPresent()) {
            LOGGER.info("Using settings for pub/sub emulator at '{}'", emulatorHostPort.get());
            channel = ManagedChannelBuilder.forTarget(emulatorHostPort.get()).usePlaintext().build();
        }

        final boolean autoCreateTopic = config.getOptionalValue(AUTO_CREATE_TOPIC_ID, Boolean.class).orElse(false);
        if (autoCreateTopic) {
            LOGGER.info("Enabling automatic creation of topics");
            TopicAdminSettings.Builder adminSettingsBuilder = TopicAdminSettings.newBuilder();

            if (emulatorHostPort.isPresent()) {
                TransportChannelProvider channelProvider =
                        FixedTransportChannelProvider.create(GrpcTransportChannel.create(channel));
                CredentialsProvider credentialsProvider = NoCredentialsProvider.create();
                adminSettingsBuilder
                        .setTransportChannelProvider(channelProvider)
                        .setCredentialsProvider(credentialsProvider);
            }

            try {
                topicClient = TopicAdminClient.create(adminSettingsBuilder.build());
            } catch (IOException e) {
                throw new DebeziumException("Error creating topic admin client", e);
            }
        }

        BatchingSettings.Builder batchingSettings = BatchingSettings.newBuilder()
                .setDelayThreshold(Duration.ofMillis(maxDelayThresholdMs))
                .setElementCountThreshold(maxBufferSize)
                .setRequestByteThreshold(maxBufferBytes);

        if (flowControlEnabled) {
            batchingSettings.setFlowControlSettings(FlowControlSettings.newBuilder()
                    .setMaxOutstandingRequestBytes(maxOutstandingRequestBytes)
                    .setMaxOutstandingElementCount(maxOutstandingMessages)
                    .setLimitExceededBehavior(FlowController.LimitExceededBehavior.Block)
                    .build());
        }


        publisherBuilder = (t) -> {
            LOGGER.info("Creating publisher for {}", t.toString());
            try {
                if (autoCreateTopic) {
                    try {
                        topicClient.getTopic(t);
                    } catch (com.google.api.gax.rpc.NotFoundException ex) {
                        // TODO: Is this the only / nicest way to check for existence!?
                        LOGGER.info("Topic {} does not exist, will create it now", t);
                        topicClient.createTopic(t);
                    }
                }

                Publisher.Builder builder = Publisher.newBuilder(t)
                        .setEnableMessageOrdering(orderingEnabled)
                        .setBatchingSettings(batchingSettings.build())
                        .setRetrySettings(
                                RetrySettings.newBuilder()
                                        .setTotalTimeout(Duration.ofMillis(maxTotalTimeoutMs))
                                        .setMaxRpcTimeout(Duration.ofMillis(maxRequestTimeoutMs))
                                        .setInitialRetryDelay(Duration.ofMillis(initialRetryDelay))
                                        .setRetryDelayMultiplier(retryDelayMultiplier)
                                        .setMaxRetryDelay(Duration.ofMillis(maxRetryDelay))
                                        .setInitialRpcTimeout(Duration.ofMillis(initialRpcTimeout))
                                        .setRpcTimeoutMultiplier(rpcTimeoutMultiplier)
                                        .build());

                // We're using a custom channel (for local emulator), do some more
                if (channel != null) {
                    TransportChannelProvider channelProvider = FixedTransportChannelProvider.create(GrpcTransportChannel.create(channel));
                    CredentialsProvider credentialsProvider = NoCredentialsProvider.create();
                    builder
                            .setChannelProvider(channelProvider)
                            .setCredentialsProvider(credentialsProvider);
                }

                return builder.build();
            } catch (IOException e) {
                throw new DebeziumException(e);
            }
        };

        LOGGER.info("Using default PublisherBuilder '{}'", publisherBuilder);
    }

    @PreDestroy
    void close() {
        publishers.values().forEach(publisher -> {
            try {
                publisher.shutdown();
            } catch (Exception e) {
                LOGGER.warn("Exception while closing publisher: {}", e);
            }
        });

        if (channel != null) {
            channel.shutdown();
            channel = null;
        }
    }

    @Override
    public void handleBatch(List<ChangeEvent<Object, Object>> records, RecordCommitter<ChangeEvent<Object, Object>> committer)
            throws InterruptedException {
        final List<ApiFuture<String>> deliveries = new ArrayList<>();
        for (ChangeEvent<Object, Object> record : records) {
            LOGGER.trace("Received event '{}'", record);
            final String topicName = streamNameMapper.map(record.destination());
            Publisher publisher = publishers.computeIfAbsent(topicName, (x) -> publisherBuilder.get(ProjectTopicName.of(projectId, x)));

            final PubsubMessage.Builder pubsubMessage = PubsubMessage.newBuilder();

            if (orderingEnabled) {
                if (record.key() == null) {
                    pubsubMessage.setOrderingKey(nullKey);
                } else if (record.key() instanceof String) {
                    pubsubMessage.setOrderingKey((String) record.key());
                } else if (record.key() instanceof byte[]) {
                    pubsubMessage.setOrderingKeyBytes(ByteString.copyFrom((byte[]) record.key()));
                }
            }

            if (record.value() instanceof String) {
                pubsubMessage.setData(ByteString.copyFromUtf8((String) record.value()));
            } else if (record.value() instanceof byte[]) {
                pubsubMessage.setData(ByteString.copyFrom((byte[]) record.value()));
            }

            deliveries.add(publisher.publish(pubsubMessage.build()));
            committer.markProcessed(record);
        }
        List<String> messageIds;
        try {
            messageIds = ApiFutures.allAsList(deliveries).get();
        } catch (ExecutionException e) {
            throw new DebeziumException(e);
        }
        LOGGER.trace("Sent messages with ids: {}", messageIds);
        committer.markBatchFinished();
    }

    @Override
    public boolean supportsTombstoneEvents() {
        return false;
    }
}
