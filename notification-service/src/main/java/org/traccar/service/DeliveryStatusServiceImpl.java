/*
 * Copyright 2023 Anton Tananaev (anton@traccar.org)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.traccar.service;

import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Timer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.data.domain.PageRequest;
import org.springframework.data.domain.Sort;
import org.springframework.data.mongodb.core.MongoTemplate;
import org.springframework.data.mongodb.core.aggregation.Aggregation;
import org.springframework.data.mongodb.core.aggregation.AggregationResults;
import org.springframework.data.mongodb.core.aggregation.ConditionalOperators;
import org.springframework.data.mongodb.core.query.Criteria;
import org.springframework.data.mongodb.core.query.Query;
import org.springframework.stereotype.Service;
import org.traccar.messaging.MessageProducer;
import org.traccar.model.DeliveryStatus;

import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import static org.springframework.data.mongodb.core.aggregation.Aggregation.*;

/**
 * Implementation of the DeliveryStatusService interface, providing comprehensive tracking
 * of notification delivery status across all channels. This implementation records delivery
 * attempts, successes, and failures, and publishes status updates to the message broker.
 */
@Service
public class DeliveryStatusServiceImpl implements DeliveryStatusService {

    private static final Logger LOGGER = LoggerFactory.getLogger(DeliveryStatusServiceImpl.class);

    private final MongoTemplate mongoTemplate;
    private final MessageProducer messageProducer;
    private final MeterRegistry meterRegistry;

    @Value("${notification.status.topic:notification.status}")
    private String statusTopic;

    @Value("${notification.collection.deliveryStatus:delivery_status}")
    private String collectionName;

    /**
     * Constructs a new DeliveryStatusServiceImpl with required dependencies
     *
     * @param mongoTemplate MongoDB template for data access
     * @param messageProducer Message producer for publishing status updates
     * @param meterRegistry Metrics registry for collecting delivery statistics
     */
    @Autowired
    public DeliveryStatusServiceImpl(
            MongoTemplate mongoTemplate,
            MessageProducer messageProducer,
            MeterRegistry meterRegistry) {
        this.mongoTemplate = mongoTemplate;
        this.messageProducer = messageProducer;
        this.meterRegistry = meterRegistry;

        // Initialize metrics
        initializeMetrics();
    }

    private Map<String, Counter> deliveryAttemptCounters = new HashMap<>();
    private Map<String, Counter> deliverySuccessCounters = new HashMap<>();
    private Map<String, Counter> deliveryFailureCounters = new HashMap<>();
    private Map<String, Timer> deliveryDurationTimers = new HashMap<>();

    /**
     * Initializes metrics for tracking delivery statistics
     */
    private void initializeMetrics() {
        // Common channels to pre-initialize metrics for
        String[] channels = {"email", "sms", "push", "web", "telegram", "pushover", "command"};

        for (String channel : channels) {
            deliveryAttemptCounters.put(channel, Counter.builder("notification.delivery.attempts")
                    .tag("channel", channel)
                    .description("Number of notification delivery attempts")
                    .register(meterRegistry));

            deliverySuccessCounters.put(channel, Counter.builder("notification.delivery.success")
                    .tag("channel", channel)
                    .description("Number of successful notification deliveries")
                    .register(meterRegistry));

            deliveryFailureCounters.put(channel, Counter.builder("notification.delivery.failure")
                    .tag("channel", channel)
                    .description("Number of failed notification deliveries")
                    .register(meterRegistry));

            deliveryDurationTimers.put(channel, Timer.builder("notification.delivery.duration")
                    .tag("channel", channel)
                    .description("Duration of notification delivery")
                    .register(meterRegistry));
        }
    }

    /**
     * Gets or creates a counter for delivery attempts for a specific channel
     *
     * @param channel The delivery channel
     * @return The counter for the specified channel
     */
    private Counter getAttemptCounter(String channel) {
        return deliveryAttemptCounters.computeIfAbsent(channel, c -> Counter.builder("notification.delivery.attempts")
                .tag("channel", c)
                .description("Number of notification delivery attempts")
                .register(meterRegistry));
    }

    /**
     * Gets or creates a counter for delivery successes for a specific channel
     *
     * @param channel The delivery channel
     * @return The counter for the specified channel
     */
    private Counter getSuccessCounter(String channel) {
        return deliverySuccessCounters.computeIfAbsent(channel, c -> Counter.builder("notification.delivery.success")
                .tag("channel", c)
                .description("Number of successful notification deliveries")
                .register(meterRegistry));
    }

    /**
     * Gets or creates a counter for delivery failures for a specific channel
     *
     * @param channel The delivery channel
     * @return The counter for the specified channel
     */
    private Counter getFailureCounter(String channel) {
        return deliveryFailureCounters.computeIfAbsent(channel, c -> Counter.builder("notification.delivery.failure")
                .tag("channel", c)
                .description("Number of failed notification deliveries")
                .register(meterRegistry));
    }

    /**
     * Gets or creates a timer for delivery duration for a specific channel
     *
     * @param channel The delivery channel
     * @return The timer for the specified channel
     */
    private Timer getDeliveryTimer(String channel) {
        return deliveryDurationTimers.computeIfAbsent(channel, c -> Timer.builder("notification.delivery.duration")
                .tag("channel", c)
                .description("Duration of notification delivery")
                .register(meterRegistry));
    }

    @Override
    public DeliveryStatus createStatus(long notificationId, String channel, String recipient) {
        DeliveryStatus status = new DeliveryStatus(notificationId, channel, recipient);
        status.setId(UUID.randomUUID().toString());
        status.setCorrelationId(UUID.randomUUID().toString());

        mongoTemplate.save(status, collectionName);
        LOGGER.debug("Created delivery status: {} for notification: {} to recipient: {} via {}",
                status.getId(), notificationId, recipient, channel);

        publishStatusUpdate(status);
        return status;
    }

    @Override
    public DeliveryStatus recordAttempt(DeliveryStatus deliveryStatus) {
        deliveryStatus.markSending();
        mongoTemplate.save(deliveryStatus, collectionName);

        // Update metrics
        getAttemptCounter(deliveryStatus.getChannel()).increment();

        LOGGER.debug("Recorded delivery attempt: {} for notification: {}, attempt #: {}",
                deliveryStatus.getId(), deliveryStatus.getNotificationId(), deliveryStatus.getAttemptCount());

        publishStatusUpdate(deliveryStatus);
        return deliveryStatus;
    }

    @Override
    public DeliveryStatus recordSuccess(DeliveryStatus deliveryStatus) {
        deliveryStatus.markDelivered();
        mongoTemplate.save(deliveryStatus, collectionName);

        // Update metrics
        getSuccessCounter(deliveryStatus.getChannel()).increment();

        // Record delivery duration if we have both sent and delivered times
        if (deliveryStatus.getSentTime() != null && deliveryStatus.getDeliveredTime() != null) {
            long durationMs = deliveryStatus.getDeliveredTime().getTime() - deliveryStatus.getSentTime().getTime();
            getDeliveryTimer(deliveryStatus.getChannel()).record(java.time.Duration.ofMillis(durationMs));
        }

        LOGGER.debug("Recorded successful delivery: {} for notification: {}",
                deliveryStatus.getId(), deliveryStatus.getNotificationId());

        publishStatusUpdate(deliveryStatus);
        return deliveryStatus;
    }

    @Override
    public DeliveryStatus recordFailure(DeliveryStatus deliveryStatus, String errorMessage) {
        deliveryStatus.markFailed(errorMessage);
        mongoTemplate.save(deliveryStatus, collectionName);

        // Update metrics
        getFailureCounter(deliveryStatus.getChannel()).increment();

        LOGGER.debug("Recorded failed delivery: {} for notification: {}, error: {}",
                deliveryStatus.getId(), deliveryStatus.getNotificationId(), errorMessage);

        publishStatusUpdate(deliveryStatus);
        return deliveryStatus;
    }

    @Override
    public DeliveryStatus recordRetry(DeliveryStatus deliveryStatus, String errorMessage) {
        deliveryStatus.markRetrying(errorMessage);
        mongoTemplate.save(deliveryStatus, collectionName);

        LOGGER.debug("Recorded delivery retry: {} for notification: {}, error: {}",
                deliveryStatus.getId(), deliveryStatus.getNotificationId(), errorMessage);

        publishStatusUpdate(deliveryStatus);
        return deliveryStatus;
    }

    @Override
    public DeliveryStatus recordCancellation(DeliveryStatus deliveryStatus, String reason) {
        deliveryStatus.markCancelled(reason);
        mongoTemplate.save(deliveryStatus, collectionName);

        LOGGER.debug("Recorded cancelled delivery: {} for notification: {}, reason: {}",
                deliveryStatus.getId(), deliveryStatus.getNotificationId(), reason);

        publishStatusUpdate(deliveryStatus);
        return deliveryStatus;
    }

    @Override
    public DeliveryStatus getStatusById(String id) {
        return mongoTemplate.findById(id, DeliveryStatus.class, collectionName);
    }

    @Override
    public List<DeliveryStatus> getStatusesByNotificationId(long notificationId) {
        Query query = new Query(Criteria.where("notificationId").is(notificationId));
        return mongoTemplate.find(query, DeliveryStatus.class, collectionName);
    }

    @Override
    public List<DeliveryStatus> getStatuses(Map<String, Object> filters, int offset, int limit) {
        Query query = createFilterQuery(filters);
        query.with(PageRequest.of(offset / limit, limit, Sort.by(Sort.Direction.DESC, "timestamp")));
        return mongoTemplate.find(query, DeliveryStatus.class, collectionName);
    }

    @Override
    public int countStatuses(Map<String, Object> filters) {
        Query query = createFilterQuery(filters);
        return (int) mongoTemplate.count(query, DeliveryStatus.class, collectionName);
    }

    /**
     * Creates a MongoDB query from a map of filter criteria
     *
     * @param filters Map of filter criteria
     * @return The MongoDB query
     */
    private Query createFilterQuery(Map<String, Object> filters) {
        Criteria criteria = new Criteria();
        List<Criteria> criteriaList = new ArrayList<>();

        if (filters != null) {
            if (filters.containsKey("notificationId")) {
                criteriaList.add(Criteria.where("notificationId").is(filters.get("notificationId")));
            }
            if (filters.containsKey("channel")) {
                criteriaList.add(Criteria.where("channel").is(filters.get("channel")));
            }
            if (filters.containsKey("status")) {
                criteriaList.add(Criteria.where("status").is(filters.get("status")));
            }
            if (filters.containsKey("recipient")) {
                criteriaList.add(Criteria.where("recipient").regex(filters.get("recipient").toString(), "i"));
            }
            if (filters.containsKey("timeFrom") && filters.containsKey("timeTo")) {
                criteriaList.add(Criteria.where("timestamp").gte(filters.get("timeFrom")).lte(filters.get("timeTo")));
            } else if (filters.containsKey("timeFrom")) {
                criteriaList.add(Criteria.where("timestamp").gte(filters.get("timeFrom")));
            } else if (filters.containsKey("timeTo")) {
                criteriaList.add(Criteria.where("timestamp").lte(filters.get("timeTo")));
            }
            if (filters.containsKey("correlationId")) {
                criteriaList.add(Criteria.where("correlationId").is(filters.get("correlationId")));
            }
        }

        if (!criteriaList.isEmpty()) {
            criteria = new Criteria().andOperator(criteriaList.toArray(new Criteria[0]));
        }

        return new Query(criteria);
    }

    @Override
    public Map<String, Map<String, Object>> getDeliveryStatistics(Date from, Date to, String groupBy) {
        Criteria timeCriteria = Criteria.where("timestamp").gte(from).lte(to);

        Aggregation aggregation = newAggregation(
                match(timeCriteria),
                group(groupBy)
                        .count().as("total")
                        .sum(ConditionalOperators.when(Criteria.where("status").is(DeliveryStatus.STATUS_DELIVERED))
                                .then(1).otherwise(0)).as("delivered")
                        .sum(ConditionalOperators.when(Criteria.where("status").is(DeliveryStatus.STATUS_FAILED))
                                .then(1).otherwise(0)).as("failed")
                        .sum(ConditionalOperators.when(Criteria.where("status").is(DeliveryStatus.STATUS_PENDING))
                                .then(1).otherwise(0)).as("pending")
                        .sum(ConditionalOperators.when(Criteria.where("status").is(DeliveryStatus.STATUS_SENDING))
                                .then(1).otherwise(0)).as("sending")
                        .sum(ConditionalOperators.when(Criteria.where("status").is(DeliveryStatus.STATUS_RETRYING))
                                .then(1).otherwise(0)).as("retrying")
                        .sum(ConditionalOperators.when(Criteria.where("status").is(DeliveryStatus.STATUS_CANCELLED))
                                .then(1).otherwise(0)).as("cancelled")
                        .avg("attemptCount").as("avgAttempts")
        );

        AggregationResults<Map> results = mongoTemplate.aggregate(
                aggregation, collectionName, Map.class);

        Map<String, Map<String, Object>> statisticsMap = new HashMap<>();
        for (Map result : results.getMappedResults()) {
            String key = result.get("_id").toString();
            Map<String, Object> stats = new HashMap<>();
            stats.put("total", result.get("total"));
            stats.put("delivered", result.get("delivered"));
            stats.put("failed", result.get("failed"));
            stats.put("pending", result.get("pending"));
            stats.put("sending", result.get("sending"));
            stats.put("retrying", result.get("retrying"));
            stats.put("cancelled", result.get("cancelled"));
            stats.put("avgAttempts", result.get("avgAttempts"));

            // Calculate success rate
            long total = ((Number) result.get("total")).longValue();
            long delivered = ((Number) result.get("delivered")).longValue();
            double successRate = total > 0 ? (double) delivered / total * 100 : 0;
            stats.put("successRate", successRate);

            statisticsMap.put(key, stats);
        }

        return statisticsMap;
    }

    @Override
    public double getSuccessRate(String channel, Date from, Date to) {
        Criteria criteria = Criteria.where("channel").is(channel)
                .and("timestamp").gte(from).lte(to);

        Aggregation aggregation = newAggregation(
                match(criteria),
                group()
                        .count().as("total")
                        .sum(ConditionalOperators.when(Criteria.where("status").is(DeliveryStatus.STATUS_DELIVERED))
                                .then(1).otherwise(0)).as("delivered")
        );

        AggregationResults<Map> results = mongoTemplate.aggregate(
                aggregation, collectionName, Map.class);

        if (results.getMappedResults().isEmpty()) {
            return 0;
        }

        Map result = results.getMappedResults().get(0);
        long total = ((Number) result.get("total")).longValue();
        long delivered = ((Number) result.get("delivered")).longValue();

        return total > 0 ? (double) delivered / total * 100 : 0;
    }

    @Override
    public double getOverallSuccessRate(Date from, Date to) {
        Criteria criteria = Criteria.where("timestamp").gte(from).lte(to);

        Aggregation aggregation = newAggregation(
                match(criteria),
                group()
                        .count().as("total")
                        .sum(ConditionalOperators.when(Criteria.where("status").is(DeliveryStatus.STATUS_DELIVERED))
                                .then(1).otherwise(0)).as("delivered")
        );

        AggregationResults<Map> results = mongoTemplate.aggregate(
                aggregation, collectionName, Map.class);

        if (results.getMappedResults().isEmpty()) {
            return 0;
        }

        Map result = results.getMappedResults().get(0);
        long total = ((Number) result.get("total")).longValue();
        long delivered = ((Number) result.get("delivered")).longValue();

        return total > 0 ? (double) delivered / total * 100 : 0;
    }

    @Override
    public void publishStatusUpdate(DeliveryStatus deliveryStatus) {
        try {
            messageProducer.send(statusTopic, deliveryStatus);
            LOGGER.debug("Published delivery status update to topic: {}, status: {}, notification: {}",
                    statusTopic, deliveryStatus.getStatus(), deliveryStatus.getNotificationId());
        } catch (Exception e) {
            LOGGER.error("Failed to publish delivery status update to topic: {}, status: {}, notification: {}",
                    statusTopic, deliveryStatus.getStatus(), deliveryStatus.getNotificationId(), e);
        }
    }
}