/*
 * Copyright 2022 Anton Tananaev (anton@traccar.org)
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
package org.traccar.storage;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.github.resilience4j.circuitbreaker.CircuitBreaker;
import io.github.resilience4j.circuitbreaker.CircuitBreakerConfig;
import io.github.resilience4j.circuitbreaker.CircuitBreakerRegistry;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Timer;
import io.opentelemetry.api.OpenTelemetry;
import io.opentelemetry.api.common.AttributeKey;
import io.opentelemetry.api.common.Attributes;
import io.opentelemetry.api.trace.Span;
import io.opentelemetry.api.trace.SpanKind;
import io.opentelemetry.api.trace.StatusCode;
import io.opentelemetry.api.trace.Tracer;
import io.opentelemetry.context.Context;
import org.traccar.config.Config;
import org.traccar.model.BaseModel;
import org.traccar.model.Device;
import org.traccar.model.Group;
import org.traccar.model.GroupedModel;
import org.traccar.model.OutboxMessage;
import org.traccar.model.Permission;
import org.traccar.storage.query.Columns;
import org.traccar.storage.query.Condition;
import org.traccar.storage.query.Order;
import org.traccar.storage.query.Request;

import jakarta.inject.Inject;
import javax.sql.DataSource;
import java.sql.SQLException;
import java.time.Duration;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.stream.Collectors;

public class DatabaseStorage extends Storage {

    private static final String CIRCUIT_BREAKER_NAME = "databaseStorage";
    private static final String TRACER_NAME = "org.traccar.storage.DatabaseStorage";
    
    private static final AttributeKey<String> DB_OPERATION = AttributeKey.stringKey("db.operation");
    private static final AttributeKey<String> DB_STATEMENT = AttributeKey.stringKey("db.statement");
    private static final AttributeKey<String> DB_TABLE = AttributeKey.stringKey("db.table");
    private static final AttributeKey<String> DB_TYPE = AttributeKey.stringKey("db.type");
    
    private final Config config;
    private final DataSource dataSource;
    private final ObjectMapper objectMapper;
    private final String databaseType;
    private final CircuitBreaker circuitBreaker;
    private final Tracer tracer;
    private final MeterRegistry meterRegistry;
    private final boolean readOnly;
    private final Executor asyncExecutor;

    @Inject
    public DatabaseStorage(Config config, DataSource dataSource, ObjectMapper objectMapper, 
                          OpenTelemetry openTelemetry, MeterRegistry meterRegistry, Executor asyncExecutor) {
        this(config, dataSource, objectMapper, openTelemetry, meterRegistry, asyncExecutor, false);
    }
    
    public DatabaseStorage(Config config, DataSource dataSource, ObjectMapper objectMapper, 
                          OpenTelemetry openTelemetry, MeterRegistry meterRegistry, Executor asyncExecutor, 
                          boolean readOnly) {
        this.config = config;
        this.dataSource = dataSource;
        this.objectMapper = objectMapper;
        this.meterRegistry = meterRegistry;
        this.asyncExecutor = asyncExecutor;
        this.readOnly = readOnly;
        
        // Initialize OpenTelemetry tracer
        this.tracer = openTelemetry.getTracer(TRACER_NAME);
        
        // Initialize circuit breaker
        CircuitBreakerConfig circuitBreakerConfig = CircuitBreakerConfig.custom()
                .failureRateThreshold(config.getFloat("database.circuitBreaker.failureRateThreshold", 50))
                .slowCallRateThreshold(config.getFloat("database.circuitBreaker.slowCallRateThreshold", 50))
                .slowCallDurationThreshold(Duration.ofMillis(
                        config.getLong("database.circuitBreaker.slowCallDurationThresholdMs", 1000)))
                .permittedNumberOfCallsInHalfOpenState(
                        config.getInteger("database.circuitBreaker.permittedNumberOfCallsInHalfOpenState", 10))
                .minimumNumberOfCalls(config.getInteger("database.circuitBreaker.minimumNumberOfCalls", 10))
                .waitDurationInOpenState(Duration.ofMillis(
                        config.getLong("database.circuitBreaker.waitDurationInOpenStateMs", 60000)))
                .automaticTransitionFromOpenToHalfOpenEnabled(true)
                .build();
        
        CircuitBreakerRegistry circuitBreakerRegistry = CircuitBreakerRegistry.of(circuitBreakerConfig);
        this.circuitBreaker = circuitBreakerRegistry.circuitBreaker(CIRCUIT_BREAKER_NAME);

        try {
            databaseType = dataSource.getConnection().getMetaData().getDatabaseProductName();
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    /**
     * Checks if this storage instance is in read-only mode
     * @return true if this storage is read-only, false otherwise
     */
    public boolean isReadOnly() {
        return readOnly;
    }
    
    /**
     * Execute database operation with circuit breaker, metrics, and tracing
     */
    private <T> T executeWithResilience(String operation, String table, String statement, Supplier<T> supplier) 
            throws StorageException {
        Timer.Sample timer = Timer.start(meterRegistry);
        
        Span span = tracer.spanBuilder(operation + " " + table)
                .setSpanKind(SpanKind.CLIENT)
                .setAttribute(DB_OPERATION, operation)
                .setAttribute(DB_TABLE, table)
                .setAttribute(DB_STATEMENT, statement)
                .setAttribute(DB_TYPE, databaseType)
                .startSpan();
        
        try {
            T result = circuitBreaker.executeSupplier(supplier);
            span.setStatus(StatusCode.OK);
            timer.stop(meterRegistry.timer("database.operation", 
                    "operation", operation, 
                    "table", table, 
                    "result", "success"));
            return result;
        } catch (Exception e) {
            span.setStatus(StatusCode.ERROR, e.getMessage());
            span.recordException(e);
            timer.stop(meterRegistry.timer("database.operation", 
                    "operation", operation, 
                    "table", table, 
                    "result", "error"));
            if (e instanceof StorageException) {
                throw (StorageException) e;
            } else {
                throw new StorageException(e);
            }
        } finally {
            span.end();
        }
    }

    @Override
    public <T> List<T> getObjects(Class<T> clazz, Request request) throws StorageException {
        if (circuitBreaker.getState() == CircuitBreaker.State.OPEN && !readOnly) {
            throw new StorageException("Database circuit breaker is open");
        }
        
        String storageName = getStorageName(clazz);
        StringBuilder query = new StringBuilder("SELECT ");
        if (request.getColumns() instanceof Columns.All) {
            query.append('*');
        } else {
            query.append(formatColumns(request.getColumns().getColumns(clazz, "set"), c -> c));
        }
        query.append(" FROM ").append(storageName);
        query.append(formatCondition(request.getCondition()));
        query.append(formatOrder(request.getOrder()));
        
        String finalQuery = query.toString();
        return executeWithResilience("SELECT", storageName, finalQuery, () -> {
            try {
                QueryBuilder builder = QueryBuilder.create(config, dataSource, objectMapper, finalQuery);
                for (Map.Entry<String, Object> variable : getConditionVariables(request.getCondition()).entrySet()) {
                    builder.setValue(variable.getKey(), variable.getValue());
                }
                return builder.executeQuery(clazz);
            } catch (SQLException e) {
                throw new StorageException(e);
            }
        });
    }

    @Override
    public <T> long addObject(T entity, Request request) throws StorageException {
        if (readOnly) {
            throw new StorageException("Cannot add object in read-only mode");
        }
        
        if (circuitBreaker.getState() == CircuitBreaker.State.OPEN) {
            throw new StorageException("Database circuit breaker is open");
        }
        
        String storageName = getStorageName(entity.getClass());
        List<String> columns = request.getColumns().getColumns(entity.getClass(), "get");
        StringBuilder query = new StringBuilder("INSERT INTO ");
        query.append(storageName);
        query.append("(");
        query.append(formatColumns(columns, c -> c));
        query.append(") VALUES (");
        query.append(formatColumns(columns, c -> ':' + c));
        query.append(")");
        
        String finalQuery = query.toString();
        return executeWithResilience("INSERT", storageName, finalQuery, () -> {
            try {
                QueryBuilder builder = QueryBuilder.create(config, dataSource, objectMapper, finalQuery, true);
                builder.setObject(entity, columns);
                return builder.executeUpdate();
            } catch (SQLException e) {
                throw new StorageException(e);
            }
        });
    }

    @Override
    public <T> void updateObject(T entity, Request request) throws StorageException {
        if (readOnly) {
            throw new StorageException("Cannot update object in read-only mode");
        }
        
        if (circuitBreaker.getState() == CircuitBreaker.State.OPEN) {
            throw new StorageException("Database circuit breaker is open");
        }
        
        String storageName = getStorageName(entity.getClass());
        List<String> columns = request.getColumns().getColumns(entity.getClass(), "get");
        StringBuilder query = new StringBuilder("UPDATE ");
        query.append(storageName);
        query.append(" SET ");
        query.append(formatColumns(columns, c -> c + " = :" + c));
        query.append(formatCondition(request.getCondition()));
        
        String finalQuery = query.toString();
        executeWithResilience("UPDATE", storageName, finalQuery, () -> {
            try {
                QueryBuilder builder = QueryBuilder.create(config, dataSource, objectMapper, finalQuery);
                builder.setObject(entity, columns);
                for (Map.Entry<String, Object> variable : getConditionVariables(request.getCondition()).entrySet()) {
                    builder.setValue(variable.getKey(), variable.getValue());
                }
                return builder.executeUpdate();
            } catch (SQLException e) {
                throw new StorageException(e);
            }
        });
    }

    @Override
    public void removeObject(Class<?> clazz, Request request) throws StorageException {
        if (readOnly) {
            throw new StorageException("Cannot remove object in read-only mode");
        }
        
        if (circuitBreaker.getState() == CircuitBreaker.State.OPEN) {
            throw new StorageException("Database circuit breaker is open");
        }
        
        String storageName = getStorageName(clazz);
        StringBuilder query = new StringBuilder("DELETE FROM ");
        query.append(storageName);
        query.append(formatCondition(request.getCondition()));
        
        String finalQuery = query.toString();
        executeWithResilience("DELETE", storageName, finalQuery, () -> {
            try {
                QueryBuilder builder = QueryBuilder.create(config, dataSource, objectMapper, finalQuery);
                for (Map.Entry<String, Object> variable : getConditionVariables(request.getCondition()).entrySet()) {
                    builder.setValue(variable.getKey(), variable.getValue());
                }
                return builder.executeUpdate();
            } catch (SQLException e) {
                throw new StorageException(e);
            }
        });
    }

    @Override
    public List<Permission> getPermissions(
            Class<? extends BaseModel> ownerClass, long ownerId,
            Class<? extends BaseModel> propertyClass, long propertyId) throws StorageException {
        if (circuitBreaker.getState() == CircuitBreaker.State.OPEN && !readOnly) {
            throw new StorageException("Database circuit breaker is open");
        }
        
        String storageName = Permission.getStorageName(ownerClass, propertyClass);
        StringBuilder query = new StringBuilder("SELECT * FROM ");
        query.append(storageName);
        var conditions = new LinkedList<Condition>();
        if (ownerId > 0) {
            conditions.add(new Condition.Equals(Permission.getKey(ownerClass), ownerId));
        }
        if (propertyId > 0) {
            conditions.add(new Condition.Equals(Permission.getKey(propertyClass), propertyId));
        }
        Condition combinedCondition = Condition.merge(conditions);
        query.append(formatCondition(combinedCondition));
        
        String finalQuery = query.toString();
        return executeWithResilience("SELECT", storageName, finalQuery, () -> {
            try {
                QueryBuilder builder = QueryBuilder.create(config, dataSource, objectMapper, finalQuery);
                for (Map.Entry<String, Object> variable : getConditionVariables(combinedCondition).entrySet()) {
                    builder.setValue(variable.getKey(), variable.getValue());
                }
                return builder.executePermissionsQuery();
            } catch (SQLException e) {
                throw new StorageException(e);
            }
        });
    }

    @Override
    public void addPermission(Permission permission) throws StorageException {
        if (readOnly) {
            throw new StorageException("Cannot add permission in read-only mode");
        }
        
        if (circuitBreaker.getState() == CircuitBreaker.State.OPEN) {
            throw new StorageException("Database circuit breaker is open");
        }
        
        String storageName = permission.getStorageName();
        StringBuilder query = new StringBuilder("INSERT INTO ");
        query.append(storageName);
        query.append(" VALUES (");
        query.append(permission.get().keySet().stream().map(key -> ':' + key).collect(Collectors.joining(", ")));
        query.append(")");
        
        String finalQuery = query.toString();
        executeWithResilience("INSERT", storageName, finalQuery, () -> {
            try {
                QueryBuilder builder = QueryBuilder.create(config, dataSource, objectMapper, finalQuery, true);
                for (var entry : permission.get().entrySet()) {
                    builder.setLong(entry.getKey(), entry.getValue());
                }
                return builder.executeUpdate();
            } catch (SQLException e) {
                throw new StorageException(e);
            }
        });
    }

    @Override
    public void removePermission(Permission permission) throws StorageException {
        if (readOnly) {
            throw new StorageException("Cannot remove permission in read-only mode");
        }
        
        if (circuitBreaker.getState() == CircuitBreaker.State.OPEN) {
            throw new StorageException("Database circuit breaker is open");
        }
        
        String storageName = permission.getStorageName();
        StringBuilder query = new StringBuilder("DELETE FROM ");
        query.append(storageName);
        query.append(" WHERE ");
        query.append(permission
                .get().keySet().stream().map(key -> key + " = :" + key).collect(Collectors.joining(" AND ")));
        
        String finalQuery = query.toString();
        executeWithResilience("DELETE", storageName, finalQuery, () -> {
            try {
                QueryBuilder builder = QueryBuilder.create(config, dataSource, objectMapper, finalQuery, true);
                for (var entry : permission.get().entrySet()) {
                    builder.setLong(entry.getKey(), entry.getValue());
                }
                return builder.executeUpdate();
            } catch (SQLException e) {
                throw new StorageException(e);
            }
        });
    }
    
    /**
     * Implements the Transaction Outbox pattern for reliable message publishing
     * Stores a message in the outbox table as part of the current transaction
     * 
     * @param topic The topic to publish the message to
     * @param payload The message payload
     * @param headers Optional message headers
     * @return The ID of the created outbox message
     * @throws StorageException If an error occurs while storing the message
     */
    public String storeOutboxMessage(String topic, Object payload, Map<String, String> headers) throws StorageException {
        if (readOnly) {
            throw new StorageException("Cannot store outbox message in read-only mode");
        }
        
        if (circuitBreaker.getState() == CircuitBreaker.State.OPEN) {
            throw new StorageException("Database circuit breaker is open");
        }
        
        String messageId = UUID.randomUUID().toString();
        OutboxMessage message = new OutboxMessage();
        message.setId(messageId);
        message.setTopic(topic);
        message.setPayload(payload);
        message.setHeaders(headers != null ? headers : new HashMap<>());
        message.setCreatedAt(System.currentTimeMillis());
        message.setStatus(OutboxMessage.Status.PENDING);
        
        Request request = Request.builder().columns(Columns.all()).build();
        addObject(message, request);
        
        return messageId;
    }
    
    /**
     * Asynchronously processes outbox messages that are pending delivery
     * This method should be called by a scheduled task or message relay service
     * 
     * @param processor Function that processes each message and returns true if successful
     * @return CompletableFuture that completes when all pending messages are processed
     */
    public CompletableFuture<Void> processOutboxMessages(Function<OutboxMessage, Boolean> processor) {
        return CompletableFuture.runAsync(() -> {
            try {
                // Get all pending outbox messages
                Request request = Request.builder()
                        .condition(new Condition.Equals("status", OutboxMessage.Status.PENDING.name()))
                        .order(Order.ascending("createdAt"))
                        .build();
                
                List<OutboxMessage> messages = getObjects(OutboxMessage.class, request);
                
                for (OutboxMessage message : messages) {
                    try {
                        boolean success = processor.apply(message);
                        if (success) {
                            // Update message status to PROCESSED
                            message.setStatus(OutboxMessage.Status.PROCESSED);
                            message.setProcessedAt(System.currentTimeMillis());
                            updateObject(message, Request.builder().columns(Columns.all()).build());
                        } else {
                            // Update message status to FAILED if processor returned false
                            message.setStatus(OutboxMessage.Status.FAILED);
                            message.setFailedAt(System.currentTimeMillis());
                            updateObject(message, Request.builder().columns(Columns.all()).build());
                        }
                    } catch (Exception e) {
                        // Update message status to FAILED if an exception occurred
                        message.setStatus(OutboxMessage.Status.FAILED);
                        message.setFailedAt(System.currentTimeMillis());
                        message.setErrorMessage(e.getMessage());
                        updateObject(message, Request.builder().columns(Columns.all()).build());
                    }
                }
            } catch (StorageException e) {
                throw new RuntimeException("Failed to process outbox messages", e);
            }
        }, asyncExecutor);
    }

    private String getStorageName(Class<?> clazz) throws StorageException {
        StorageName storageName = clazz.getAnnotation(StorageName.class);
        if (storageName == null) {
            throw new StorageException("StorageName annotation is missing");
        }
        return storageName.value();
    }

    private Map<String, Object> getConditionVariables(Condition genericCondition) {
        Map<String, Object> results = new HashMap<>();
        if (genericCondition instanceof Condition.Compare condition) {
            if (condition.getValue() != null) {
                results.put(condition.getVariable(), condition.getValue());
            }
        } else if (genericCondition instanceof Condition.Between condition) {
            results.put(condition.getFromVariable(), condition.getFromValue());
            results.put(condition.getToVariable(), condition.getToValue());
        } else if (genericCondition instanceof Condition.Binary condition) {
            results.putAll(getConditionVariables(condition.getFirst()));
            results.putAll(getConditionVariables(condition.getSecond()));
        } else if (genericCondition instanceof Condition.Permission condition) {
            if (condition.getOwnerId() > 0) {
                results.put(Permission.getKey(condition.getOwnerClass()), condition.getOwnerId());
            } else {
                results.put(Permission.getKey(condition.getPropertyClass()), condition.getPropertyId());
            }
        } else if (genericCondition instanceof Condition.LatestPositions condition) {
            if (condition.getDeviceId() > 0) {
                results.put("deviceId", condition.getDeviceId());
            }
        }
        return results;
    }

    private String formatColumns(List<String> columns, Function<String, String> mapper) {
        return columns.stream().map(mapper).collect(Collectors.joining(", "));
    }

    private String formatCondition(Condition genericCondition) throws StorageException {
        return formatCondition(genericCondition, true);
    }

    private String formatCondition(Condition genericCondition, boolean appendWhere) throws StorageException {
        StringBuilder result = new StringBuilder();
        if (genericCondition != null) {
            if (appendWhere) {
                result.append(" WHERE ");
            }
            if (genericCondition instanceof Condition.Compare condition) {

                result.append(condition.getColumn());
                result.append(" ");
                result.append(condition.getOperator());
                result.append(" :");
                result.append(condition.getVariable());

            } else if (genericCondition instanceof Condition.Between condition) {

                result.append(condition.getColumn());
                result.append(" BETWEEN :");
                result.append(condition.getFromVariable());
                result.append(" AND :");
                result.append(condition.getToVariable());

            } else if (genericCondition instanceof Condition.Binary condition) {

                result.append(formatCondition(condition.getFirst(), false));
                result.append(" ");
                result.append(condition.getOperator());
                result.append(" ");
                result.append(formatCondition(condition.getSecond(), false));

            } else if (genericCondition instanceof Condition.Permission condition) {

                result.append("id IN (");
                result.append(formatPermissionQuery(condition));
                result.append(")");

            } else if (genericCondition instanceof Condition.LatestPositions condition) {

                result.append("id IN (");
                result.append("SELECT positionId FROM ");
                result.append(getStorageName(Device.class));
                if (condition.getDeviceId() > 0) {
                    result.append(" WHERE id = :deviceId");
                }
                result.append(")");

            }
        }
        return result.toString();
    }

    private String formatOrder(Order order) {
        StringBuilder result = new StringBuilder();
        if (order != null) {
            result.append(" ORDER BY ");
            result.append(order.getColumn());
            if (order.getDescending()) {
                result.append(" DESC");
            }
            if (order.getLimit() > 0) {
                if (databaseType.equals("Microsoft SQL Server")) {
                    result.append(" OFFSET 0 ROWS FETCH FIRST ");
                    result.append(order.getLimit());
                    result.append(" ROWS ONLY");
                } else {
                    result.append(" LIMIT ");
                    result.append(order.getLimit());
                }
            }
        }
        return result.toString();
    }

    private String formatPermissionQuery(Condition.Permission condition) throws StorageException {
        StringBuilder result = new StringBuilder();

        String outputKey;
        String conditionKey;
        if (condition.getOwnerId() > 0) {
            outputKey = Permission.getKey(condition.getPropertyClass());
            conditionKey = Permission.getKey(condition.getOwnerClass());
        } else {
            outputKey = Permission.getKey(condition.getOwnerClass());
            conditionKey = Permission.getKey(condition.getPropertyClass());
        }

        String storageName = Permission.getStorageName(condition.getOwnerClass(), condition.getPropertyClass());
        result.append("SELECT ");
        result.append(storageName).append('.').append(outputKey);
        result.append(" FROM ");
        result.append(storageName);
        result.append(" WHERE ");
        result.append(conditionKey);
        result.append(" = :");
        result.append(conditionKey);

        if (condition.getIncludeGroups()) {

            boolean expandDevices;
            String groupStorageName;
            if (GroupedModel.class.isAssignableFrom(condition.getOwnerClass())) {
                expandDevices = Device.class.isAssignableFrom(condition.getOwnerClass());
                groupStorageName = Permission.getStorageName(Group.class, condition.getPropertyClass());
            } else {
                expandDevices = Device.class.isAssignableFrom(condition.getPropertyClass());
                groupStorageName = Permission.getStorageName(condition.getOwnerClass(), Group.class);
            }

            result.append(" UNION ");

            result.append("SELECT DISTINCT ");
            if (!expandDevices) {
                if (outputKey.equals("groupId")) {
                    result.append("all_groups.");
                } else {
                    result.append(groupStorageName).append('.');
                }
            }
            result.append(outputKey);
            result.append(" FROM ");
            result.append(groupStorageName);

            result.append(" INNER JOIN (");
            result.append("SELECT id as parentId, id as groupId FROM ");
            result.append(getStorageName(Group.class));
            result.append(" UNION ");
            result.append("SELECT groupId as parentId, id as groupId FROM ");
            result.append(getStorageName(Group.class));
            result.append(" WHERE groupId IS NOT NULL");
            result.append(" UNION ");
            result.append("SELECT g2.groupId as parentId, g1.id as groupId FROM ");
            result.append(getStorageName(Group.class));
            result.append(" AS g2");
            result.append(" INNER JOIN ");
            result.append(getStorageName(Group.class));
            result.append(" AS g1 ON g2.id = g1.groupId");
            result.append(" WHERE g2.groupId IS NOT NULL");
            result.append(") AS all_groups ON ");
            result.append(groupStorageName);
            result.append(".groupId = all_groups.parentId");

            if (expandDevices) {
                result.append(" INNER JOIN (");
                result.append("SELECT groupId as parentId, id as deviceId FROM ");
                result.append(getStorageName(Device.class));
                result.append(" WHERE groupId IS NOT NULL");
                result.append(") AS devices ON all_groups.groupId = devices.parentId");
            }

            result.append(" WHERE ");
            result.append(conditionKey);
            result.append(" = :");
            result.append(conditionKey);

        }

        return result.toString();
    }

}