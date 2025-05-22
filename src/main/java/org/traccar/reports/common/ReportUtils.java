/*
 * Copyright 2016 - 2023 Anton Tananaev (anton@traccar.org)
 * Copyright 2016 - 2017 Andrey Kunitsyn (andrey@traccar.org)
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
package org.traccar.reports.common;

import jakarta.annotation.Nullable;
import jakarta.inject.Inject;
import jakarta.inject.Singleton;

import org.apache.velocity.app.VelocityEngine;
import org.apache.velocity.tools.generic.DateTool;
import org.apache.velocity.tools.generic.NumberTool;
import org.jxls.area.Area;
import org.jxls.builder.xls.XlsCommentAreaBuilder;
import org.jxls.common.CellRef;
import org.jxls.formula.StandardFormulaProcessor;
import org.jxls.transform.Transformer;
import org.jxls.transform.poi.PoiTransformer;
import org.jxls.util.TransformerFactory;
import org.traccar.api.security.PermissionsService;
import org.traccar.config.Config;
import org.traccar.config.Keys;
import org.traccar.discovery.ServiceDiscovery;
import org.traccar.geocoder.Geocoder;
import org.traccar.helper.UnitsConverter;
import org.traccar.helper.model.AttributeUtil;
import org.traccar.helper.model.PositionUtil;
import org.traccar.helper.model.UserUtil;
import org.traccar.metrics.MetricsService;
import org.traccar.model.BaseModel;
import org.traccar.model.Device;
import org.traccar.model.Driver;
import org.traccar.model.Event;
import org.traccar.model.Position;
import org.traccar.model.User;
import org.traccar.reports.model.BaseReportItem;
import org.traccar.reports.model.StopReportItem;
import org.traccar.reports.model.TripReportItem;
import org.traccar.session.state.MotionProcessor;
import org.traccar.session.state.MotionState;
import org.traccar.storage.Storage;
import org.traccar.storage.StorageException;
import org.traccar.storage.query.Columns;
import org.traccar.storage.query.Condition;
import org.traccar.storage.query.Order;
import org.traccar.storage.query.Request;

// Object storage imports
import org.traccar.storage.ObjectStorage;
import org.traccar.storage.ObjectStorageException;

// Distributed tracing imports
import io.opentelemetry.api.trace.Span;
import io.opentelemetry.api.trace.Tracer;
import io.opentelemetry.context.Scope;
import io.opentelemetry.api.trace.SpanKind;
import io.opentelemetry.api.common.AttributeKey;
import io.opentelemetry.api.common.Attributes;

// Circuit breaker imports
import io.github.resilience4j.circuitbreaker.CircuitBreaker;
import io.github.resilience4j.circuitbreaker.CircuitBreakerConfig;
import io.github.resilience4j.circuitbreaker.CircuitBreakerRegistry;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.URI;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Locale;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.function.Supplier;
import java.util.stream.Collectors;

/**
 * Utility class for report generation with support for distributed tracing,
 * service discovery, object storage, metrics collection, and circuit breakers.
 */
@Singleton
public class ReportUtils {

    private final Config config;
    private final Storage storage;
    private final PermissionsService permissionsService;
    private final VelocityEngine velocityEngine;
    private final Geocoder geocoder;
    
    // New dependencies for microservices architecture
    private final ServiceDiscovery serviceDiscovery;
    private final ObjectStorage objectStorage;
    private final MetricsService metricsService;
    private final Tracer tracer;
    private final CircuitBreakerRegistry circuitBreakerRegistry;
    private final ExecutorService executorService;

    /**
     * Circuit breaker for geocoding operations
     */
    private final CircuitBreaker geocoderCircuitBreaker;
    
    /**
     * Circuit breaker for storage operations
     */
    private final CircuitBreaker storageCircuitBreaker;
    
    /**
     * Circuit breaker for object storage operations
     */
    private final CircuitBreaker objectStorageCircuitBreaker;

    /**
     * Constructor with all required dependencies for the microservices architecture.
     * 
     * @param config Configuration provider
     * @param storage Data storage provider
     * @param permissionsService Permissions service
     * @param velocityEngine Velocity template engine
     * @param geocoder Geocoding service (optional)
     * @param serviceDiscovery Service discovery provider
     * @param objectStorage Object storage provider
     * @param metricsService Metrics collection service
     * @param tracer OpenTelemetry tracer
     * @param circuitBreakerRegistry Circuit breaker registry
     */
    @Inject
    public ReportUtils(
            Config config, 
            Storage storage, 
            PermissionsService permissionsService,
            VelocityEngine velocityEngine, 
            @Nullable Geocoder geocoder,
            ServiceDiscovery serviceDiscovery,
            ObjectStorage objectStorage,
            MetricsService metricsService,
            Tracer tracer,
            CircuitBreakerRegistry circuitBreakerRegistry) {
        this.config = config;
        this.storage = storage;
        this.permissionsService = permissionsService;
        this.velocityEngine = velocityEngine;
        this.geocoder = geocoder;
        this.serviceDiscovery = serviceDiscovery;
        this.objectStorage = objectStorage;
        this.metricsService = metricsService;
        this.tracer = tracer;
        this.circuitBreakerRegistry = circuitBreakerRegistry;
        
        // Initialize thread pool for parallel processing
        int threadPoolSize = config.getInteger(Keys.REPORT_THREAD_POOL_SIZE, Runtime.getRuntime().availableProcessors());
        this.executorService = Executors.newFixedThreadPool(threadPoolSize);
        
        // Initialize circuit breakers
        CircuitBreakerConfig geocoderCircuitBreakerConfig = CircuitBreakerConfig.custom()
                .failureRateThreshold(50)
                .waitDurationInOpenState(Duration.ofSeconds(30))
                .permittedNumberOfCallsInHalfOpenState(5)
                .slidingWindowSize(10)
                .slidingWindowType(CircuitBreakerConfig.SlidingWindowType.COUNT_BASED)
                .build();
        
        CircuitBreakerConfig storageCircuitBreakerConfig = CircuitBreakerConfig.custom()
                .failureRateThreshold(50)
                .waitDurationInOpenState(Duration.ofSeconds(20))
                .permittedNumberOfCallsInHalfOpenState(3)
                .slidingWindowSize(10)
                .slidingWindowType(CircuitBreakerConfig.SlidingWindowType.COUNT_BASED)
                .build();
                
        CircuitBreakerConfig objectStorageCircuitBreakerConfig = CircuitBreakerConfig.custom()
                .failureRateThreshold(50)
                .waitDurationInOpenState(Duration.ofSeconds(20))
                .permittedNumberOfCallsInHalfOpenState(3)
                .slidingWindowSize(10)
                .slidingWindowType(CircuitBreakerConfig.SlidingWindowType.COUNT_BASED)
                .build();
        
        this.geocoderCircuitBreaker = circuitBreakerRegistry.circuitBreaker("geocoder", geocoderCircuitBreakerConfig);
        this.storageCircuitBreaker = circuitBreakerRegistry.circuitBreaker("storage", storageCircuitBreakerConfig);
        this.objectStorageCircuitBreaker = circuitBreakerRegistry.circuitBreaker("objectStorage", objectStorageCircuitBreakerConfig);
    }

    /**
     * Retrieves an object from storage with permission checks and circuit breaker protection.
     * 
     * @param <T> Type of object to retrieve
     * @param userId User ID for permission check
     * @param clazz Class of object to retrieve
     * @param objectId ID of object to retrieve
     * @return Retrieved object
     * @throws StorageException If storage operation fails
     * @throws SecurityException If permission check fails
     */
    public <T extends BaseModel> T getObject(
            long userId, Class<T> clazz, long objectId) throws StorageException, SecurityException {
        Span span = tracer.spanBuilder("ReportUtils.getObject")
                .setSpanKind(SpanKind.CLIENT)
                .setAttribute("userId", userId)
                .setAttribute("objectId", objectId)
                .setAttribute("objectType", clazz.getSimpleName())
                .startSpan();
        
        try (Scope scope = span.makeCurrent()) {
            metricsService.startTimer("report.getObject");
            return storageCircuitBreaker.executeSupplier(() -> {
                try {
                    return storage.getObject(clazz, new Request(
                            new Columns.All(),
                            new Condition.And(
                                    new Condition.Equals("id", objectId),
                                    new Condition.Permission(User.class, userId, clazz))));
                } catch (StorageException e) {
                    span.recordException(e);
                    throw e;
                }
            });
        } catch (Exception e) {
            span.recordException(e);
            span.setStatus(io.opentelemetry.api.trace.StatusCode.ERROR, e.getMessage());
            throw e;
        } finally {
            metricsService.stopTimer("report.getObject");
            span.end();
        }
    }

    /**
     * Checks if the requested time period exceeds the configured limit.
     * 
     * @param from Start date
     * @param to End date
     * @throws IllegalArgumentException If period exceeds limit
     */
    public void checkPeriodLimit(Date from, Date to) {
        Span span = tracer.spanBuilder("ReportUtils.checkPeriodLimit")
                .setSpanKind(SpanKind.INTERNAL)
                .setAttribute("from", from.getTime())
                .setAttribute("to", to.getTime())
                .startSpan();
        
        try (Scope scope = span.makeCurrent()) {
            long limit = config.getLong(Keys.REPORT_PERIOD_LIMIT) * 1000;
            long requestedPeriod = to.getTime() - from.getTime();
            span.setAttribute("periodLimit", limit);
            span.setAttribute("requestedPeriod", requestedPeriod);
            
            if (limit > 0 && requestedPeriod > limit) {
                span.setAttribute("limitExceeded", true);
                throw new IllegalArgumentException("Time period exceeds the limit");
            }
            span.setAttribute("limitExceeded", false);
        } finally {
            span.end();
        }
    }

    /**
     * Calculates fuel consumption between two positions.
     * 
     * @param first Start position
     * @param last End position
     * @return Calculated fuel consumption
     */
    public double calculateFuel(Position first, Position last) {
        if (first.hasAttribute(Position.KEY_FUEL_USED) && last.hasAttribute(Position.KEY_FUEL_USED)) {
            return last.getDouble(Position.KEY_FUEL_USED) - first.getDouble(Position.KEY_FUEL_USED);
        } else if (first.hasAttribute(Position.KEY_FUEL_LEVEL) && last.hasAttribute(Position.KEY_FUEL_LEVEL)) {
            return first.getDouble(Position.KEY_FUEL_LEVEL) - last.getDouble(Position.KEY_FUEL_LEVEL);
        }
        return 0;
    }

    /**
     * Finds driver ID from position attributes.
     * 
     * @param firstPosition First position
     * @param lastPosition Last position
     * @return Driver ID or null if not found
     */
    public String findDriver(Position firstPosition, Position lastPosition) {
        if (firstPosition.hasAttribute(Position.KEY_DRIVER_UNIQUE_ID)) {
            return firstPosition.getString(Position.KEY_DRIVER_UNIQUE_ID);
        } else if (lastPosition.hasAttribute(Position.KEY_DRIVER_UNIQUE_ID)) {
            return lastPosition.getString(Position.KEY_DRIVER_UNIQUE_ID);
        }
        return null;
    }

    /**
     * Finds driver name by unique ID with circuit breaker protection.
     * 
     * @param driverUniqueId Driver unique ID
     * @return Driver name or null if not found
     * @throws StorageException If storage operation fails
     */
    public String findDriverName(String driverUniqueId) throws StorageException {
        if (driverUniqueId == null) {
            return null;
        }
        
        Span span = tracer.spanBuilder("ReportUtils.findDriverName")
                .setSpanKind(SpanKind.CLIENT)
                .setAttribute("driverUniqueId", driverUniqueId)
                .startSpan();
        
        try (Scope scope = span.makeCurrent()) {
            metricsService.startTimer("report.findDriverName");
            return storageCircuitBreaker.executeSupplier(() -> {
                try {
                    Driver driver = storage.getObject(Driver.class, new Request(
                            new Columns.All(),
                            new Condition.Equals("uniqueId", driverUniqueId)));
                    return driver != null ? driver.getName() : null;
                } catch (StorageException e) {
                    span.recordException(e);
                    throw e;
                }
            });
        } catch (Exception e) {
            span.recordException(e);
            span.setStatus(io.opentelemetry.api.trace.StatusCode.ERROR, e.getMessage());
            throw e;
        } finally {
            metricsService.stopTimer("report.findDriverName");
            span.end();
        }
    }

    /**
     * Initializes JXLS context for report generation.
     * 
     * @param userId User ID
     * @return Initialized context
     * @throws StorageException If storage operation fails
     */
    public org.jxls.common.Context initializeContext(long userId) throws StorageException {
        Span span = tracer.spanBuilder("ReportUtils.initializeContext")
                .setSpanKind(SpanKind.INTERNAL)
                .setAttribute("userId", userId)
                .startSpan();
        
        try (Scope scope = span.makeCurrent()) {
            metricsService.startTimer("report.initializeContext");
            var server = permissionsService.getServer();
            var user = permissionsService.getUser(userId);
            var context = PoiTransformer.createInitialContext();
            context.putVar("distanceUnit", UserUtil.getDistanceUnit(server, user));
            context.putVar("speedUnit", UserUtil.getSpeedUnit(server, user));
            context.putVar("volumeUnit", UserUtil.getVolumeUnit(server, user));
            context.putVar("webUrl", velocityEngine.getProperty("web.url"));
            context.putVar("dateTool", new DateTool());
            context.putVar("numberTool", new NumberTool());
            context.putVar("timezone", UserUtil.getTimezone(server, user));
            context.putVar("locale", Locale.getDefault());
            context.putVar("bracketsRegex", "[\\{\\}\"]");
            return context;
        } catch (Exception e) {
            span.recordException(e);
            span.setStatus(io.opentelemetry.api.trace.StatusCode.ERROR, e.getMessage());
            throw e;
        } finally {
            metricsService.stopTimer("report.initializeContext");
            span.end();
        }
    }

    /**
     * Processes a template with sheets using JXLS.
     * This method now supports both file system and object storage templates.
     * 
     * @param templateStream Template input stream
     * @param targetStream Target output stream
     * @param context JXLS context
     * @throws IOException If I/O operation fails
     */
    public void processTemplateWithSheets(
            InputStream templateStream, OutputStream targetStream, org.jxls.common.Context context) throws IOException {
        Span span = tracer.spanBuilder("ReportUtils.processTemplateWithSheets")
                .setSpanKind(SpanKind.INTERNAL)
                .startSpan();
        
        try (Scope scope = span.makeCurrent()) {
            metricsService.startTimer("report.processTemplate");
            Transformer transformer = TransformerFactory.createTransformer(templateStream, targetStream);
            List<Area> xlsAreas = new XlsCommentAreaBuilder(transformer).build();
            for (Area xlsArea : xlsAreas) {
                xlsArea.applyAt(new CellRef(xlsArea.getStartCellRef().getCellName()), context);
                xlsArea.setFormulaProcessor(new StandardFormulaProcessor());
                xlsArea.processFormulas();
            }
            transformer.deleteSheet(xlsAreas.get(0).getStartCellRef().getSheetName());
            transformer.write();
        } catch (Exception e) {
            span.recordException(e);
            span.setStatus(io.opentelemetry.api.trace.StatusCode.ERROR, e.getMessage());
            throw e;
        } finally {
            metricsService.stopTimer("report.processTemplate");
            span.end();
        }
    }
    
    /**
     * Loads a template from object storage or falls back to file system.
     * 
     * @param templatePath Template path
     * @return Template input stream
     * @throws IOException If I/O operation fails
     */
    public InputStream getTemplateStream(String templatePath) throws IOException {
        Span span = tracer.spanBuilder("ReportUtils.getTemplateStream")
                .setSpanKind(SpanKind.CLIENT)
                .setAttribute("templatePath", templatePath)
                .startSpan();
        
        try (Scope scope = span.makeCurrent()) {
            metricsService.startTimer("report.getTemplateStream");
            boolean useObjectStorage = config.getBoolean(Keys.REPORT_USE_OBJECT_STORAGE, false);
            span.setAttribute("useObjectStorage", useObjectStorage);
            
            if (useObjectStorage) {
                String bucketName = config.getString(Keys.REPORT_TEMPLATE_BUCKET);
                span.setAttribute("bucketName", bucketName);
                
                return objectStorageCircuitBreaker.executeSupplier(() -> {
                    try {
                        return objectStorage.getObject(bucketName, templatePath);
                    } catch (ObjectStorageException e) {
                        span.recordException(e);
                        throw new IOException("Failed to load template from object storage: " + e.getMessage(), e);
                    }
                });
            } else {
                // Fall back to file system
                String templatesRoot = config.getString(Keys.TEMPLATES_ROOT, "templates");
                java.nio.file.Path path = java.nio.file.Paths.get(templatesRoot, templatePath);
                span.setAttribute("filePath", path.toString());
                
                return new java.io.FileInputStream(path.toFile());
            }
        } catch (Exception e) {
            span.recordException(e);
            span.setStatus(io.opentelemetry.api.trace.StatusCode.ERROR, e.getMessage());
            throw e;
        } finally {
            metricsService.stopTimer("report.getTemplateStream");
            span.end();
        }
    }
    
    /**
     * Saves a report output to object storage.
     * 
     * @param reportName Report name
     * @param outputStream Output stream containing the report data
     * @return URI to the saved report
     * @throws IOException If I/O operation fails
     */
    public URI saveReportToObjectStorage(String reportName, java.io.ByteArrayOutputStream outputStream) throws IOException {
        Span span = tracer.spanBuilder("ReportUtils.saveReportToObjectStorage")
                .setSpanKind(SpanKind.CLIENT)
                .setAttribute("reportName", reportName)
                .startSpan();
        
        try (Scope scope = span.makeCurrent()) {
            metricsService.startTimer("report.saveToObjectStorage");
            String bucketName = config.getString(Keys.REPORT_OUTPUT_BUCKET);
            String objectKey = reportName + "_" + System.currentTimeMillis();
            span.setAttribute("bucketName", bucketName);
            span.setAttribute("objectKey", objectKey);
            
            return objectStorageCircuitBreaker.executeSupplier(() -> {
                try {
                    return objectStorage.putObject(bucketName, objectKey, outputStream.toByteArray());
                } catch (ObjectStorageException e) {
                    span.recordException(e);
                    throw new IOException("Failed to save report to object storage: " + e.getMessage(), e);
                }
            });
        } catch (Exception e) {
            span.recordException(e);
            span.setStatus(io.opentelemetry.api.trace.StatusCode.ERROR, e.getMessage());
            throw e;
        } finally {
            metricsService.stopTimer("report.saveToObjectStorage");
            span.end();
        }
    }

    private TripReportItem calculateTrip(
            Device device, Position startTrip, Position endTrip, double maxSpeed,
            boolean ignoreOdometer) throws StorageException {
        Span span = tracer.spanBuilder("ReportUtils.calculateTrip")
                .setSpanKind(SpanKind.INTERNAL)
                .setAttribute("deviceId", device.getId())
                .setAttribute("startPositionId", startTrip.getId())
                .setAttribute("endPositionId", endTrip.getId())
                .startSpan();
        
        try (Scope scope = span.makeCurrent()) {
            metricsService.startTimer("report.calculateTrip");
            TripReportItem trip = new TripReportItem();

            long tripDuration = endTrip.getFixTime().getTime() - startTrip.getFixTime().getTime();
            long deviceId = startTrip.getDeviceId();
            trip.setDeviceId(deviceId);
            trip.setDeviceName(device.getName());

            trip.setStartPositionId(startTrip.getId());
            trip.setStartLat(startTrip.getLatitude());
            trip.setStartLon(startTrip.getLongitude());
            trip.setStartTime(startTrip.getFixTime());
            String startAddress = startTrip.getAddress();
            if (startAddress == null && geocoder != null && config.getBoolean(Keys.GEOCODER_ON_REQUEST)) {
                startAddress = getGeocoderAddress(startTrip.getLatitude(), startTrip.getLongitude());
            }
            trip.setStartAddress(startAddress);

            trip.setEndPositionId(endTrip.getId());
            trip.setEndLat(endTrip.getLatitude());
            trip.setEndLon(endTrip.getLongitude());
            trip.setEndTime(endTrip.getFixTime());
            String endAddress = endTrip.getAddress();
            if (endAddress == null && geocoder != null && config.getBoolean(Keys.GEOCODER_ON_REQUEST)) {
                endAddress = getGeocoderAddress(endTrip.getLatitude(), endTrip.getLongitude());
            }
            trip.setEndAddress(endAddress);

            trip.setDistance(PositionUtil.calculateDistance(startTrip, endTrip, !ignoreOdometer));
            trip.setDuration(tripDuration);
            if (tripDuration > 0) {
                trip.setAverageSpeed(UnitsConverter.knotsFromMps(trip.getDistance() * 1000 / tripDuration));
            }
            trip.setMaxSpeed(maxSpeed);
            trip.setSpentFuel(calculateFuel(startTrip, endTrip));

            trip.setDriverUniqueId(findDriver(startTrip, endTrip));
            trip.setDriverName(findDriverName(trip.getDriverUniqueId()));

            if (!ignoreOdometer
                    && startTrip.getDouble(Position.KEY_ODOMETER) != 0
                    && endTrip.getDouble(Position.KEY_ODOMETER) != 0) {
                trip.setStartOdometer(startTrip.getDouble(Position.KEY_ODOMETER));
                trip.setEndOdometer(endTrip.getDouble(Position.KEY_ODOMETER));
            } else {
                trip.setStartOdometer(startTrip.getDouble(Position.KEY_TOTAL_DISTANCE));
                trip.setEndOdometer(endTrip.getDouble(Position.KEY_TOTAL_DISTANCE));
            }

            return trip;
        } catch (Exception e) {
            span.recordException(e);
            span.setStatus(io.opentelemetry.api.trace.StatusCode.ERROR, e.getMessage());
            throw e;
        } finally {
            metricsService.stopTimer("report.calculateTrip");
            span.end();
        }
    }
    
    /**
     * Gets address from geocoder with circuit breaker protection.
     * 
     * @param latitude Latitude
     * @param longitude Longitude
     * @return Address or null if geocoder is unavailable
     */
    private String getGeocoderAddress(double latitude, double longitude) {
        if (geocoder == null) {
            return null;
        }
        
        Span span = tracer.spanBuilder("ReportUtils.getGeocoderAddress")
                .setSpanKind(SpanKind.CLIENT)
                .setAttribute("latitude", latitude)
                .setAttribute("longitude", longitude)
                .startSpan();
        
        try (Scope scope = span.makeCurrent()) {
            metricsService.startTimer("report.geocoder");
            return geocoderCircuitBreaker.executeSupplier(() -> geocoder.getAddress(latitude, longitude, null));
        } catch (Exception e) {
            span.recordException(e);
            span.setStatus(io.opentelemetry.api.trace.StatusCode.ERROR, e.getMessage());
            return null; // Fallback to null address if geocoder fails
        } finally {
            metricsService.stopTimer("report.geocoder");
            span.end();
        }
    }

    private StopReportItem calculateStop(
            Device device, Position startStop, Position endStop, boolean ignoreOdometer) {
        Span span = tracer.spanBuilder("ReportUtils.calculateStop")
                .setSpanKind(SpanKind.INTERNAL)
                .setAttribute("deviceId", device.getId())
                .setAttribute("positionId", startStop.getId())
                .startSpan();
        
        try (Scope scope = span.makeCurrent()) {
            metricsService.startTimer("report.calculateStop");
            StopReportItem stop = new StopReportItem();

            long deviceId = startStop.getDeviceId();
            stop.setDeviceId(deviceId);
            stop.setDeviceName(device.getName());

            stop.setPositionId(startStop.getId());
            stop.setLatitude(startStop.getLatitude());
            stop.setLongitude(startStop.getLongitude());
            stop.setStartTime(startStop.getFixTime());
            String address = startStop.getAddress();
            if (address == null && geocoder != null && config.getBoolean(Keys.GEOCODER_ON_REQUEST)) {
                address = getGeocoderAddress(stop.getLatitude(), stop.getLongitude());
            }
            stop.setAddress(address);

            stop.setEndTime(endStop.getFixTime());

            long stopDuration = endStop.getFixTime().getTime() - startStop.getFixTime().getTime();
            stop.setDuration(stopDuration);
            stop.setSpentFuel(calculateFuel(startStop, endStop));

            if (startStop.hasAttribute(Position.KEY_HOURS) && endStop.hasAttribute(Position.KEY_HOURS)) {
                stop.setEngineHours(endStop.getLong(Position.KEY_HOURS) - startStop.getLong(Position.KEY_HOURS));
            }

            if (!ignoreOdometer
                    && startStop.getDouble(Position.KEY_ODOMETER) != 0
                    && endStop.getDouble(Position.KEY_ODOMETER) != 0) {
                stop.setStartOdometer(startStop.getDouble(Position.KEY_ODOMETER));
                stop.setEndOdometer(endStop.getDouble(Position.KEY_ODOMETER));
            } else {
                stop.setStartOdometer(startStop.getDouble(Position.KEY_TOTAL_DISTANCE));
                stop.setEndOdometer(endStop.getDouble(Position.KEY_TOTAL_DISTANCE));
            }

            return stop;
        } catch (Exception e) {
            span.recordException(e);
            span.setStatus(io.opentelemetry.api.trace.StatusCode.ERROR, e.getMessage());
            throw e;
        } finally {
            metricsService.stopTimer("report.calculateStop");
            span.end();
        }
    }

    @SuppressWarnings("unchecked")
    private <T extends BaseReportItem> T calculateTripOrStop(
            Device device, Position startPosition, Position endPosition, double maxSpeed,
            boolean ignoreOdometer, Class<T> reportClass) throws StorageException {

        if (reportClass.equals(TripReportItem.class)) {
            return (T) calculateTrip(device, startPosition, endPosition, maxSpeed, ignoreOdometer);
        } else {
            return (T) calculateStop(device, startPosition, endPosition, ignoreOdometer);
        }
    }

    private boolean isMoving(List<Position> positions, int index, TripsConfig tripsConfig) {
        if (tripsConfig.getMinimalNoDataDuration() > 0) {
            boolean beforeGap = index < positions.size() - 1
                    && positions.get(index + 1).getFixTime().getTime() - positions.get(index).getFixTime().getTime()
                    >= tripsConfig.getMinimalNoDataDuration();
            boolean afterGap = index > 0
                    && positions.get(index).getFixTime().getTime() - positions.get(index - 1).getFixTime().getTime()
                    >= tripsConfig.getMinimalNoDataDuration();
            if (beforeGap || afterGap) {
                return false;
            }
        }
        return positions.get(index).getBoolean(Position.KEY_MOTION);
    }

    /**
     * Detects trips and stops in position data.
     * This method now supports parallel processing for large datasets.
     * 
     * @param <T> Type of report item
     * @param device Device
     * @param from Start date
     * @param to End date
     * @param reportClass Report item class
     * @return List of detected trips or stops
     * @throws StorageException If storage operation fails
     */
    public <T extends BaseReportItem> List<T> detectTripsAndStops(
            Device device, Date from, Date to, Class<T> reportClass) throws StorageException {
        Span span = tracer.spanBuilder("ReportUtils.detectTripsAndStops")
                .setSpanKind(SpanKind.INTERNAL)
                .setAttribute("deviceId", device.getId())
                .setAttribute("from", from.getTime())
                .setAttribute("to", to.getTime())
                .setAttribute("reportType", reportClass.getSimpleName())
                .startSpan();
        
        try (Scope scope = span.makeCurrent()) {
            metricsService.startTimer("report.detectTripsAndStops");
            long threshold = config.getLong(Keys.REPORT_FAST_THRESHOLD);
            boolean useParallel = config.getBoolean(Keys.REPORT_USE_PARALLEL_PROCESSING, false);
            span.setAttribute("useParallel", useParallel);
            
            if (Duration.between(from.toInstant(), to.toInstant()).toSeconds() > threshold) {
                return fastTripsAndStops(device, from, to, reportClass);
            } else {
                return slowTripsAndStops(device, from, to, reportClass);
            }
        } catch (Exception e) {
            span.recordException(e);
            span.setStatus(io.opentelemetry.api.trace.StatusCode.ERROR, e.getMessage());
            throw e;
        } finally {
            metricsService.stopTimer("report.detectTripsAndStops");
            span.end();
        }
    }

    /**
     * Slow algorithm for trip/stop detection.
     * 
     * @param <T> Type of report item
     * @param device Device
     * @param from Start date
     * @param to End date
     * @param reportClass Report item class
     * @return List of detected trips or stops
     * @throws StorageException If storage operation fails
     */
    public <T extends BaseReportItem> List<T> slowTripsAndStops(
            Device device, Date from, Date to, Class<T> reportClass) throws StorageException {
        Span span = tracer.spanBuilder("ReportUtils.slowTripsAndStops")
                .setSpanKind(SpanKind.INTERNAL)
                .setAttribute("deviceId", device.getId())
                .setAttribute("reportType", reportClass.getSimpleName())
                .startSpan();
        
        try (Scope scope = span.makeCurrent()) {
            metricsService.startTimer("report.slowTripsAndStops");
            List<T> result = new ArrayList<>();
            TripsConfig tripsConfig = new TripsConfig(
                    new AttributeUtil.StorageProvider(config, storage, permissionsService, device));
            boolean ignoreOdometer = tripsConfig.getIgnoreOdometer();
            boolean useParallel = config.getBoolean(Keys.REPORT_USE_PARALLEL_PROCESSING, false);
            span.setAttribute("useParallel", useParallel);

            var positions = PositionUtil.getPositions(storage, device.getId(), from, to);
            span.setAttribute("positionsCount", positions.size());
            
            if (!positions.isEmpty()) {
                boolean trips = reportClass.equals(TripReportItem.class);

                MotionState motionState = new MotionState();
                boolean initialValue = isMoving(positions, 0, tripsConfig);
                motionState.setMotionStreak(initialValue);
                motionState.setMotionState(initialValue);

                boolean detected = trips == motionState.getMotionState();
                double maxSpeed = 0;
                int startEventIndex = detected ? 0 : -1;
                int startNoEventIndex = -1;
                for (int i = 0; i < positions.size(); i++) {
                    boolean motion = isMoving(positions, i, tripsConfig);
                    if (motionState.getMotionState() != motion) {
                        if (motion == trips) {
                            if (!detected) {
                                startEventIndex = i;
                                maxSpeed = positions.get(i).getSpeed();
                            }
                            startNoEventIndex = -1;
                        } else {
                            startNoEventIndex = i;
                        }
                    } else {
                        maxSpeed = Math.max(maxSpeed, positions.get(i).getSpeed());
                    }

                    MotionProcessor.updateState(motionState, positions.get(i), motion, tripsConfig);
                    if (motionState.getEvent() != null) {
                        if (motion == trips) {
                            detected = true;
                            startNoEventIndex = -1;
                        } else if (startEventIndex >= 0 && startNoEventIndex >= 0) {
                            result.add(calculateTripOrStop(
                                    device, positions.get(startEventIndex), positions.get(startNoEventIndex),
                                    maxSpeed, ignoreOdometer, reportClass));
                            detected = false;
                            startEventIndex = -1;
                            startNoEventIndex = -1;
                        }
                    }
                }
                if (detected & startEventIndex >= 0 && startEventIndex < positions.size() - 1) {
                    int endIndex = startNoEventIndex >= 0 ? startNoEventIndex : positions.size() - 1;
                    result.add(calculateTripOrStop(
                            device, positions.get(startEventIndex), positions.get(endIndex),
                            maxSpeed, ignoreOdometer, reportClass));
                }
            }

            return result;
        } catch (Exception e) {
            span.recordException(e);
            span.setStatus(io.opentelemetry.api.trace.StatusCode.ERROR, e.getMessage());
            throw e;
        } finally {
            metricsService.stopTimer("report.slowTripsAndStops");
            span.end();
        }
    }

    /**
     * Fast algorithm for trip/stop detection.
     * 
     * @param <T> Type of report item
     * @param device Device
     * @param from Start date
     * @param to End date
     * @param reportClass Report item class
     * @return List of detected trips or stops
     * @throws StorageException If storage operation fails
     */
    public <T extends BaseReportItem> List<T> fastTripsAndStops(
            Device device, Date from, Date to, Class<T> reportClass) throws StorageException {
        Span span = tracer.spanBuilder("ReportUtils.fastTripsAndStops")
                .setSpanKind(SpanKind.INTERNAL)
                .setAttribute("deviceId", device.getId())
                .setAttribute("reportType", reportClass.getSimpleName())
                .startSpan();
        
        try (Scope scope = span.makeCurrent()) {
            metricsService.startTimer("report.fastTripsAndStops");
            List<T> result = new ArrayList<>();
            TripsConfig tripsConfig = new TripsConfig(
                    new AttributeUtil.StorageProvider(config, storage, permissionsService, device));
            boolean ignoreOdometer = tripsConfig.getIgnoreOdometer();
            boolean trips = reportClass.equals(TripReportItem.class);
            Set<String> filter = Set.of(Event.TYPE_DEVICE_MOVING, Event.TYPE_DEVICE_STOPPED);
            boolean useParallel = config.getBoolean(Keys.REPORT_USE_PARALLEL_PROCESSING, false);
            span.setAttribute("useParallel", useParallel);

            var events = storageCircuitBreaker.executeSupplier(() -> {
                try {
                    return storage.getObjects(Event.class, new Request(
                            new Columns.All(),
                            new Condition.And(
                                    new Condition.Equals("deviceId", device.getId()),
                                    new Condition.Between("eventTime", "from", from, "to", to)),
                            new Order("eventTime")));
                } catch (StorageException e) {
                    span.recordException(e);
                    throw e;
                }
            });
            
            var filteredEvents = events.stream()
                    .filter(event -> filter.contains(event.getType()))
                    .collect(Collectors.toList());
            span.setAttribute("eventsCount", filteredEvents.size());

            Event startEvent = null;
            
            // Process events sequentially or in parallel based on configuration
            if (useParallel && filteredEvents.size() > 10) { // Only use parallel for larger datasets
                List<CompletableFuture<T>> futures = new ArrayList<>();
                
                for (int i = 0; i < filteredEvents.size(); i++) {
                    Event event = filteredEvents.get(i);
                    boolean motion = event.getType().equals(Event.TYPE_DEVICE_MOVING);
                    
                    if (motion == trips) {
                        startEvent = event;
                    } else if (startEvent != null) {
                        Event finalStartEvent = startEvent;
                        Event finalEndEvent = event;
                        
                        futures.add(CompletableFuture.supplyAsync(() -> {
                            try {
                                Position startPosition = getPositionById(finalStartEvent.getPositionId());
                                Position endPosition = getPositionById(finalEndEvent.getPositionId());
                                
                                if (startPosition != null && endPosition != null) {
                                    return calculateTripOrStop(
                                            device, startPosition, endPosition, 0, ignoreOdometer, reportClass);
                                }
                            } catch (Exception e) {
                                // Log and continue with other events
                                span.recordException(e);
                            }
                            return null;
                        }, executorService));
                        
                        startEvent = null;
                    }
                }
                
                // Collect results from futures
                CompletableFuture.allOf(futures.toArray(new CompletableFuture[0])).join();
                for (CompletableFuture<T> future : futures) {
                    try {
                        T item = future.get();
                        if (item != null) {
                            result.add(item);
                        }
                    } catch (Exception e) {
                        span.recordException(e);
                    }
                }
            } else {
                // Sequential processing
                for (Event event : filteredEvents) {
                    boolean motion = event.getType().equals(Event.TYPE_DEVICE_MOVING);
                    if (motion == trips) {
                        startEvent = event;
                    } else if (startEvent != null) {
                        Position startPosition = getPositionById(startEvent.getPositionId());
                        Position endPosition = getPositionById(event.getPositionId());
                        if (startPosition != null && endPosition != null) {
                            result.add(calculateTripOrStop(
                                    device, startPosition, endPosition, 0, ignoreOdometer, reportClass));
                        }
                        startEvent = null;
                    }
                }
            }

            return result;
        } catch (Exception e) {
            span.recordException(e);
            span.setStatus(io.opentelemetry.api.trace.StatusCode.ERROR, e.getMessage());
            throw e;
        } finally {
            metricsService.stopTimer("report.fastTripsAndStops");
            span.end();
        }
    }
    
    /**
     * Gets position by ID with circuit breaker protection.
     * 
     * @param positionId Position ID
     * @return Position or null if not found
     * @throws StorageException If storage operation fails
     */
    private Position getPositionById(long positionId) throws StorageException {
        Span span = tracer.spanBuilder("ReportUtils.getPositionById")
                .setSpanKind(SpanKind.CLIENT)
                .setAttribute("positionId", positionId)
                .startSpan();
        
        try (Scope scope = span.makeCurrent()) {
            metricsService.startTimer("report.getPositionById");
            return storageCircuitBreaker.executeSupplier(() -> {
                try {
                    return storage.getObject(Position.class, new Request(
                            new Columns.All(), new Condition.Equals("id", positionId)));
                } catch (StorageException e) {
                    span.recordException(e);
                    throw e;
                }
            });
        } catch (Exception e) {
            span.recordException(e);
            span.setStatus(io.opentelemetry.api.trace.StatusCode.ERROR, e.getMessage());
            throw e;
        } finally {
            metricsService.stopTimer("report.getPositionById");
            span.end();
        }
    }
}