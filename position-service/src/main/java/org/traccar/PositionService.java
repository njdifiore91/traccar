package org.traccar;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

/**
 * Service for processing position data.
 * <p>
 * This class demonstrates how to use the TransactionOutboxManager to ensure
 * reliable message publishing when processing position data.
 */
@Service
public class PositionService {

    private static final Logger LOGGER = LoggerFactory.getLogger(PositionService.class);
    private static final String POSITION_TOPIC = "position-updates";

    private final TransactionOutboxManager outboxManager;
    private final ObjectMapper objectMapper;
    private final PositionRepository positionRepository;

    /**
     * Constructs a new PositionService.
     *
     * @param outboxManager      the transaction outbox manager
     * @param objectMapper       the object mapper for JSON serialization
     * @param positionRepository the repository for position data
     */
    @Autowired
    public PositionService(TransactionOutboxManager outboxManager, 
                          ObjectMapper objectMapper,
                          PositionRepository positionRepository) {
        this.outboxManager = outboxManager;
        this.objectMapper = objectMapper;
        this.positionRepository = positionRepository;
    }

    /**
     * Processes a new position update.
     * <p>
     * This method demonstrates how to use the TransactionOutboxManager to ensure
     * reliable message publishing when processing position data. The position is
     * saved to the database and a message is stored in the outbox table as part
     * of the same transaction.
     *
     * @param position the position to process
     * @return the processed position
     */
    @Transactional
    public Position processPosition(Position position) {
        LOGGER.info("Processing position for device {}", position.getDeviceId());
        
        // Save the position to the database
        Position savedPosition = positionRepository.save(position);
        
        try {
            // Convert the position to JSON
            String payload = objectMapper.writeValueAsString(savedPosition);
            
            // Store the message in the outbox table
            // The device ID is used as the message key for partitioning
            String messageId = outboxManager.storeMessage(
                POSITION_TOPIC, 
                String.valueOf(savedPosition.getDeviceId()), 
                payload
            );
            
            LOGGER.debug("Stored position update message in outbox: id={}", messageId);
        } catch (Exception e) {
            LOGGER.error("Failed to store position update message in outbox", e);
            // The transaction will be rolled back if an exception is thrown
            throw new RuntimeException("Failed to process position", e);
        }
        
        return savedPosition;
    }

    /**
     * Placeholder for the Position class.
     * In a real implementation, this would be a proper entity class.
     */
    public static class Position {
        private long id;
        private long deviceId;
        private double latitude;
        private double longitude;
        private double altitude;
        private double speed;
        private double course;
        private long time;

        // Getters and setters
        public long getId() { return id; }
        public void setId(long id) { this.id = id; }
        public long getDeviceId() { return deviceId; }
        public void setDeviceId(long deviceId) { this.deviceId = deviceId; }
        public double getLatitude() { return latitude; }
        public void setLatitude(double latitude) { this.latitude = latitude; }
        public double getLongitude() { return longitude; }
        public void setLongitude(double longitude) { this.longitude = longitude; }
        public double getAltitude() { return altitude; }
        public void setAltitude(double altitude) { this.altitude = altitude; }
        public double getSpeed() { return speed; }
        public void setSpeed(double speed) { this.speed = speed; }
        public double getCourse() { return course; }
        public void setCourse(double course) { this.course = course; }
        public long getTime() { return time; }
        public void setTime(long time) { this.time = time; }
    }

    /**
     * Placeholder for the PositionRepository interface.
     * In a real implementation, this would be a proper Spring Data repository.
     */
    public interface PositionRepository {
        Position save(Position position);
    }
}