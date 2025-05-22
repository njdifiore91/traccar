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
package org.traccar.model;

import java.io.Serializable;
import java.util.Objects;

/**
 * Manages versioning information for model schemas to support evolution in a distributed system.
 * This class tracks major, minor, and patch version numbers along with compatibility information
 * to ensure proper serialization and deserialization across different service versions.
 */
public class SchemaVersion implements Serializable, Comparable<SchemaVersion> {

    private static final long serialVersionUID = 1L;

    private final int major;
    private final int minor;
    private final int patch;
    private final String format;
    private final boolean backwardCompatible;
    private final boolean forwardCompatible;

    /**
     * Creates a new SchemaVersion with the specified version components.
     *
     * @param major The major version number (incremented for incompatible changes)
     * @param minor The minor version number (incremented for backward compatible feature additions)
     * @param patch The patch version number (incremented for backward compatible bug fixes)
     * @param format The serialization format identifier (e.g., "json", "protobuf")
     * @param backwardCompatible Whether this version can read data from previous versions
     * @param forwardCompatible Whether this version's data can be read by future versions
     */
    public SchemaVersion(int major, int minor, int patch, String format, 
                        boolean backwardCompatible, boolean forwardCompatible) {
        this.major = major;
        this.minor = minor;
        this.patch = patch;
        this.format = Objects.requireNonNull(format, "Format cannot be null");
        this.backwardCompatible = backwardCompatible;
        this.forwardCompatible = forwardCompatible;
    }

    /**
     * Creates a new SchemaVersion with the specified version components.
     * Assumes backward compatibility but not forward compatibility.
     *
     * @param major The major version number
     * @param minor The minor version number
     * @param patch The patch version number
     * @param format The serialization format identifier
     */
    public SchemaVersion(int major, int minor, int patch, String format) {
        this(major, minor, patch, format, true, false);
    }

    /**
     * Creates a new SchemaVersion from a version string in the format "major.minor.patch".
     *
     * @param versionString The version string (e.g., "1.2.3")
     * @param format The serialization format identifier
     * @param backwardCompatible Whether this version can read data from previous versions
     * @param forwardCompatible Whether this version's data can be read by future versions
     * @throws IllegalArgumentException If the version string is not in the correct format
     */
    public SchemaVersion(String versionString, String format, 
                        boolean backwardCompatible, boolean forwardCompatible) {
        Objects.requireNonNull(versionString, "Version string cannot be null");
        Objects.requireNonNull(format, "Format cannot be null");
        
        String[] parts = versionString.split("\\.");
        if (parts.length != 3) {
            throw new IllegalArgumentException("Version string must be in the format 'major.minor.patch'");
        }
        
        try {
            this.major = Integer.parseInt(parts[0]);
            this.minor = Integer.parseInt(parts[1]);
            this.patch = Integer.parseInt(parts[2]);
        } catch (NumberFormatException e) {
            throw new IllegalArgumentException("Version components must be integers", e);
        }
        
        this.format = format;
        this.backwardCompatible = backwardCompatible;
        this.forwardCompatible = forwardCompatible;
    }

    /**
     * Creates a new SchemaVersion from a version string in the format "major.minor.patch".
     * Assumes backward compatibility but not forward compatibility.
     *
     * @param versionString The version string (e.g., "1.2.3")
     * @param format The serialization format identifier
     * @throws IllegalArgumentException If the version string is not in the correct format
     */
    public SchemaVersion(String versionString, String format) {
        this(versionString, format, true, false);
    }

    /**
     * Gets the major version number.
     *
     * @return The major version number
     */
    public int getMajor() {
        return major;
    }

    /**
     * Gets the minor version number.
     *
     * @return The minor version number
     */
    public int getMinor() {
        return minor;
    }

    /**
     * Gets the patch version number.
     *
     * @return The patch version number
     */
    public int getPatch() {
        return patch;
    }

    /**
     * Gets the serialization format identifier.
     *
     * @return The format identifier
     */
    public String getFormat() {
        return format;
    }

    /**
     * Checks if this version is backward compatible.
     *
     * @return True if this version can read data from previous versions
     */
    public boolean isBackwardCompatible() {
        return backwardCompatible;
    }

    /**
     * Checks if this version is forward compatible.
     *
     * @return True if this version's data can be read by future versions
     */
    public boolean isForwardCompatible() {
        return forwardCompatible;
    }

    /**
     * Checks if this version is compatible with the specified version.
     * Compatibility is determined based on semantic versioning rules and compatibility flags.
     *
     * @param other The version to check compatibility with
     * @return True if the versions are compatible, false otherwise
     */
    public boolean isCompatibleWith(SchemaVersion other) {
        // Different formats are never compatible
        if (!this.format.equals(other.format)) {
            return false;
        }
        
        // Same major version is always compatible
        if (this.major == other.major) {
            return true;
        }
        
        // Check if this version can read older versions
        if (this.major > other.major && this.backwardCompatible) {
            return true;
        }
        
        // Check if this version can be read by newer versions
        if (this.major < other.major && this.forwardCompatible) {
            return true;
        }
        
        return false;
    }

    /**
     * Extracts the schema version from a serialized message header or metadata.
     * This is a placeholder method that should be implemented based on the specific
     * serialization format and message structure used in the application.
     *
     * @param messageHeader The message header or metadata containing version information
     * @return The extracted schema version
     * @throws IllegalArgumentException If the version information cannot be extracted
     */
    public static SchemaVersion extractFromMessageHeader(String messageHeader) {
        // This is a placeholder implementation
        // In a real application, this would parse the message header format
        // and extract the version information
        
        if (messageHeader == null || messageHeader.isEmpty()) {
            throw new IllegalArgumentException("Message header cannot be null or empty");
        }
        
        // Example implementation assuming a header format like "format:version:compatibility"
        // where compatibility is a two-character string with backward and forward flags
        String[] parts = messageHeader.split(":");
        if (parts.length < 3) {
            throw new IllegalArgumentException("Invalid message header format");
        }
        
        String format = parts[0];
        String versionString = parts[1];
        String compatibility = parts.length > 2 ? parts[2] : "10"; // Default: backward compatible only
        
        boolean backwardCompatible = compatibility.length() > 0 && compatibility.charAt(0) == '1';
        boolean forwardCompatible = compatibility.length() > 1 && compatibility.charAt(1) == '1';
        
        return new SchemaVersion(versionString, format, backwardCompatible, forwardCompatible);
    }

    /**
     * Validates that the provided schema version meets the minimum required version.
     *
     * @param version The version to validate
     * @param minimumVersion The minimum required version
     * @throws IllegalArgumentException If the version is below the minimum required version
     */
    public static void validateMinimumVersion(SchemaVersion version, SchemaVersion minimumVersion) {
        if (version.compareTo(minimumVersion) < 0) {
            throw new IllegalArgumentException(String.format(
                "Schema version %s is below the minimum required version %s",
                version.toString(), minimumVersion.toString()));
        }
    }

    /**
     * Compares this version with another version based on semantic versioning rules.
     * Major version has highest precedence, followed by minor and then patch.
     *
     * @param other The version to compare with
     * @return A negative integer, zero, or a positive integer as this version is less than,
     *         equal to, or greater than the specified version
     */
    @Override
    public int compareTo(SchemaVersion other) {
        if (this.major != other.major) {
            return Integer.compare(this.major, other.major);
        }
        if (this.minor != other.minor) {
            return Integer.compare(this.minor, other.minor);
        }
        return Integer.compare(this.patch, other.patch);
    }

    /**
     * Returns a string representation of this version in the format "major.minor.patch".
     *
     * @return The string representation of this version
     */
    @Override
    public String toString() {
        return String.format("%d.%d.%d[%s]", major, minor, patch, format);
    }

    /**
     * Generates a header string for inclusion in serialized messages.
     *
     * @return A header string containing version and compatibility information
     */
    public String toHeaderString() {
        // Format: format:major.minor.patch:compatibility
        // where compatibility is a two-character string with backward and forward flags
        String compatibility = (backwardCompatible ? "1" : "0") + (forwardCompatible ? "1" : "0");
        return String.format("%s:%d.%d.%d:%s", format, major, minor, patch, compatibility);
    }

    /**
     * Checks if this version is equal to another object.
     * Two versions are equal if they have the same major, minor, patch, and format values.
     *
     * @param obj The object to compare with
     * @return True if the objects are equal, false otherwise
     */
    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }
        if (obj == null || getClass() != obj.getClass()) {
            return false;
        }
        SchemaVersion other = (SchemaVersion) obj;
        return major == other.major && 
               minor == other.minor && 
               patch == other.patch && 
               format.equals(other.format);
    }

    /**
     * Generates a hash code for this version.
     *
     * @return The hash code
     */
    @Override
    public int hashCode() {
        return Objects.hash(major, minor, patch, format);
    }
}