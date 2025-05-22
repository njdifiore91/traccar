/*
 * Copyright 2013 - 2024 Anton Tananaev (anton@traccar.org)
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

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;

import org.apache.commons.lang3.builder.EqualsBuilder;
import org.traccar.storage.QueryIgnore;
import org.traccar.helper.Hashing;
import org.traccar.storage.StorageName;

import jakarta.validation.constraints.Email;
import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.Size;

import java.util.Date;
import java.util.HashMap;
import java.util.Objects;

/**
 * User entity class with validation and serialization annotations for microservices architecture.
 */
@StorageName("tc_users")
@JsonInclude(JsonInclude.Include.NON_NULL)
public class User extends ExtendedModel implements UserRestrictions, Disableable {

    @NotBlank(message = "Name cannot be empty")
    @Size(max = 128, message = "Name cannot exceed 128 characters")
    private String name;

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    @NotBlank(message = "Login cannot be empty")
    @Size(min = 3, max = 128, message = "Login must be between 3 and 128 characters")
    private String login;

    public String getLogin() {
        return login;
    }

    public void setLogin(String login) {
        this.login = login;
    }

    @Email(message = "Email must be valid")
    @Size(max = 128, message = "Email cannot exceed 128 characters")
    private String email;

    public String getEmail() {
        return email;
    }

    public void setEmail(String email) {
        this.email = email != null ? email.trim() : null;
    }

    @Size(max = 32, message = "Phone number cannot exceed 32 characters")
    private String phone;

    public String getPhone() {
        return phone;
    }

    public void setPhone(String phone) {
        this.phone = phone != null ? phone.trim() : null;
    }

    private boolean readonly;

    @Override
    public boolean getReadonly() {
        return readonly;
    }

    public void setReadonly(boolean readonly) {
        this.readonly = readonly;
    }

    private boolean administrator;

    @QueryIgnore
    @JsonIgnore
    public boolean getManager() {
        return userLimit != 0;
    }

    public boolean getAdministrator() {
        return administrator;
    }

    public void setAdministrator(boolean administrator) {
        this.administrator = administrator;
    }

    private String map;

    public String getMap() {
        return map;
    }

    public void setMap(String map) {
        this.map = map;
    }

    private double latitude;

    public double getLatitude() {
        return latitude;
    }

    public void setLatitude(double latitude) {
        this.latitude = latitude;
    }

    private double longitude;

    public double getLongitude() {
        return longitude;
    }

    public void setLongitude(double longitude) {
        this.longitude = longitude;
    }

    private int zoom;

    public int getZoom() {
        return zoom;
    }

    public void setZoom(int zoom) {
        this.zoom = zoom;
    }

    private String coordinateFormat;

    public String getCoordinateFormat() {
        return coordinateFormat;
    }

    public void setCoordinateFormat(String coordinateFormat) {
        this.coordinateFormat = coordinateFormat;
    }

    private boolean disabled;

    @Override
    public boolean getDisabled() {
        return disabled;
    }

    @Override
    public void setDisabled(boolean disabled) {
        this.disabled = disabled;
    }

    private Date expirationTime;

    @Override
    public Date getExpirationTime() {
        return expirationTime;
    }

    @Override
    public void setExpirationTime(Date expirationTime) {
        this.expirationTime = expirationTime;
    }

    private int deviceLimit;

    public int getDeviceLimit() {
        return deviceLimit;
    }

    public void setDeviceLimit(int deviceLimit) {
        this.deviceLimit = deviceLimit;
    }

    private int userLimit;

    public int getUserLimit() {
        return userLimit;
    }

    public void setUserLimit(int userLimit) {
        this.userLimit = userLimit;
    }

    private boolean deviceReadonly;

    @Override
    public boolean getDeviceReadonly() {
        return deviceReadonly;
    }

    public void setDeviceReadonly(boolean deviceReadonly) {
        this.deviceReadonly = deviceReadonly;
    }

    private boolean limitCommands;

    @Override
    public boolean getLimitCommands() {
        return limitCommands;
    }

    public void setLimitCommands(boolean limitCommands) {
        this.limitCommands = limitCommands;
    }

    private boolean disableReports;

    @Override
    public boolean getDisableReports() {
        return disableReports;
    }

    public void setDisableReports(boolean disableReports) {
        this.disableReports = disableReports;
    }

    private boolean fixedEmail;

    @Override
    public boolean getFixedEmail() {
        return fixedEmail;
    }

    public void setFixedEmail(boolean fixedEmail) {
        this.fixedEmail = fixedEmail;
    }

    private String poiLayer;

    public String getPoiLayer() {
        return poiLayer;
    }

    public void setPoiLayer(String poiLayer) {
        this.poiLayer = poiLayer;
    }

    private String totpKey;

    @JsonIgnore
    public String getTotpKey() {
        return totpKey;
    }

    public void setTotpKey(String totpKey) {
        this.totpKey = totpKey;
    }

    private boolean temporary;

    public boolean getTemporary() {
        return temporary;
    }

    public void setTemporary(boolean temporary) {
        this.temporary = temporary;
    }

    /**
     * Password is a write-only field that triggers password hashing when set.
     * It is never serialized or stored directly.
     */
    @QueryIgnore
    @JsonProperty(access = JsonProperty.Access.WRITE_ONLY)
    public String getPassword() {
        return null;
    }

    @QueryIgnore
    public void setPassword(String password) {
        if (password != null && !password.isEmpty()) {
            Hashing.HashingResult hashingResult = Hashing.createHash(password);
            hashedPassword = hashingResult.getHash();
            salt = hashingResult.getSalt();
        }
    }

    private String hashedPassword;

    @JsonIgnore
    @QueryIgnore
    public String getHashedPassword() {
        return hashedPassword;
    }

    @QueryIgnore
    public void setHashedPassword(String hashedPassword) {
        this.hashedPassword = hashedPassword;
    }

    private String salt;

    @JsonIgnore
    @QueryIgnore
    public String getSalt() {
        return salt;
    }

    @QueryIgnore
    public void setSalt(String salt) {
        this.salt = salt;
    }

    /**
     * Validates if the provided password matches the stored hashed password.
     * 
     * @param password The password to validate
     * @return true if the password is valid, false otherwise
     */
    public boolean isPasswordValid(String password) {
        return Hashing.validatePassword(password, hashedPassword, salt);
    }

    /**
     * Compares this user with another user, excluding specified attributes.
     * 
     * @param other The other user to compare with
     * @param exclusions Attributes to exclude from comparison
     * @return true if the users are equal (excluding specified attributes), false otherwise
     */
    public boolean compare(User other, String... exclusions) {
        if (!EqualsBuilder.reflectionEquals(this, other, "attributes", "hashedPassword", "salt")) {
            return false;
        }
        var thisAttributes = new HashMap<>(getAttributes());
        var otherAttributes = new HashMap<>(other.getAttributes());
        for (String exclusion : exclusions) {
            thisAttributes.remove(exclusion);
            otherAttributes.remove(exclusion);
        }
        return thisAttributes.equals(otherAttributes);
    }

    /**
     * Merges partial updates from another user object.
     * Only non-null fields from the source user will be applied to this user.
     * Sensitive fields like password, hashedPassword, and salt are not merged.
     * 
     * @param source The source user containing updates
     * @return this user instance for method chaining
     */
    public User mergePartialUpdate(User source) {
        if (source == null) {
            return this;
        }
        
        if (source.name != null) {
            this.name = source.name;
        }
        if (source.login != null) {
            this.login = source.login;
        }
        if (source.email != null) {
            this.email = source.email;
        }
        if (source.phone != null) {
            this.phone = source.phone;
        }
        
        // Boolean fields and primitive types need special handling
        // We only update if the source object has explicitly set these fields
        if (source.getAttributes().containsKey("readonly")) {
            this.readonly = source.readonly;
        }
        if (source.getAttributes().containsKey("administrator")) {
            this.administrator = source.administrator;
        }
        if (source.map != null) {
            this.map = source.map;
        }
        if (source.getAttributes().containsKey("latitude")) {
            this.latitude = source.latitude;
        }
        if (source.getAttributes().containsKey("longitude")) {
            this.longitude = source.longitude;
        }
        if (source.getAttributes().containsKey("zoom")) {
            this.zoom = source.zoom;
        }
        if (source.coordinateFormat != null) {
            this.coordinateFormat = source.coordinateFormat;
        }
        if (source.getAttributes().containsKey("disabled")) {
            this.disabled = source.disabled;
        }
        if (source.expirationTime != null) {
            this.expirationTime = source.expirationTime;
        }
        if (source.getAttributes().containsKey("deviceLimit")) {
            this.deviceLimit = source.deviceLimit;
        }
        if (source.getAttributes().containsKey("userLimit")) {
            this.userLimit = source.userLimit;
        }
        if (source.getAttributes().containsKey("deviceReadonly")) {
            this.deviceReadonly = source.deviceReadonly;
        }
        if (source.getAttributes().containsKey("limitCommands")) {
            this.limitCommands = source.limitCommands;
        }
        if (source.getAttributes().containsKey("disableReports")) {
            this.disableReports = source.disableReports;
        }
        if (source.getAttributes().containsKey("fixedEmail")) {
            this.fixedEmail = source.fixedEmail;
        }
        if (source.poiLayer != null) {
            this.poiLayer = source.poiLayer;
        }
        if (source.getAttributes().containsKey("temporary")) {
            this.temporary = source.temporary;
        }
        
        // Merge attributes but don't overwrite existing ones
        if (source.getAttributes() != null && !source.getAttributes().isEmpty()) {
            source.getAttributes().forEach((key, value) -> {
                if (value != null) {
                    this.getAttributes().put(key, value);
                }
            });
        }
        
        return this;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        if (!super.equals(o)) return false;
        User user = (User) o;
        return readonly == user.readonly &&
                administrator == user.administrator &&
                Double.compare(user.latitude, latitude) == 0 &&
                Double.compare(user.longitude, longitude) == 0 &&
                zoom == user.zoom &&
                disabled == user.disabled &&
                deviceLimit == user.deviceLimit &&
                userLimit == user.userLimit &&
                deviceReadonly == user.deviceReadonly &&
                limitCommands == user.limitCommands &&
                disableReports == user.disableReports &&
                fixedEmail == user.fixedEmail &&
                temporary == user.temporary &&
                Objects.equals(name, user.name) &&
                Objects.equals(login, user.login) &&
                Objects.equals(email, user.email) &&
                Objects.equals(phone, user.phone) &&
                Objects.equals(map, user.map) &&
                Objects.equals(coordinateFormat, user.coordinateFormat) &&
                Objects.equals(expirationTime, user.expirationTime) &&
                Objects.equals(poiLayer, user.poiLayer) &&
                Objects.equals(totpKey, user.totpKey);
    }

    @Override
    public int hashCode() {
        return Objects.hash(super.hashCode(), name, login, email, phone, readonly, administrator,
                map, latitude, longitude, zoom, coordinateFormat, disabled, expirationTime,
                deviceLimit, userLimit, deviceReadonly, limitCommands, disableReports,
                fixedEmail, poiLayer, totpKey, temporary);
    }
}