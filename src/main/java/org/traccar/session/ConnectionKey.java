/*
 * Copyright 2024 Anton Tananaev (anton@traccar.org)
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
package org.traccar.session;

import io.netty.channel.Channel;

import java.io.Serializable;
import java.net.SocketAddress;

/**
 * A key used to uniquely identify a connection in the session management system.
 * This class is serializable to support distributed session storage in Redis.
 */
public record ConnectionKey(SocketAddress localAddress, SocketAddress remoteAddress) implements Serializable {
    
    /**
     * Serial version UID for serialization compatibility across different versions.
     */
    private static final long serialVersionUID = 1L;
    
    /**
     * Creates a ConnectionKey from a Channel and remote address.
     *
     * @param channel The Netty channel
     * @param remoteAddress The remote socket address
     */
    public ConnectionKey(Channel channel, SocketAddress remoteAddress) {
        this(channel.localAddress(), remoteAddress);
    }
    
    /**
     * Returns a string representation of this ConnectionKey.
     * This implementation provides detailed connection information for logging and debugging.
     *
     * @return A string representation of this ConnectionKey
     */
    @Override
    public String toString() {
        return "ConnectionKey[local=" + localAddress + ", remote=" + remoteAddress + "]";
    }
    
    /**
     * Compares this ConnectionKey with another object for equality.
     * This implementation ensures proper comparison in distributed environments.
     *
     * @param o The object to compare with
     * @return true if the objects are equal, false otherwise
     */
    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        
        ConnectionKey that = (ConnectionKey) o;
        
        if (localAddress != null ? !localAddress.equals(that.localAddress) : that.localAddress != null) return false;
        return remoteAddress != null ? remoteAddress.equals(that.remoteAddress) : that.remoteAddress == null;
    }
    
    /**
     * Returns a hash code for this ConnectionKey.
     * This implementation ensures consistent hashing in distributed environments.
     *
     * @return A hash code value for this ConnectionKey
     */
    @Override
    public int hashCode() {
        int result = localAddress != null ? localAddress.hashCode() : 0;
        result = 31 * result + (remoteAddress != null ? remoteAddress.hashCode() : 0);
        return result;
    }
}