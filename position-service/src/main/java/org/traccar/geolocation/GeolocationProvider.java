/*
 * Copyright 2015 - 2016 Anton Tananaev (anton@traccar.org)
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
package org.traccar.geolocation;

import org.traccar.model.Network;

/**
 * Interface for geolocation providers that can determine a device's location
 * based on network information when GPS coordinates are unavailable.
 */
public interface GeolocationProvider {

    /**
     * Callback interface for asynchronous geolocation operations.
     */
    interface LocationProviderCallback {

        /**
         * Called when geolocation is successful.
         *
         * @param latitude  the determined latitude
         * @param longitude the determined longitude
         * @param accuracy  the accuracy of the location in meters
         */
        void onSuccess(double latitude, double longitude, double accuracy);

        /**
         * Called when geolocation fails.
         *
         * @param e the exception or error that caused the failure
         */
        void onFailure(Throwable e);

    }

    /**
     * Get location based on network information.
     *
     * @param network  the network information to use for geolocation
     * @param callback the callback to receive the result
     */
    void getLocation(Network network, LocationProviderCallback callback);

}