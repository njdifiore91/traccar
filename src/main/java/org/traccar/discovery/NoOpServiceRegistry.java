/*
 * Copyright 2022 - 2023 Anton Tananaev (anton@traccar.org)
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
package org.traccar.discovery;

import java.util.logging.Logger;

/**
 * No-op implementation of ServiceRegistry that doesn't actually register services.
 * Used when service discovery is disabled or not configured.
 */
public class NoOpServiceRegistry implements ServiceRegistry {

    private static final Logger LOGGER = Logger.getLogger(NoOpServiceRegistry.class.getName());

    public NoOpServiceRegistry() {
        LOGGER.info("Initialized NoOpServiceRegistry (service discovery disabled)");
    }

    @Override
    public void register(ServiceRegistration registration) {
        // Do nothing
        LOGGER.fine("Service registration ignored (service discovery disabled): " + registration.getId());
    }

    @Override
    public void deregister(ServiceRegistration registration) {
        // Do nothing
        LOGGER.fine("Service deregistration ignored (service discovery disabled): " + registration.getId());
    }
}