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
package org.traccar.storage;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;

/**
 * Interface for object storage operations.
 * Implementations can use S3-compatible storage, local filesystem, or other storage backends.
 */
public interface ObjectStorage {

    /**
     * Check if an object exists in storage.
     *
     * @param path Object path
     * @return True if object exists, false otherwise
     * @throws IOException If I/O operation fails
     */
    boolean exists(String path) throws IOException;

    /**
     * Get an object from storage.
     *
     * @param path Object path
     * @return Object input stream
     * @throws IOException If I/O operation fails
     */
    InputStream getObject(String path) throws IOException;

    /**
     * Put an object to storage.
     *
     * @param path Object path
     * @param outputStream Object output stream
     * @throws IOException If I/O operation fails
     */
    void putObject(String path, OutputStream outputStream) throws IOException;

    /**
     * Delete an object from storage.
     *
     * @param path Object path
     * @throws IOException If I/O operation fails
     */
    void deleteObject(String path) throws IOException;

}