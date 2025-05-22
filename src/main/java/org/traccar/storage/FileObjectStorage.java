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

import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Timer;
import io.opentelemetry.api.trace.Span;
import io.opentelemetry.api.trace.SpanKind;
import io.opentelemetry.api.trace.StatusCode;
import io.opentelemetry.api.trace.Tracer;
import io.opentelemetry.context.Scope;
import jakarta.inject.Inject;
import jakarta.inject.Singleton;
import org.traccar.config.Config;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;

/**
 * Implementation of ObjectStorage using local filesystem.
 */
@Singleton
public class FileObjectStorage implements ObjectStorage {

    private final Path basePath;
    private final Tracer tracer;
    private final MeterRegistry meterRegistry;

    /**
     * Constructor with all required dependencies.
     *
     * @param config Configuration
     * @param tracer OpenTelemetry tracer
     * @param meterRegistry Metrics registry
     */
    @Inject
    public FileObjectStorage(Config config, Tracer tracer, MeterRegistry meterRegistry) {
        this.tracer = tracer;
        this.meterRegistry = meterRegistry;

        // Get base path from configuration
        String configPath = config.getString("storage.file.path", "storage");
        this.basePath = Paths.get(configPath).toAbsolutePath();

        // Create base directory if it doesn't exist
        try {
            Files.createDirectories(basePath);
        } catch (IOException e) {
            throw new RuntimeException("Failed to create storage directory: " + basePath, e);
        }
    }

    private Path getFullPath(String path) {
        return basePath.resolve(path);
    }

    @Override
    public boolean exists(String path) throws IOException {
        Span span = tracer.spanBuilder("FileObjectStorage.exists")
                .setSpanKind(SpanKind.INTERNAL)
                .startSpan();
        try (Scope scope = span.makeCurrent()) {
            span.setAttribute("path", path);
            Timer.Sample sample = Timer.start(meterRegistry);

            Path fullPath = getFullPath(path);
            boolean result = Files.exists(fullPath);

            sample.stop(meterRegistry.timer("storage.file.exists"));
            return result;
        } catch (Exception e) {
            span.recordException(e);
            span.setStatus(StatusCode.ERROR);
            meterRegistry.counter("storage.file.failures", "operation", "exists").increment();
            throw new IOException("Failed to check if file exists: " + path, e);
        } finally {
            span.end();
        }
    }

    @Override
    public InputStream getObject(String path) throws IOException {
        Span span = tracer.spanBuilder("FileObjectStorage.getObject")
                .setSpanKind(SpanKind.INTERNAL)
                .startSpan();
        try (Scope scope = span.makeCurrent()) {
            span.setAttribute("path", path);
            Timer.Sample sample = Timer.start(meterRegistry);

            Path fullPath = getFullPath(path);
            if (!Files.exists(fullPath)) {
                throw new IOException("File does not exist: " + fullPath);
            }

            // Copy to ByteArrayInputStream to avoid keeping the file handle open
            try (FileInputStream fis = new FileInputStream(fullPath.toFile())) {
                ByteArrayOutputStream baos = new ByteArrayOutputStream();
                byte[] buffer = new byte[4096];
                int bytesRead;
                while ((bytesRead = fis.read(buffer)) != -1) {
                    baos.write(buffer, 0, bytesRead);
                }

                sample.stop(meterRegistry.timer("storage.file.getObject"));
                return new ByteArrayInputStream(baos.toByteArray());
            }
        } catch (Exception e) {
            span.recordException(e);
            span.setStatus(StatusCode.ERROR);
            meterRegistry.counter("storage.file.failures", "operation", "getObject").increment();
            throw new IOException("Failed to get file: " + path, e);
        } finally {
            span.end();
        }
    }

    @Override
    public void putObject(String path, OutputStream outputStream) throws IOException {
        Span span = tracer.spanBuilder("FileObjectStorage.putObject")
                .setSpanKind(SpanKind.INTERNAL)
                .startSpan();
        try (Scope scope = span.makeCurrent()) {
            span.setAttribute("path", path);
            Timer.Sample sample = Timer.start(meterRegistry);

            Path fullPath = getFullPath(path);
            Files.createDirectories(fullPath.getParent());

            // Convert OutputStream to byte array for file write
            ByteArrayOutputStream baos;
            if (outputStream instanceof ByteArrayOutputStream) {
                baos = (ByteArrayOutputStream) outputStream;
            } else {
                throw new IOException("OutputStream must be a ByteArrayOutputStream");
            }

            byte[] bytes = baos.toByteArray();
            span.setAttribute("contentLength", bytes.length);

            try (FileOutputStream fos = new FileOutputStream(fullPath.toFile())) {
                fos.write(bytes);
            }

            sample.stop(meterRegistry.timer("storage.file.putObject"));
        } catch (Exception e) {
            span.recordException(e);
            span.setStatus(StatusCode.ERROR);
            meterRegistry.counter("storage.file.failures", "operation", "putObject").increment();
            throw new IOException("Failed to put file: " + path, e);
        } finally {
            span.end();
        }
    }

    @Override
    public void deleteObject(String path) throws IOException {
        Span span = tracer.spanBuilder("FileObjectStorage.deleteObject")
                .setSpanKind(SpanKind.INTERNAL)
                .startSpan();
        try (Scope scope = span.makeCurrent()) {
            span.setAttribute("path", path);
            Timer.Sample sample = Timer.start(meterRegistry);

            Path fullPath = getFullPath(path);
            if (Files.exists(fullPath)) {
                Files.delete(fullPath);
            }

            sample.stop(meterRegistry.timer("storage.file.deleteObject"));
        } catch (Exception e) {
            span.recordException(e);
            span.setStatus(StatusCode.ERROR);
            meterRegistry.counter("storage.file.failures", "operation", "deleteObject").increment();
            throw new IOException("Failed to delete file: " + path, e);
        } finally {
            span.end();
        }
    }
}