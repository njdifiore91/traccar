package org.traccar.template;

import org.apache.velocity.Template;
import org.apache.velocity.app.VelocityEngine;
import org.apache.velocity.exception.ParseErrorException;
import org.apache.velocity.exception.ResourceNotFoundException;
import org.apache.velocity.runtime.RuntimeConstants;
import org.apache.velocity.runtime.resource.loader.ClasspathResourceLoader;
import org.apache.velocity.runtime.resource.loader.FileResourceLoader;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.actuate.health.Health;
import org.springframework.boot.actuate.health.HealthIndicator;
import org.springframework.core.io.Resource;
import org.springframework.core.io.ResourceLoader;
import org.springframework.stereotype.Component;

import io.github.resilience4j.circuitbreaker.annotation.CircuitBreaker;

import javax.annotation.PostConstruct;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.StringWriter;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Handles the loading of notification templates from various sources including filesystem,
 * classpath resources, and potentially database storage. It provides a unified interface for
 * template retrieval regardless of the underlying storage mechanism, with support for template
 * versioning and hot reloading.
 */
@Component
public class TemplateLoader implements HealthIndicator {

    private static final Logger logger = LoggerFactory.getLogger(TemplateLoader.class);
    
    private final VelocityEngine velocityEngine;
    private final ResourceLoader resourceLoader;
    private final Map<String, TemplateEntry> templateCache = new ConcurrentHashMap<>();
    private final AtomicBoolean healthStatus = new AtomicBoolean(true);
    private final Map<String, Long> templateLastModified = new ConcurrentHashMap<>();
    private ScheduledExecutorService templateWatcher;
    
    @Value("${notification.templates.path:classpath:templates/}")
    private String templatesPath;
    
    @Value("${notification.templates.hotReload:false}")
    private boolean hotReloadEnabled;
    
    @Value("${notification.templates.hotReloadIntervalSeconds:30}")
    private int hotReloadIntervalSeconds;
    
    @Value("${notification.templates.defaultVersion:1}")
    private int defaultTemplateVersion;
    
    @Value("${notification.templates.validateOnStartup:true}")
    private boolean validateOnStartup;

    /**
     * Constructs a new TemplateLoader with the specified dependencies.
     *
     * @param resourceLoader Spring ResourceLoader for accessing template resources
     */
    @Autowired
    public TemplateLoader(ResourceLoader resourceLoader) {
        this.resourceLoader = resourceLoader;
        this.velocityEngine = new VelocityEngine();
        initializeVelocityEngine();
    }

    /**
     * Initializes the Velocity engine with appropriate configuration.
     */
    private void initializeVelocityEngine() {
        velocityEngine.setProperty(RuntimeConstants.RESOURCE_LOADER, "file,classpath");
        
        // File resource loader configuration
        velocityEngine.setProperty("file.resource.loader.class", FileResourceLoader.class.getName());
        velocityEngine.setProperty("file.resource.loader.cache", "false");
        velocityEngine.setProperty("file.resource.loader.modificationCheckInterval", "2");
        
        // Classpath resource loader configuration
        velocityEngine.setProperty("classpath.resource.loader.class", ClasspathResourceLoader.class.getName());
        velocityEngine.setProperty("classpath.resource.loader.cache", "true");
        
        // Runtime configuration
        velocityEngine.setProperty(RuntimeConstants.RUNTIME_LOG_LOGSYSTEM_CLASS, 
                "org.apache.velocity.runtime.log.Slf4jLogChute");
        velocityEngine.setProperty("runtime.log.logsystem.slf4j.logger", "velocity");
        velocityEngine.setProperty(RuntimeConstants.VM_LIBRARY, "VM_global_library.vm");
        velocityEngine.setProperty(RuntimeConstants.VM_LIBRARY_AUTORELOAD, "true");
        velocityEngine.setProperty(RuntimeConstants.VM_PERM_ALLOW_INLINE, "true");
        velocityEngine.setProperty(RuntimeConstants.VM_PERM_ALLOW_INLINE_REPLACE_GLOBAL, "true");
        velocityEngine.setProperty(RuntimeConstants.VM_PERM_INLINE_LOCAL, "true");
        
        velocityEngine.init();
    }

    /**
     * Initializes the template loader after construction.
     * Loads and validates templates if configured to do so.
     */
    @PostConstruct
    public void initialize() {
        logger.info("Initializing TemplateLoader with templates path: {}", templatesPath);
        
        if (validateOnStartup) {
            validateAllTemplates();
        }
        
        if (hotReloadEnabled && isFileSystemPath()) {
            startTemplateWatcher();
        }
    }

    /**
     * Validates all templates in the configured templates path.
     */
    private void validateAllTemplates() {
        logger.info("Validating all templates in path: {}", templatesPath);
        try {
            if (isFileSystemPath()) {
                File templateDir = new File(templatesPath);
                if (templateDir.exists() && templateDir.isDirectory()) {
                    File[] templateFiles = templateDir.listFiles((dir, name) -> name.endsWith(".vm"));
                    if (templateFiles != null) {
                        for (File templateFile : templateFiles) {
                            String templateName = templateFile.getName().replace(".vm", "");
                            try {
                                loadAndValidateTemplate(templateName);
                                logger.debug("Successfully validated template: {}", templateName);
                            } catch (Exception e) {
                                logger.error("Failed to validate template: {}", templateName, e);
                                healthStatus.set(false);
                            }
                        }
                    }
                } else {
                    logger.warn("Template directory does not exist or is not a directory: {}", templatesPath);
                }
            } else {
                // For classpath resources, we need to know the template names in advance
                // This could be enhanced with a resource pattern resolver
                logger.info("Classpath template validation requires template names to be known in advance");
            }
        } catch (Exception e) {
            logger.error("Error during template validation", e);
            healthStatus.set(false);
        }
    }

    /**
     * Starts a background thread that watches for template changes and reloads them.
     */
    private void startTemplateWatcher() {
        logger.info("Starting template hot reload watcher with interval: {} seconds", hotReloadIntervalSeconds);
        templateWatcher = Executors.newSingleThreadScheduledExecutor(r -> {
            Thread thread = new Thread(r, "template-watcher");
            thread.setDaemon(true);
            return thread;
        });
        
        templateWatcher.scheduleWithFixedDelay(
                this::checkForTemplateChanges,
                hotReloadIntervalSeconds,
                hotReloadIntervalSeconds,
                TimeUnit.SECONDS
        );
    }

    /**
     * Checks for changes in template files and reloads them if necessary.
     */
    private void checkForTemplateChanges() {
        if (!isFileSystemPath()) {
            return;
        }
        
        try {
            File templateDir = new File(templatesPath);
            if (templateDir.exists() && templateDir.isDirectory()) {
                File[] templateFiles = templateDir.listFiles((dir, name) -> name.endsWith(".vm"));
                if (templateFiles != null) {
                    for (File templateFile : templateFiles) {
                        String templateName = templateFile.getName().replace(".vm", "");
                        long lastModified = templateFile.lastModified();
                        Long previousModified = templateLastModified.get(templateName);
                        
                        if (previousModified == null || lastModified > previousModified) {
                            logger.info("Template change detected for: {}", templateName);
                            templateLastModified.put(templateName, lastModified);
                            try {
                                // Remove from cache to force reload
                                templateCache.remove(getCacheKey(templateName, defaultTemplateVersion));
                                // Load and validate the template
                                loadAndValidateTemplate(templateName);
                                logger.info("Successfully reloaded template: {}", templateName);
                            } catch (Exception e) {
                                logger.error("Failed to reload template: {}", templateName, e);
                            }
                        }
                    }
                }
            }
        } catch (Exception e) {
            logger.error("Error checking for template changes", e);
        }
    }

    /**
     * Loads and validates a template by name.
     *
     * @param templateName The name of the template to load and validate
     * @return The loaded template
     * @throws Exception If the template cannot be loaded or is invalid
     */
    private Template loadAndValidateTemplate(String templateName) throws Exception {
        Template template = loadTemplate(templateName, defaultTemplateVersion);
        validateTemplate(template);
        return template;
    }

    /**
     * Validates a template by attempting to merge it with an empty context.
     *
     * @param template The template to validate
     * @throws Exception If the template is invalid
     */
    private void validateTemplate(Template template) throws Exception {
        StringWriter writer = new StringWriter();
        template.merge(new org.apache.velocity.VelocityContext(), writer);
    }

    /**
     * Gets a template by name, using the default version.
     *
     * @param templateName The name of the template to get
     * @return The template
     * @throws ResourceNotFoundException If the template cannot be found
     * @throws ParseErrorException If the template cannot be parsed
     */
    @CircuitBreaker(name = "templateLoader", fallbackMethod = "getTemplateFallback")
    public Template getTemplate(String templateName) throws ResourceNotFoundException, ParseErrorException {
        return getTemplate(templateName, defaultTemplateVersion);
    }

    /**
     * Gets a template by name and version.
     *
     * @param templateName The name of the template to get
     * @param version The version of the template to get
     * @return The template
     * @throws ResourceNotFoundException If the template cannot be found
     * @throws ParseErrorException If the template cannot be parsed
     */
    @CircuitBreaker(name = "templateLoader", fallbackMethod = "getTemplateFallback")
    public Template getTemplate(String templateName, int version) throws ResourceNotFoundException, ParseErrorException {
        String cacheKey = getCacheKey(templateName, version);
        
        // Check if the template is already in the cache
        TemplateEntry cachedEntry = templateCache.get(cacheKey);
        if (cachedEntry != null) {
            return cachedEntry.getTemplate();
        }
        
        // Load the template
        Template template = loadTemplate(templateName, version);
        templateCache.put(cacheKey, new TemplateEntry(template, version));
        return template;
    }

    /**
     * Fallback method for getTemplate when the circuit breaker is open.
     *
     * @param templateName The name of the template that was requested
     * @param version The version of the template that was requested
     * @param e The exception that caused the fallback to be invoked
     * @return A fallback template or null
     */
    private Template getTemplateFallback(String templateName, int version, Exception e) {
        logger.error("Circuit breaker triggered for template loading: {}, version: {}", templateName, version, e);
        return getFallbackTemplate(templateName, version);
    }

    /**
     * Fallback method for getTemplate when the circuit breaker is open.
     *
     * @param templateName The name of the template that was requested
     * @param e The exception that caused the fallback to be invoked
     * @return A fallback template or null
     */
    private Template getTemplateFallback(String templateName, Exception e) {
        logger.error("Circuit breaker triggered for template loading: {}", templateName, e);
        return getFallbackTemplate(templateName, defaultTemplateVersion);
    }

    /**
     * Gets a fallback template for when the primary template cannot be loaded.
     *
     * @param templateName The name of the template that was requested
     * @param version The version of the template that was requested
     * @return A fallback template or null
     */
    private Template getFallbackTemplate(String templateName, int version) {
        // Try to get a fallback template from the cache
        String fallbackKey = getCacheKey(templateName + "-fallback", version);
        TemplateEntry fallbackEntry = templateCache.get(fallbackKey);
        if (fallbackEntry != null) {
            return fallbackEntry.getTemplate();
        }
        
        // Try to load a fallback template
        try {
            Template fallbackTemplate = loadTemplate(templateName + "-fallback", version);
            templateCache.put(fallbackKey, new TemplateEntry(fallbackTemplate, version));
            return fallbackTemplate;
        } catch (Exception ex) {
            logger.error("Failed to load fallback template for: {}, version: {}", templateName, version, ex);
            return null;
        }
    }

    /**
     * Loads a template by name and version from the appropriate source.
     *
     * @param templateName The name of the template to load
     * @param version The version of the template to load
     * @return The loaded template
     * @throws ResourceNotFoundException If the template cannot be found
     * @throws ParseErrorException If the template cannot be parsed
     */
    private Template loadTemplate(String templateName, int version) throws ResourceNotFoundException, ParseErrorException {
        String templatePath = getTemplatePath(templateName, version);
        logger.debug("Loading template from path: {}", templatePath);
        
        try {
            // First try to load as a versioned template
            return velocityEngine.getTemplate(templatePath);
        } catch (ResourceNotFoundException e) {
            // If version > 1 and template not found, try to fall back to the default version
            if (version > 1) {
                logger.warn("Versioned template not found: {}, falling back to default version", templatePath);
                return loadTemplate(templateName, 1);
            }
            throw e;
        }
    }

    /**
     * Gets the path to a template based on its name and version.
     *
     * @param templateName The name of the template
     * @param version The version of the template
     * @return The path to the template
     */
    private String getTemplatePath(String templateName, int version) {
        if (version > 1) {
            // For versioned templates, use format: name-v2.vm
            return templateName + "-v" + version + ".vm";
        } else {
            // For default version, use format: name.vm
            return templateName + ".vm";
        }
    }

    /**
     * Gets a cache key for a template based on its name and version.
     *
     * @param templateName The name of the template
     * @param version The version of the template
     * @return The cache key
     */
    private String getCacheKey(String templateName, int version) {
        return templateName + "-v" + version;
    }

    /**
     * Checks if the configured templates path is a file system path.
     *
     * @return true if the templates path is a file system path, false otherwise
     */
    private boolean isFileSystemPath() {
        return !templatesPath.startsWith("classpath:");
    }

    /**
     * Creates a new template in the configured templates path.
     *
     * @param templateName The name of the template to create
     * @param content The content of the template
     * @return true if the template was created successfully, false otherwise
     */
    public boolean createTemplate(String templateName, String content) {
        if (!isFileSystemPath()) {
            logger.error("Cannot create template in classpath resources");
            return false;
        }
        
        try {
            File templateFile = new File(templatesPath, templateName + ".vm");
            if (!templateFile.getParentFile().exists()) {
                templateFile.getParentFile().mkdirs();
            }
            
            try (FileWriter writer = new FileWriter(templateFile)) {
                writer.write(content);
            }
            
            // Validate the template
            try {
                Template template = velocityEngine.getTemplate(templateName + ".vm");
                validateTemplate(template);
                
                // Update cache and last modified time
                String cacheKey = getCacheKey(templateName, defaultTemplateVersion);
                templateCache.put(cacheKey, new TemplateEntry(template, defaultTemplateVersion));
                templateLastModified.put(templateName, templateFile.lastModified());
                
                return true;
            } catch (Exception e) {
                logger.error("Created template is invalid: {}", templateName, e);
                // Delete the invalid template
                templateFile.delete();
                return false;
            }
        } catch (IOException e) {
            logger.error("Failed to create template: {}", templateName, e);
            return false;
        }
    }

    /**
     * Updates an existing template in the configured templates path.
     *
     * @param templateName The name of the template to update
     * @param content The new content of the template
     * @return true if the template was updated successfully, false otherwise
     */
    public boolean updateTemplate(String templateName, String content) {
        if (!isFileSystemPath()) {
            logger.error("Cannot update template in classpath resources");
            return false;
        }
        
        try {
            File templateFile = new File(templatesPath, templateName + ".vm");
            if (!templateFile.exists()) {
                logger.error("Template does not exist: {}", templateName);
                return false;
            }
            
            // Create a backup of the template
            File backupFile = new File(templatesPath, templateName + ".bak");
            Files.copy(templateFile.toPath(), backupFile.toPath(), java.nio.file.StandardCopyOption.REPLACE_EXISTING);
            
            try (FileWriter writer = new FileWriter(templateFile)) {
                writer.write(content);
            }
            
            // Validate the template
            try {
                // Clear from cache to force reload
                String cacheKey = getCacheKey(templateName, defaultTemplateVersion);
                templateCache.remove(cacheKey);
                
                Template template = velocityEngine.getTemplate(templateName + ".vm");
                validateTemplate(template);
                
                // Update cache and last modified time
                templateCache.put(cacheKey, new TemplateEntry(template, defaultTemplateVersion));
                templateLastModified.put(templateName, templateFile.lastModified());
                
                // Delete the backup if everything is successful
                backupFile.delete();
                
                return true;
            } catch (Exception e) {
                logger.error("Updated template is invalid: {}", templateName, e);
                // Restore the backup
                Files.copy(backupFile.toPath(), templateFile.toPath(), java.nio.file.StandardCopyOption.REPLACE_EXISTING);
                backupFile.delete();
                return false;
            }
        } catch (IOException e) {
            logger.error("Failed to update template: {}", templateName, e);
            return false;
        }
    }

    /**
     * Deletes a template from the configured templates path.
     *
     * @param templateName The name of the template to delete
     * @return true if the template was deleted successfully, false otherwise
     */
    public boolean deleteTemplate(String templateName) {
        if (!isFileSystemPath()) {
            logger.error("Cannot delete template in classpath resources");
            return false;
        }
        
        try {
            File templateFile = new File(templatesPath, templateName + ".vm");
            if (!templateFile.exists()) {
                logger.error("Template does not exist: {}", templateName);
                return false;
            }
            
            // Create a backup of the template
            File backupFile = new File(templatesPath, templateName + ".bak");
            Files.copy(templateFile.toPath(), backupFile.toPath(), java.nio.file.StandardCopyOption.REPLACE_EXISTING);
            
            // Delete the template
            boolean deleted = templateFile.delete();
            if (deleted) {
                // Remove from cache and last modified time
                for (int version = 1; version <= 10; version++) { // Assume max 10 versions
                    String cacheKey = getCacheKey(templateName, version);
                    templateCache.remove(cacheKey);
                }
                templateLastModified.remove(templateName);
            }
            
            return deleted;
        } catch (IOException e) {
            logger.error("Failed to delete template: {}", templateName, e);
            return false;
        }
    }

    /**
     * Lists all available templates in the configured templates path.
     *
     * @return A map of template names to their versions
     */
    public Map<String, Integer> listTemplates() {
        Map<String, Integer> templates = new HashMap<>();
        
        try {
            if (isFileSystemPath()) {
                File templateDir = new File(templatesPath);
                if (templateDir.exists() && templateDir.isDirectory()) {
                    File[] templateFiles = templateDir.listFiles((dir, name) -> name.endsWith(".vm"));
                    if (templateFiles != null) {
                        for (File templateFile : templateFiles) {
                            String fileName = templateFile.getName();
                            String templateName;
                            int version = 1;
                            
                            // Check if the template is versioned
                            if (fileName.matches(".*-v\\d+\\.vm$")) {
                                int versionIndex = fileName.lastIndexOf("-v");
                                templateName = fileName.substring(0, versionIndex);
                                version = Integer.parseInt(fileName.substring(versionIndex + 2, fileName.length() - 3));
                            } else {
                                templateName = fileName.substring(0, fileName.length() - 3);
                            }
                            
                            // Store the highest version for each template
                            Integer currentVersion = templates.get(templateName);
                            if (currentVersion == null || version > currentVersion) {
                                templates.put(templateName, version);
                            }
                        }
                    }
                }
            } else {
                // For classpath resources, we need to know the template names in advance
                // This could be enhanced with a resource pattern resolver
                logger.info("Listing classpath templates requires template names to be known in advance");
            }
        } catch (Exception e) {
            logger.error("Error listing templates", e);
        }
        
        return templates;
    }

    /**
     * Gets the content of a template.
     *
     * @param templateName The name of the template
     * @return The content of the template, or empty if the template cannot be found
     */
    public Optional<String> getTemplateContent(String templateName) {
        return getTemplateContent(templateName, defaultTemplateVersion);
    }

    /**
     * Gets the content of a template by name and version.
     *
     * @param templateName The name of the template
     * @param version The version of the template
     * @return The content of the template, or empty if the template cannot be found
     */
    public Optional<String> getTemplateContent(String templateName, int version) {
        String templatePath = getTemplatePath(templateName, version);
        
        try {
            if (isFileSystemPath()) {
                Path path = Paths.get(templatesPath, templatePath);
                if (Files.exists(path)) {
                    return Optional.of(new String(Files.readAllBytes(path)));
                }
            } else {
                Resource resource = resourceLoader.getResource(templatesPath + templatePath);
                if (resource.exists()) {
                    return Optional.of(new String(Files.readAllBytes(resource.getFile().toPath())));
                }
            }
        } catch (IOException e) {
            logger.error("Failed to read template content: {}", templatePath, e);
        }
        
        return Optional.empty();
    }

    /**
     * Provides health information for the template loader.
     *
     * @return Health information
     */
    @Override
    public Health health() {
        if (healthStatus.get()) {
            return Health.up()
                    .withDetail("templatesPath", templatesPath)
                    .withDetail("templateCount", templateCache.size())
                    .withDetail("hotReloadEnabled", hotReloadEnabled)
                    .build();
        } else {
            return Health.down()
                    .withDetail("templatesPath", templatesPath)
                    .withDetail("templateCount", templateCache.size())
                    .withDetail("hotReloadEnabled", hotReloadEnabled)
                    .build();
        }
    }

    /**
     * Represents a cached template entry with its version.
     */
    private static class TemplateEntry {
        private final Template template;
        private final int version;

        public TemplateEntry(Template template, int version) {
            this.template = template;
            this.version = version;
        }

        public Template getTemplate() {
            return template;
        }

        public int getVersion() {
            return version;
        }
    }
}