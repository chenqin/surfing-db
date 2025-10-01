package com.pinterest.surfing.config;

import java.util.Arrays;
import java.util.List;
import java.util.ArrayList;

/**
 * Memory configuration for Surfing DB native operations.
 * Controls memory limits and disk spilling behavior.
 */
public class MemoryConfig {

    // Default: 512MB per batch
    public static final long DEFAULT_MAX_BATCH_MEMORY = 512L * 1024 * 1024;

    // Default temp directory
    public static final String DEFAULT_TEMP_DIR = "/tmp/surfing_spill";

    public enum LoadBalancing {
        ROUND_ROBIN,   // Distribute files evenly across directories
        SPACE_AWARE,   // Prefer directories with more free space
        RANDOM         // Random selection
    }

    private long maxBatchMemory;
    private String tempDir;  // Deprecated, use tempDirs
    private List<String> tempDirs;
    private boolean enableSpilling;
    private boolean autoCleanup;
    private LoadBalancing loadBalancing;

    public MemoryConfig() {
        this.maxBatchMemory = DEFAULT_MAX_BATCH_MEMORY;
        this.tempDir = DEFAULT_TEMP_DIR;
        this.tempDirs = new ArrayList<>(Arrays.asList(DEFAULT_TEMP_DIR));
        this.enableSpilling = true;
        this.autoCleanup = true;
        this.loadBalancing = LoadBalancing.ROUND_ROBIN;
    }

    /**
     * Create configuration from environment variables.
     *
     * Supported environment variables:
     * - SURFING_MAX_BATCH_MEMORY: Maximum memory per batch in bytes (default: 512MB)
     * - SURFING_TEMP_DIR: Single temporary directory (backward compatible)
     * - SURFING_TEMP_DIRS: Comma-separated list of temp directories
     * - SURFING_ENABLE_SPILLING: Enable disk spilling (default: 1)
     * - SURFING_LOAD_BALANCING: Load balancing strategy (ROUND_ROBIN, SPACE_AWARE, RANDOM)
     */
    public static MemoryConfig fromEnvironment() {
        MemoryConfig config = new MemoryConfig();

        String maxMem = System.getenv("SURFING_MAX_BATCH_MEMORY");
        if (maxMem != null) {
            try {
                config.maxBatchMemory = Long.parseLong(maxMem);
            } catch (NumberFormatException e) {
                System.err.println("Invalid SURFING_MAX_BATCH_MEMORY: " + maxMem);
            }
        }

        // Support both single and multiple temp directories
        String tempDirs = System.getenv("SURFING_TEMP_DIRS");
        String tempDir = System.getenv("SURFING_TEMP_DIR");

        if (tempDirs != null) {
            // Parse comma-separated list
            config.tempDirs = new ArrayList<>();
            for (String dir : tempDirs.split(",")) {
                dir = dir.trim();
                if (!dir.isEmpty()) {
                    config.tempDirs.add(dir);
                }
            }
            // Update deprecated tempDir field to first directory
            if (!config.tempDirs.isEmpty()) {
                config.tempDir = config.tempDirs.get(0);
            }
        } else if (tempDir != null) {
            // Single directory (backward compatible)
            config.tempDir = tempDir;
            config.tempDirs = new ArrayList<>(Arrays.asList(tempDir));
        }

        String enableSpilling = System.getenv("SURFING_ENABLE_SPILLING");
        if (enableSpilling != null && enableSpilling.equals("0")) {
            config.enableSpilling = false;
        }

        String loadBalance = System.getenv("SURFING_LOAD_BALANCING");
        if (loadBalance != null) {
            try {
                config.loadBalancing = LoadBalancing.valueOf(loadBalance);
            } catch (IllegalArgumentException e) {
                System.err.println("Invalid SURFING_LOAD_BALANCING: " + loadBalance);
            }
        }

        return config;
    }

    /**
     * Apply this configuration by setting environment variables.
     * Note: This must be called before loading native libraries.
     */
    public void apply() {
        System.setProperty("SURFING_MAX_BATCH_MEMORY", String.valueOf(maxBatchMemory));

        // Set both old and new temp directory properties for compatibility
        if (tempDirs != null && !tempDirs.isEmpty()) {
            System.setProperty("SURFING_TEMP_DIR", tempDirs.get(0));
            System.setProperty("SURFING_TEMP_DIRS", String.join(",", tempDirs));
        } else {
            System.setProperty("SURFING_TEMP_DIR", tempDir);
        }

        System.setProperty("SURFING_ENABLE_SPILLING", enableSpilling ? "1" : "0");
        System.setProperty("SURFING_LOAD_BALANCING", loadBalancing.name());
    }

    // Builder pattern
    public static class Builder {
        private final MemoryConfig config = new MemoryConfig();

        public Builder maxBatchMemory(long bytes) {
            config.maxBatchMemory = bytes;
            return this;
        }

        public Builder maxBatchMemoryMB(long mb) {
            config.maxBatchMemory = mb * 1024 * 1024;
            return this;
        }

        public Builder maxBatchMemoryGB(long gb) {
            config.maxBatchMemory = gb * 1024 * 1024 * 1024;
            return this;
        }

        public Builder tempDir(String dir) {
            config.tempDir = dir;
            config.tempDirs = new ArrayList<>(Arrays.asList(dir));
            return this;
        }

        public Builder tempDirs(List<String> dirs) {
            config.tempDirs = new ArrayList<>(dirs);
            if (!dirs.isEmpty()) {
                config.tempDir = dirs.get(0);
            }
            return this;
        }

        public Builder tempDirs(String... dirs) {
            return tempDirs(Arrays.asList(dirs));
        }

        public Builder enableSpilling(boolean enable) {
            config.enableSpilling = enable;
            return this;
        }

        public Builder autoCleanup(boolean enable) {
            config.autoCleanup = enable;
            return this;
        }

        public Builder loadBalancing(LoadBalancing strategy) {
            config.loadBalancing = strategy;
            return this;
        }

        public MemoryConfig build() {
            return config;
        }
    }

    public static Builder builder() {
        return new Builder();
    }

    // Getters and setters
    public long getMaxBatchMemory() {
        return maxBatchMemory;
    }

    public void setMaxBatchMemory(long maxBatchMemory) {
        this.maxBatchMemory = maxBatchMemory;
    }

    public String getTempDir() {
        return tempDir;
    }

    public void setTempDir(String tempDir) {
        this.tempDir = tempDir;
        this.tempDirs = new ArrayList<>(Arrays.asList(tempDir));
    }

    public List<String> getTempDirs() {
        return tempDirs;
    }

    public void setTempDirs(List<String> tempDirs) {
        this.tempDirs = tempDirs;
        if (!tempDirs.isEmpty()) {
            this.tempDir = tempDirs.get(0);
        }
    }

    public boolean isEnableSpilling() {
        return enableSpilling;
    }

    public void setEnableSpilling(boolean enableSpilling) {
        this.enableSpilling = enableSpilling;
    }

    public boolean isAutoCleanup() {
        return autoCleanup;
    }

    public void setAutoCleanup(boolean autoCleanup) {
        this.autoCleanup = autoCleanup;
    }

    public LoadBalancing getLoadBalancing() {
        return loadBalancing;
    }

    public void setLoadBalancing(LoadBalancing loadBalancing) {
        this.loadBalancing = loadBalancing;
    }

    @Override
    public String toString() {
        return "MemoryConfig{" +
                "maxBatchMemory=" + (maxBatchMemory / (1024 * 1024)) + "MB" +
                ", tempDirs=" + tempDirs +
                ", enableSpilling=" + enableSpilling +
                ", autoCleanup=" + autoCleanup +
                ", loadBalancing=" + loadBalancing +
                '}';
    }
}
