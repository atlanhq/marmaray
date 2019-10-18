package com.uber.marmaray.common.configuration;

import lombok.Getter;
import lombok.NonNull;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class StreamingConfiguration {
    public static final String STREAMING_PREFIX = Configuration.MARMARAY_PREFIX + "streaming.";

    /**
     * Flag to control whether error table is enabled
     */
    public static final String IS_ENABLED = STREAMING_PREFIX + "enabled";
    public static final boolean DEFAULT_IS_ENABLED = false;

    @Getter
    private final Configuration conf;

    @Getter
    public final boolean isEnabled;

    public StreamingConfiguration(@NonNull final Configuration conf) {
        this.conf = conf;
        this.isEnabled = conf.getBooleanProperty(IS_ENABLED, DEFAULT_IS_ENABLED);
    }
}
