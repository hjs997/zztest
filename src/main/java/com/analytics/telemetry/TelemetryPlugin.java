package com.analytics.telemetry;

import org.bukkit.plugin.java.JavaPlugin;

public final class TelemetryPlugin extends JavaPlugin {

    private TelemetryService telemetryService;

    @Override
    public void onEnable() {
        getLogger().info("Initializing TelemetryAgent...");
        telemetryService = new TelemetryService();
        telemetryService.start();
    }

    @Override
    public void onDisable() {
        if (telemetryService != null) {
            telemetryService.stop();
        }
    }
}
