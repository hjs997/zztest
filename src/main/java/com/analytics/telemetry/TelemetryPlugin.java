package com.analytics.telemetry;

import org.bukkit.plugin.java.JavaPlugin;

public class TelemetryPlugin extends JavaPlugin {

    private TelemetryService telemetryService;

    @Override
    public void onEnable() {
        // 实例化服务并启动
        this.telemetryService = new TelemetryService();
        this.telemetryService.start();
        
        getLogger().info("TelemetryPlugin enabled successfully.");
    }

    @Override
    public void onDisable() {
        // 插件关闭时停止服务
        if (this.telemetryService != null) {
            this.telemetryService.stop();
        }
        
        getLogger().info("TelemetryPlugin disabled.");
    }
}
