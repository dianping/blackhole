package com.dp.blackhole.conf;

import java.util.HashMap;
import java.util.Map;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;


public class Configuration {
        private static final Log logger = LogFactory.getLog(Configuration.class);
        public static Map<String, Context> configMap = new HashMap<String, Context>();

        public boolean addRawProperty(String name, String value) {
                // Null names and values not supported
                if (name == null || value == null) {
                        logger.error("Null names and values not supported");
                        return false;
                }

                // Empty values are not supported
                if (value.trim().length() == 0) {
                        logger.error("Empty values are not supported");
                        return false;
                }

                // Remove leading and trailing spaces
                name = name.trim();
                value = value.trim();

                int index = name.indexOf('.');

                // All configuration keys must have a prefix defined as app name or conf identify
                if (index == -1) {
                        logger.error("All configuration keys must have a prefix defined as app name or conf identify");
                        return false;
                }
                String appName = name.substring(0, index);

                // App name or conf identify must be specified for all properties
                if (appName.length() == 0) {
                        logger.error("App name or conf identify must be specified for all properties");
                        return false;
                }

                String configKey = name.substring(index + 1);

                // Configuration key must be specified for every property
                if (configKey.length() == 0) {
                        logger.error("Configuration key must be specified for every property");
                        return false;
                }

                if (!configMap.containsKey(appName)) {
                    configMap.put(appName, new Context(configKey, value));
                } else {
                    configMap.get(appName).put(configKey, value);
                }

                return true;
        }
}
