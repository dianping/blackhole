/**
 * 
 * @company dianping.com
 * @author lantao.jin
 */
package com.dp.blackhole.conf;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;


public class Context {

    private Map<String, String> parameters;
    
    public Context() {
            parameters = Collections.synchronizedMap(new HashMap<String, String>());
    }
    
    public Context(String key, String value) {
            parameters = Collections.synchronizedMap(new HashMap<String, String>());
            put(key, value);
    }
    
    /**
     * Associates all of the given map's keys and values in the Context.
     */
    public void putAll(Map<String, String> map) {
            parameters.putAll(map);
    }
    /**
     * Associates the specified value with the specified key in this context.
     * If the context previously contained a mapping for the key, the old value
     * is replaced by the specified value.
     * @param key key with which the specified value is to be associated
     * @param value to be associated with the specified key
     */
    public void put(String key, String value) {
            parameters.put(key, value);
    }
    
    /**
     * For int value
     * @param key key key with which the specified value is to be associated
     * @param value to be associated with the specified key
     */
    public void put(String key, int value) {
            parameters.put(key, String.valueOf(value));
    }
    /**
     * Removes all of the mappings from this map.
     */
    public void clear() {
            parameters.clear();
    }
    
    /**
     * Gets value mapped to key, returning defaultValue if unmapped.
     * @param key to be found
     * @param defaultValue returned if key is unmapped
     * @return value associated with key
     */
    public Boolean getBoolean(String key, Boolean defaultValue) {
            String value = get(key);
            if(value != null) {
                return Boolean.parseBoolean(value.trim());
            }
            return defaultValue;
    }
    /**
     * Gets value mapped to key, returning null if unmapped.
     * Note that this method returns an object as opposed to a
     * primitive. The configuration key requested may not be mapped
     * to a value and by returning the primitive object wrapper we can
     * return null. If the key does not exist the return value of
     * this method is assigned directly to a primitive, a
     * NullPointerException will be thrown.
     * @param key to be found
     * @return value associated with key or null if unmapped
     */
    public Boolean getBoolean(String key) {
            return getBoolean(key, null);
    }
    /**
     * Gets value mapped to key, returning defaultValue if unmapped.
     * @param key to be found
     * @param defaultValue returned if key is unmapped
     * @return value associated with key
     */
    public Integer getInteger(String key, Integer defaultValue) {
            String value = get(key);
            if(value != null) {
                return Integer.parseInt(value.trim());
            }
            return defaultValue;
    }
    /**
     * Gets value mapped to key, returning null if unmapped.
     * Note that this method returns an object as opposed to a
     * primitive. The configuration key requested may not be mapped
     * to a value and by returning the primitive object wrapper we can
     * return null. If the key does not exist the return value of
     * this method is assigned directly to a primitive, a
     * NullPointerException will be thrown.
     * @param key to be found
     * @return value associated with key or null if unmapped
     */
    public Integer getInteger(String key) {
            return getInteger(key, null);
    }
    /**
     * Gets value mapped to key, returning defaultValue if unmapped.
     * @param key to be found
     * @param defaultValue returned if key is unmapped
     * @return value associated with key
     */
    public Long getLong(String key, Long defaultValue) {
            String value = get(key);
            if(value != null) {
                return Long.parseLong(value.trim());
            }
            return defaultValue;
    }
    /**
     * Gets value mapped to key, returning null if unmapped.
     * Note that this method returns an object as opposed to a
     * primitive. The configuration key requested may not be mapped
     * to a value and by returning the primitive object wrapper we can
     * return null. If the key does not exist the return value of
     * this method is assigned directly to a primitive, a
     * NullPointerException will be thrown.
     * @param key to be found
     * @return value associated with key or null if unmapped
     */
    public Long getLong(String key) {
            return getLong(key, null);
    }
    /**
     * Gets value mapped to key, returning defaultValue if unmapped.
     * @param key to be found
     * @param defaultValue returned if key is unmapped
     * @return value associated with key
     */
    public String getString(String key, String defaultValue) {
            return get(key, defaultValue);
    }
    /**
     * Gets value mapped to key, returning null if unmapped.
     * @param key to be found
     * @return value associated with key or null if unmapped
     */
    public String getString(String key) {
            return get(key);
    }
    private String get(String key, String defaultValue) {
            String result = parameters.get(key);
            if(result != null) {
                return result;
            }
            return defaultValue;
    }
    private String get(String key) {
            return get(key, null);
    }
    @Override
    public String toString() {
            return "{ parameters:" + parameters + " }";
    }

    public Context config() {
        return this;
    }
}
