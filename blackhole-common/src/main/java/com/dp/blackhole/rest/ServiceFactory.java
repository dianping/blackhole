package com.dp.blackhole.rest;

import java.util.concurrent.ConcurrentHashMap;

public class ServiceFactory {
    private static ConcurrentHashMap<String, Object> serviceHolder = new ConcurrentHashMap<String, Object>();
    
    private ServiceFactory() {}
    
    @SuppressWarnings("unchecked")
    public static <T> T getServiceByName(Class<T> clazz) {
        T service = (T) serviceHolder.get(clazz.getCanonicalName());
        T result = null;
        if (service == null) {
            try {
                service = clazz.newInstance();
            } catch (Throwable t) {
                throw new RuntimeException("Faild to get service by " + clazz, t);
            }
            result = (T)serviceHolder.putIfAbsent(clazz.getCanonicalName(), service);
        }
        if (result == null) {
            result = service;
        }
        return result;
    }
}
