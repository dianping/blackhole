package com.dp.blackhole.broker.ftp;

public class FTPConfigration {
    private String url;
    private int port;
    private String username;
    private String password;
    private String rootDir;
    
    public FTPConfigration(String url, int port, String username,
            String password, String rootDir) {
        super();
        this.url = url;
        this.port = port;
        this.username = username;
        this.password = password;
        this.rootDir = rootDir;
    }
    
    public String getUrl() {
        return url;
    }
    
    public void setUrl(String url) {
        this.url = url;
    }
    
    public int getPort() {
        return port;
    }
    
    public void setPort(int port) {
        this.port = port;
    }
    
    public String getUsername() {
        return username;
    }
    public void setUsername(String username) {
        this.username = username;
    }
    
    public String getPassword() {
        return password;
    }
    
    public void setPassword(String password) {
        this.password = password;
    }
    public String getRootDir() {
        return rootDir;
    }
    
    public void setRootDir(String rootDir) {
        this.rootDir = rootDir;
    }
}