package com.intellicess.union.java_union_reader_writer_example.client.model;

import java.util.List;

public class JwlfResponse {

    private String id;

    private String client;

    private String region;

    private String asset;

    private String folder;

    private Long creationTimestamp;

    private JwlfHeader header;

    private List<JwlfCurve> curves;

    private List<List<Object>> data;

    public String getId() {
        return id;
    }

    public void setId(String id) {
        this.id = id;
    }

    public String getClient() {
        return client;
    }

    public void setClient(String client) {
        this.client = client;
    }

    public String getRegion() {
        return region;
    }

    public void setRegion(String region) {
        this.region = region;
    }

    public String getAsset() {
        return asset;
    }

    public void setAsset(String asset) {
        this.asset = asset;
    }

    public String getFolder() {
        return folder;
    }

    public void setFolder(String folder) {
        this.folder = folder;
    }

    public Long getCreationTimestamp() {
        return creationTimestamp;
    }

    public void setCreationTimestamp(Long creationTimestamp) {
        this.creationTimestamp = creationTimestamp;
    }

    public JwlfHeader getHeader() {
        return header;
    }

    public void setHeader(JwlfHeader header) {
        this.header = header;
    }

    public List<JwlfCurve> getCurves() {
        return curves;
    }

    public void setCurves(List<JwlfCurve> curves) {
        this.curves = curves;
    }

    public List<List<Object>> getData() {
        return data;
    }

    public void setData(List<List<Object>> data) {
        this.data = data;
    }
}
