package com.intellicess.union.java_union_reader_writer_example.client.model;

import java.util.List;

public class JwlfResponseList {

    private List<JwlfResponse> list;

    private Long stableDataTimestamp;

    public List<JwlfResponse> getList() {
        return list;
    }

    public void setList(List<JwlfResponse> list) {
        this.list = list;
    }

    public Long getStableDataTimestamp() {
        return stableDataTimestamp;
    }

    public void setStableDataTimestamp(Long stableDataTimestamp) {
        this.stableDataTimestamp = stableDataTimestamp;
    }
}
