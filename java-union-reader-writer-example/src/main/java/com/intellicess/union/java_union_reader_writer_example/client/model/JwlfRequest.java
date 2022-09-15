package com.intellicess.union.java_union_reader_writer_example.client.model;

import java.util.List;

public class JwlfRequest {

    private JwlfHeader header;

    private List<JwlfCurve> curves;

    private List<List<Object>> data;

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

    public static JwlfRequestBuilder builder() {
        return new JwlfRequestBuilder();
    }

    public static final class JwlfRequestBuilder {
        private JwlfHeader header;
        private List<JwlfCurve> curves;
        private List<List<Object>> data;

        private JwlfRequestBuilder() {
        }

        public JwlfRequestBuilder header(JwlfHeader header) {
            this.header = header;
            return this;
        }

        public JwlfRequestBuilder curves(List<JwlfCurve> curves) {
            this.curves = curves;
            return this;
        }

        public JwlfRequestBuilder data(List<List<Object>> data) {
            this.data = data;
            return this;
        }

        public JwlfRequest build() {
            JwlfRequest jwlfRequest = new JwlfRequest();
            jwlfRequest.setHeader(header);
            jwlfRequest.setCurves(curves);
            jwlfRequest.setData(data);
            return jwlfRequest;
        }
    }
}
