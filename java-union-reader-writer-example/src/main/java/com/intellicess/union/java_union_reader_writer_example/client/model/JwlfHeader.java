package com.intellicess.union.java_union_reader_writer_example.client.model;


import java.time.ZonedDateTime;
import java.util.HashMap;
import java.util.Map;

public class JwlfHeader {

    private String name;

    private String description;

    private String well;

    private String wellbore;

    private String field;

    private String country;

    private ZonedDateTime date;

    private String operator;

    private String serviceCompany;

    private String runNumber;

    private Double elevation;

    private String source;

    private Double startIndex;

    private Double endIndex;

    private Double step;

    private String dataUri;

    private Map<String, Object> metadata = new HashMap<>();

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public String getDescription() {
        return description;
    }

    public void setDescription(String description) {
        this.description = description;
    }

    public String getWell() {
        return well;
    }

    public void setWell(String well) {
        this.well = well;
    }

    public String getWellbore() {
        return wellbore;
    }

    public void setWellbore(String wellbore) {
        this.wellbore = wellbore;
    }

    public String getField() {
        return field;
    }

    public void setField(String field) {
        this.field = field;
    }

    public String getCountry() {
        return country;
    }

    public void setCountry(String country) {
        this.country = country;
    }

    public ZonedDateTime getDate() {
        return date;
    }

    public void setDate(ZonedDateTime date) {
        this.date = date;
    }

    public String getOperator() {
        return operator;
    }

    public void setOperator(String operator) {
        this.operator = operator;
    }

    public String getServiceCompany() {
        return serviceCompany;
    }

    public void setServiceCompany(String serviceCompany) {
        this.serviceCompany = serviceCompany;
    }

    public String getRunNumber() {
        return runNumber;
    }

    public void setRunNumber(String runNumber) {
        this.runNumber = runNumber;
    }

    public Double getElevation() {
        return elevation;
    }

    public void setElevation(Double elevation) {
        this.elevation = elevation;
    }

    public String getSource() {
        return source;
    }

    public void setSource(String source) {
        this.source = source;
    }

    public Double getStartIndex() {
        return startIndex;
    }

    public void setStartIndex(Double startIndex) {
        this.startIndex = startIndex;
    }

    public Double getEndIndex() {
        return endIndex;
    }

    public void setEndIndex(Double endIndex) {
        this.endIndex = endIndex;
    }

    public Double getStep() {
        return step;
    }

    public void setStep(Double step) {
        this.step = step;
    }

    public String getDataUri() {
        return dataUri;
    }

    public void setDataUri(String dataUri) {
        this.dataUri = dataUri;
    }

    public Map<String, Object> getMetadata() {
        return metadata;
    }

    public void setMetadata(Map<String, Object> metadata) {
        this.metadata = metadata;
    }

    public static JwlfHeaderBuilder builder() {
        return new JwlfHeaderBuilder();
    }

    public static final class JwlfHeaderBuilder {
        private String name;
        private String description;
        private String well;
        private String wellbore;
        private String field;
        private String country;
        private ZonedDateTime date;
        private String operator;
        private String serviceCompany;
        private String runNumber;
        private Double elevation;
        private String source;
        private Double startIndex;
        private Double endIndex;
        private Double step;
        private String dataUri;
        private Map<String, Object> metadata = new HashMap<>();

        private JwlfHeaderBuilder() {
        }

        public JwlfHeaderBuilder name(String name) {
            this.name = name;
            return this;
        }

        public JwlfHeaderBuilder description(String description) {
            this.description = description;
            return this;
        }

        public JwlfHeaderBuilder well(String well) {
            this.well = well;
            return this;
        }

        public JwlfHeaderBuilder wellbore(String wellbore) {
            this.wellbore = wellbore;
            return this;
        }

        public JwlfHeaderBuilder field(String field) {
            this.field = field;
            return this;
        }

        public JwlfHeaderBuilder country(String country) {
            this.country = country;
            return this;
        }

        public JwlfHeaderBuilder date(ZonedDateTime date) {
            this.date = date;
            return this;
        }

        public JwlfHeaderBuilder operator(String operator) {
            this.operator = operator;
            return this;
        }

        public JwlfHeaderBuilder serviceCompany(String serviceCompany) {
            this.serviceCompany = serviceCompany;
            return this;
        }

        public JwlfHeaderBuilder runNumber(String runNumber) {
            this.runNumber = runNumber;
            return this;
        }

        public JwlfHeaderBuilder elevation(Double elevation) {
            this.elevation = elevation;
            return this;
        }

        public JwlfHeaderBuilder source(String source) {
            this.source = source;
            return this;
        }

        public JwlfHeaderBuilder startIndex(Double startIndex) {
            this.startIndex = startIndex;
            return this;
        }

        public JwlfHeaderBuilder endIndex(Double endIndex) {
            this.endIndex = endIndex;
            return this;
        }

        public JwlfHeaderBuilder step(Double step) {
            this.step = step;
            return this;
        }

        public JwlfHeaderBuilder dataUri(String dataUri) {
            this.dataUri = dataUri;
            return this;
        }

        public JwlfHeaderBuilder metadata(Map<String, Object> metadata) {
            this.metadata = metadata;
            return this;
        }

        public JwlfHeader build() {
            JwlfHeader jwlfHeader = new JwlfHeader();
            jwlfHeader.setName(name);
            jwlfHeader.setDescription(description);
            jwlfHeader.setWell(well);
            jwlfHeader.setWellbore(wellbore);
            jwlfHeader.setField(field);
            jwlfHeader.setCountry(country);
            jwlfHeader.setDate(date);
            jwlfHeader.setOperator(operator);
            jwlfHeader.setServiceCompany(serviceCompany);
            jwlfHeader.setRunNumber(runNumber);
            jwlfHeader.setElevation(elevation);
            jwlfHeader.setSource(source);
            jwlfHeader.setStartIndex(startIndex);
            jwlfHeader.setEndIndex(endIndex);
            jwlfHeader.setStep(step);
            jwlfHeader.setDataUri(dataUri);
            jwlfHeader.setMetadata(metadata);
            return jwlfHeader;
        }
    }
}
