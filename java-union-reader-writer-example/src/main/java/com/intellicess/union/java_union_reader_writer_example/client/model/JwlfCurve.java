package com.intellicess.union.java_union_reader_writer_example.client.model;

import java.util.List;

public class JwlfCurve {

    private String name = "";

    private String description;

    private String quantity;

    private String unit;

    private JwlfValueType valueType;

    private Long dimensions = 1L;

    private List<Object> axis;

    private Long maxSize;

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

    public String getQuantity() {
        return quantity;
    }

    public void setQuantity(String quantity) {
        this.quantity = quantity;
    }

    public String getUnit() {
        return unit;
    }

    public void setUnit(String unit) {
        this.unit = unit;
    }

    public JwlfValueType getValueType() {
        return valueType;
    }

    public void setValueType(JwlfValueType valueType) {
        this.valueType = valueType;
    }

    public Long getDimensions() {
        return dimensions;
    }

    public void setDimensions(Long dimensions) {
        this.dimensions = dimensions;
    }

    public List<Object> getAxis() {
        return axis;
    }

    public void setAxis(List<Object> axis) {
        this.axis = axis;
    }

    public Long getMaxSize() {
        return maxSize;
    }

    public void setMaxSize(Long maxSize) {
        this.maxSize = maxSize;
    }

    public static JwlfCurveBuilder builder() {
        return new JwlfCurveBuilder();
    }

    public static final class JwlfCurveBuilder {
        private String name;
        private String description;
        private String quantity;
        private String unit;
        private JwlfValueType valueType;
        private Long dimensions;
        private List<Object> axis;
        private Long maxSize;

        private JwlfCurveBuilder() {
        }

        public JwlfCurveBuilder name(String name) {
            this.name = name;
            return this;
        }

        public JwlfCurveBuilder description(String description) {
            this.description = description;
            return this;
        }

        public JwlfCurveBuilder quantity(String quantity) {
            this.quantity = quantity;
            return this;
        }

        public JwlfCurveBuilder unit(String unit) {
            this.unit = unit;
            return this;
        }

        public JwlfCurveBuilder valueType(JwlfValueType valueType) {
            this.valueType = valueType;
            return this;
        }

        public JwlfCurveBuilder dimensions(Long dimensions) {
            this.dimensions = dimensions;
            return this;
        }

        public JwlfCurveBuilder axis(List<Object> axis) {
            this.axis = axis;
            return this;
        }

        public JwlfCurveBuilder maxSize(Long maxSize) {
            this.maxSize = maxSize;
            return this;
        }

        public JwlfCurve build() {
            JwlfCurve jwlfCurve = new JwlfCurve();
            jwlfCurve.setName(name);
            jwlfCurve.setDescription(description);
            jwlfCurve.setQuantity(quantity);
            jwlfCurve.setUnit(unit);
            jwlfCurve.setValueType(valueType);
            jwlfCurve.setDimensions(dimensions);
            jwlfCurve.setAxis(axis);
            jwlfCurve.setMaxSize(maxSize);
            return jwlfCurve;
        }
    }
}
