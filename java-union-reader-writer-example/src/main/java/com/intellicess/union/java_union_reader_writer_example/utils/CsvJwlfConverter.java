package com.intellicess.union.java_union_reader_writer_example.utils;

import com.intellicess.union.java_union_reader_writer_example.client.DateTimeConverter;
import com.intellicess.union.java_union_reader_writer_example.client.model.JwlfCurve;
import com.intellicess.union.java_union_reader_writer_example.client.model.JwlfHeader;
import com.intellicess.union.java_union_reader_writer_example.client.model.JwlfRequest;
import com.intellicess.union.java_union_reader_writer_example.client.model.JwlfValueType;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

public class CsvJwlfConverter {

    private static final List<String> TRUE_BOOLEAN_VALUES = List.of("yes", "true", "on");
    private static final List<String> FALSE_BOOLEAN_VALUES = List.of("no", "false", "off");
    public static final String FILENAME_METADATA_KEY = "filename";

    private static final Logger log = LogManager.getLogger(CsvJwlfConverter.class);

    private CsvJwlfConverter() {
    }

    public static JwlfRequest convertCsvToJwlf(String well, String filename, String content,
                                               Map<String, JwlfValueType> headerValueTypeMap) {
        String filenameWithoutExtension = filename.replace(".csv", "");
        JwlfRequest jwlfRequest = new JwlfRequest();
        JwlfHeader jwlfHeader = JwlfHeader.builder()
                .name(filenameWithoutExtension)
                .well(well)
                .build();
        jwlfRequest.setHeader(jwlfHeader);
        if (content == null || content.isBlank()) {
            return jwlfRequest;
        }

        List<String> lines = Arrays.asList(content.split("\n"));
        String titleLine = lines.get(0);
        List<String> headers = Arrays.asList(titleLine.split(","));
        List<JwlfCurve> jwlfCurves = headers.stream()
                .map(header -> JwlfCurve.builder()
                        .name(header)
                        .valueType(headerValueTypeMap.get(header))
                        .build())
                .collect(Collectors.toList());
        jwlfRequest.setCurves(jwlfCurves);
        if (lines.size() == 1) {
            return jwlfRequest;
        }

        List<String> dataLines = lines.stream().skip(1).collect(Collectors.toList());
        List<List<Object>> data = dataLines.stream()
                .map(dataLine -> convertCsvLineToJwlfData(dataLine, jwlfCurves))
                .collect(Collectors.toList());
        jwlfRequest.setData(data);
        return jwlfRequest;
    }

    private static List<Object> convertCsvLineToJwlfData(String dataLine, List<JwlfCurve> jwlfCurves) {
        List<String> values = Arrays.asList(dataLine.split(","));
        if (values.size() != jwlfCurves.size()) {
            throw new RuntimeException("Data line='" + dataLine + "' should have the same number of values as number of headers ('" + jwlfCurves.size() + "').");
        }
        List<Object> castedValues = new ArrayList<>();
        for (int i = 0; i < values.size(); i++) {
            JwlfCurve jwlfCurve = jwlfCurves.get(i);
            Object value = values.get(i);
            Object castedValue = castValueToRightType(jwlfCurve, value);
            castedValues.add(castedValue);
        }
        return castedValues;
    }

    public static Object castValueToRightType(JwlfCurve jwlfCurve, Object value) {
        if (value == null) {
            return null;
        }

        JwlfValueType jwlfValueType = jwlfCurve.getValueType();
        if (jwlfValueType == null) {
            return value;
        }

        String stringValue = value.toString();
        switch (jwlfValueType) {
            case STRING:
                return stringValue;
            case INTEGER:
                return Integer.parseInt(stringValue);
            case FLOAT:
                return Double.parseDouble(stringValue);
            case BOOLEAN:
                String lowerCaseStringValue = stringValue.toLowerCase();
                if (TRUE_BOOLEAN_VALUES.contains(lowerCaseStringValue)) {
                    return true;
                } else if (FALSE_BOOLEAN_VALUES.contains(lowerCaseStringValue)) {
                    return false;
                }
                break;
            case DATETIME:
                return DateTimeConverter.parseZonedDateTime(stringValue);
        }
        log.warn("Couldn't cast value='{}' to value type='{}'. Returning null for curve with name='{}'",
                value, jwlfValueType, jwlfCurve.getName());
        return null;
    }
}
