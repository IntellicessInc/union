package com.intellicess.union.java_union_reader_writer_example.utils;

import com.intellicess.union.java_union_reader_writer_example.client.model.*;
import org.apache.commons.io.FileUtils;

import java.io.File;
import java.io.IOException;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.ZonedDateTime;
import java.util.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class CsvJwlfWithBase64EncodedBinariesConverter {

    private static final String BINARIES_DATA_CURVE_NAME = "Binaries data";
    private static final String BINARIES_NAMES_CURVE_NAME = "Binaries names";
    public static final String BASE64_ENCODED_BINARIES_EXAMPLE_METADATA_KEY = "base64-encoded-binaries-example";
    public static final Charset CHARSET = StandardCharsets.US_ASCII;


    public static Optional<JwlfRequest> convertFolderToJwlf(String folderPath, Map<String, JwlfValueType> headerValueTypeMapping) {
        String jwlfHeaderName = null;
        Map<String, Object> jwlfHeaderValuesMap = new HashMap<>();
        List<JwlfCurve> jwlfCurves = new ArrayList<>();
        List<Object> dataEntry = new ArrayList<>();
        List<String> encodedBinaryFilesContents = new ArrayList<>();
        List<String> encodedBinaryFilesNames = new ArrayList<>();
        for (File file : Objects.requireNonNull(new File(folderPath).listFiles())) {
            if (file.isDirectory()) {
                continue;
            }
            String name = file.getName();
            if (name.endsWith(".csv")) {
                jwlfHeaderName = name.replace(".csv", "");
                String csvContent;
                try {
                    csvContent = Files.readString(file.toPath());
                } catch (IOException e) {
                    throw new RuntimeException(e);
                }
                List<String> lines = Arrays.asList(csvContent.split("\n"));
                if (lines.size() != 2) {
                    return Optional.empty();
                }
                List<String> headers = Arrays.asList(lines.get(0).split(","));
                List<String> dataElements = Arrays.asList(lines.get(1).split(","));
                for (int i = 0; i < headers.size(); i++) {
                    String header = headers.get(i);
                    String curveUnit = null;
                    String data = dataElements.get(i);
                    Optional<String> match = findMatch(header, "\\[.+]");
                    if (match.isPresent()) {
                        String fieldName = match.get().replace("[", "").replace("]", "");
                        jwlfHeaderValuesMap.put(fieldName, data);
                        continue;
                    }
                    Optional<String> curveUnitMatch = findMatch(header, "\\(.+\\)");
                    if (curveUnitMatch.isPresent()) {
                        String curveUnitMatched = curveUnitMatch.get();
                        curveUnit = curveUnitMatched.replace("(", "").replace(")", "");
                        header = header.replace(curveUnitMatched, "");
                    }
                    JwlfCurve jwlfCurve = JwlfCurve.builder()
                            .name(header)
                            .valueType(headerValueTypeMapping.get(header))
                            .dimensions(1L)
                            .unit(curveUnit)
                            .build();
                    jwlfCurves.add(jwlfCurve);
                    dataEntry.add(CsvJwlfConverter.castValueToRightType(jwlfCurve, data));
                }
            } else {
                try {
                    byte[] contentBytes = Files.readAllBytes(file.toPath());
                    String base64EncodedContent = new String(Base64.getEncoder().encode(contentBytes), CHARSET);
                    encodedBinaryFilesNames.add(name);
                    encodedBinaryFilesContents.add(base64EncodedContent);
                } catch (IOException e) {
                    throw new RuntimeException(e);
                }
            }
        }
        if (jwlfHeaderName == null) {
            return Optional.empty();
        }
        jwlfHeaderValuesMap.put("name", jwlfHeaderName);
        jwlfHeaderValuesMap.put("metadata", Map.of(
                CsvJwlfConverter.FILENAME_METADATA_KEY, jwlfHeaderName.replace(".csv", ""),
                BASE64_ENCODED_BINARIES_EXAMPLE_METADATA_KEY, true
        ));
        JwlfHeader jwlfHeader = JwlfHeader.builder()
                .name((String) jwlfHeaderValuesMap.get("name"))
                .description((String) jwlfHeaderValuesMap.get("description"))
                .well((String) jwlfHeaderValuesMap.get("well"))
                .wellbore((String) jwlfHeaderValuesMap.get("wellbore"))
                .field((String) jwlfHeaderValuesMap.get("field"))
                .country((String) jwlfHeaderValuesMap.get("country"))
                .date((ZonedDateTime) jwlfHeaderValuesMap.get("date"))
                .operator((String) jwlfHeaderValuesMap.get("operator"))
                .serviceCompany((String) jwlfHeaderValuesMap.get("serviceCompany"))
                .runNumber((String) jwlfHeaderValuesMap.get("runNumber"))
                .elevation((Double) jwlfHeaderValuesMap.get("elevation"))
                .source((String) jwlfHeaderValuesMap.get("source"))
                .startIndex((Double) jwlfHeaderValuesMap.get("startIndex"))
                .endIndex((Double) jwlfHeaderValuesMap.get("endIndex"))
                .step((Double) jwlfHeaderValuesMap.get("step"))
                .dataUri((String) jwlfHeaderValuesMap.get("dataUri"))
                .metadata((Map<String, Object>) jwlfHeaderValuesMap.get("metadata"))
                .build();

        jwlfCurves.add(JwlfCurve.builder().name(BINARIES_NAMES_CURVE_NAME)
                .name(BINARIES_NAMES_CURVE_NAME)
                .valueType(JwlfValueType.STRING)
                .dimensions((long) encodedBinaryFilesNames.size())
                .build());
        dataEntry.add(encodedBinaryFilesNames);
        jwlfCurves.add(JwlfCurve.builder().name(BINARIES_DATA_CURVE_NAME)
                .valueType(JwlfValueType.STRING)
                .dimensions((long) encodedBinaryFilesContents.size())
                .build());
        dataEntry.add(encodedBinaryFilesContents);

        List<List<Object>> jwlfData = new ArrayList<>();
        jwlfData.add(dataEntry);
        return Optional.of(JwlfRequest.builder()
                .header(jwlfHeader)
                .curves(jwlfCurves)
                .data(jwlfData)
                .build());
    }

    public static void convertJwlfToFolder(String basePath, JwlfResponse log) {
        String folderPath = basePath + "/" + log.getHeader().getName() + " files";
        try {
            FileUtils.deleteDirectory(new File(folderPath));
            Files.createDirectory(Path.of(folderPath));
        } catch (IOException e) {
            throw new RuntimeException(e);
        }

        Integer binariesNamesCurveIndex = null;
        for (int i = 0; i < log.getCurves().size(); i++) {
            if (BINARIES_NAMES_CURVE_NAME.equals(log.getCurves().get(i).getName())) {
                binariesNamesCurveIndex = i;
            }
        }
        if (binariesNamesCurveIndex == null) {
            return;
        }

        Integer binariesDataCurveIndex = null;
        for (int i = 0; i < log.getCurves().size(); i++) {
            if (BINARIES_DATA_CURVE_NAME.equals(log.getCurves().get(i).getName())) {
                binariesDataCurveIndex = i;
            }
        }
        if (binariesDataCurveIndex == null) {
            return;
        }

        List<String> binariesNames = (List<String>) log.getData().get(0).get(binariesNamesCurveIndex);
        List<String> binariesDataElements = (List<String>) log.getData().get(0).get(binariesDataCurveIndex);
        for (int i = 0; i < binariesDataElements.size(); i++) {
            String filePathStr = folderPath + "/" + binariesNames.get(i);
            String base64EncodedData = binariesDataElements.get(i);
            byte[] decodedData = Base64.getDecoder().decode(base64EncodedData);
            try {
                Files.write(Path.of(filePathStr), decodedData);
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        }
    }

    private static Optional<String> findMatch(String text, String regex) {
        Pattern pattern = Pattern.compile(regex);
        Matcher matcher = pattern.matcher(text);
        if (matcher.find()) {
            return Optional.of(matcher.group());
        }
        return Optional.empty();
    }
}
