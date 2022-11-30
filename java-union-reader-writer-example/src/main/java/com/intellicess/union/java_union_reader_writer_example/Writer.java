package com.intellicess.union.java_union_reader_writer_example;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.intellicess.union.java_union_reader_writer_example.client.UnionClient;
import com.intellicess.union.java_union_reader_writer_example.client.UnionClientConfiguration;
import com.intellicess.union.java_union_reader_writer_example.client.model.JwlfRequest;
import com.intellicess.union.java_union_reader_writer_example.client.model.JwlfSavedResponse;
import com.intellicess.union.java_union_reader_writer_example.client.model.JwlfSavedResponseList;
import com.intellicess.union.java_union_reader_writer_example.client.model.JwlfValueType;
import com.intellicess.union.java_union_reader_writer_example.config.DevAppConfiguration;
import com.intellicess.union.java_union_reader_writer_example.config.EnvAppConfiguration;
import com.intellicess.union.java_union_reader_writer_example.utils.AppConfiguration;
import com.intellicess.union.java_union_reader_writer_example.utils.CsvJwlfConverter;
import com.intellicess.union.java_union_reader_writer_example.utils.CsvJwlfWithBase64EncodedBinariesConverter;
import com.intellicess.union.java_union_reader_writer_example.utils.ObjectMapperFactory;
import org.apache.commons.io.FileUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

public class Writer {
    private static final Map<String, JwlfValueType> HEADER_VALUE_TYPE_MAPPING = new HashMap<>() {{
        put("Integer Number 1", JwlfValueType.INTEGER);
        put("Float Number 2", JwlfValueType.FLOAT);
        put("String Text A", JwlfValueType.STRING);
        put("Bit SN", JwlfValueType.INTEGER);
        put("TFA", JwlfValueType.FLOAT);
        put("Bit Size", JwlfValueType.FLOAT);
        put("DI", JwlfValueType.INTEGER);
        put("DO", JwlfValueType.INTEGER);
        put("Run Length", JwlfValueType.INTEGER);
        put("Hours", JwlfValueType.FLOAT);
        put("ROP", JwlfValueType.FLOAT);
    }};


    private static final Logger log = LogManager.getLogger(Writer.class);

    public static void main(String[] args) throws InterruptedException, IOException {
        AppConfiguration config;
        try {
            config = EnvAppConfiguration.init();
        } catch (Exception e) {
            log.warn("Using writer dev configuration because of the following failure in initialization of" +
                    " environment variables based configuration:", e);
            config = DevAppConfiguration.initWriter();
        }
        String writerLocalWorkingDirectory = config.getLocalWorkingDirectory();
        File writerDirectory = new File(writerLocalWorkingDirectory);
        if (!writerDirectory.isDirectory()) {
            throw new RuntimeException("com.intellicess.union.java_union_reader_writer_example.Writer local working directory with path='"
                    + writerLocalWorkingDirectory + "' is not directory");
        }

        ObjectMapper objectMapper = ObjectMapperFactory.createObjectMapper();
        UnionClient unionClient = new UnionClient(
                UnionClientConfiguration.builder()
                        .unionUrl(config.getUnionUrl())
                        .authProviderUrl(config.getAuthProviderUrl())
                        .authProviderRealm(config.getAuthProviderRealm())
                        .authClientId(config.getAuthClientId())
                        .username(config.getAuthUserUsername())
                        .password(config.getAuthUserPassword())
                        .build(),
                objectMapper
        );

        String client = config.getUnionClient();
        String region = config.getUnionRegion();
        String asset = config.getUnionAsset();
        String folder = config.getUnionFolder();

        log.info("Listening to local writer folder...");
        while (!Thread.interrupted()) {
            for (String filename : writerDirectory.list()) {
                Path filePath = Path.of(writerLocalWorkingDirectory, filename);
                File file = filePath.toFile();
                if (filename.endsWith(".csv")) {
                    String csvContent = Files.readString(filePath);
                    JwlfRequest jwlfRequest = CsvJwlfConverter.convertCsvToJwlf(asset, filename, csvContent, HEADER_VALUE_TYPE_MAPPING);

                    String filenameWithoutExtension = filename.replace(".csv", "");
                    jwlfRequest.getHeader().getMetadata().put(CsvJwlfConverter.FILENAME_METADATA_KEY, filenameWithoutExtension);

                    JwlfSavedResponse savedJwlf = unionClient.saveJwlfLog(client, region, asset, folder, jwlfRequest);
                    log.info("JWLF Log from file '{}' got saved with id={}", filename, savedJwlf.getId());
                }
                if (filename.endsWith(".json")) {
                    String jsonContent = Files.readString(filePath);
                    List<JwlfRequest> jwlfRequests = Arrays.asList(objectMapper.readValue(jsonContent, JwlfRequest[].class));

                    String filenameWithoutExtension = filename.replace(".json", "");
                    for (int i = 0; i < jwlfRequests.size(); i++) {
                        JwlfRequest jwlfRequest = jwlfRequests.get(i);
                        jwlfRequest.getHeader().getMetadata().put(CsvJwlfConverter.FILENAME_METADATA_KEY, filenameWithoutExtension + (i + 1));
                    }

                    JwlfSavedResponseList savedJwlfs = unionClient.saveJwlfLogs(client, region, asset, folder, jwlfRequests);
                    log.info("JWLF Logs from file '{}' got saved with ids={}", filename,
                            savedJwlfs.getList().stream().map(JwlfSavedResponse::getId).collect(Collectors.toList()));
                }
                if (file.isDirectory()
                        && CsvJwlfWithBase64EncodedBinariesConverter.BASE64_ENCODED_BINARIES_EXAMPLE_METADATA_KEY.equals(filename)) {
                    CsvJwlfWithBase64EncodedBinariesConverter.convertFolderToJwlf(filePath.toString(), HEADER_VALUE_TYPE_MAPPING)
                            .map(jwlfRequest -> unionClient.saveJwlfLog(client, region, asset, folder, jwlfRequest))
                            .ifPresent(savedJwlf -> log.info("JWLF Log from file '{}' got saved with id={}", filename, savedJwlf.getId()));
                    FileUtils.deleteDirectory(file);
                }
                file.delete();
            }
            Thread.sleep(1000L);
        }
    }
}
