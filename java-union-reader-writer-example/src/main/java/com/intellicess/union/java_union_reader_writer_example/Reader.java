package com.intellicess.union.java_union_reader_writer_example;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.util.DefaultIndenter;
import com.fasterxml.jackson.core.util.DefaultPrettyPrinter;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.intellicess.union.java_union_reader_writer_example.client.UnionClient;
import com.intellicess.union.java_union_reader_writer_example.client.UnionClientConfiguration;
import com.intellicess.union.java_union_reader_writer_example.client.model.JwlfResponse;
import com.intellicess.union.java_union_reader_writer_example.client.model.JwlfResponseList;
import com.intellicess.union.java_union_reader_writer_example.config.DevAppConfiguration;
import com.intellicess.union.java_union_reader_writer_example.config.EnvAppConfiguration;
import com.intellicess.union.java_union_reader_writer_example.utils.*;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.util.List;
import java.util.Optional;

public class Reader {

    private static final Logger log = LogManager.getLogger(Reader.class);

    public static void main(String[] args) throws InterruptedException, IOException {
        AppConfiguration config;
        try {
            config = EnvAppConfiguration.init();
        } catch (Exception e) {
            log.warn("Using reader dev configuration because of the following failure in initialization of" +
                    " environment variables based configuration:", e);
            config = DevAppConfiguration.initReader();
        }

        String readerLocalWorkingDirectory = config.getLocalWorkingDirectory();
        File readerDirectory = new File(readerLocalWorkingDirectory);
        if (!readerDirectory.isDirectory()) {
            throw new RuntimeException("com.intellicess.union.java_union_reader_writer_example.Reader local working directory with path='" + readerLocalWorkingDirectory + "' is not directory");
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

        // if the following variable is set to null, it will read all data that was saved so far in given data space (i.e. in client/region/asset/folder)
        Long inclusiveSinceTimestamp = TimeUtils.dateTimeToMillisecondsTimestamp(LocalDateTime.now(ZoneOffset.UTC)) - 5000L;
        log.info("Listening to '{}/{}/{}/{}' data space in Union since timestamp='{}'...",
                client, region, asset, folder, inclusiveSinceTimestamp);
        while (!Thread.interrupted()) {
            JwlfResponseList responseList = unionClient.getNewJwlfLogsWithData(client, region, asset, folder, inclusiveSinceTimestamp);
            List<JwlfResponse> jwlfLogs = responseList.getList();
            for (JwlfResponse jwlfLog : jwlfLogs) {
                String filename = (String) jwlfLog.getHeader().getMetadata().get(CsvJwlfConverter.FILENAME_METADATA_KEY);

                boolean base64EncodedBinariesExample = Optional.ofNullable(jwlfLog
                        .getHeader()
                        .getMetadata()
                        .get(CsvJwlfWithBase64EncodedBinariesConverter.BASE64_ENCODED_BINARIES_EXAMPLE_METADATA_KEY))
                        .map(o -> Boolean.parseBoolean(o.toString()))
                        .orElse(false);
                if (base64EncodedBinariesExample) {
                    CsvJwlfWithBase64EncodedBinariesConverter.convertJwlfToFolder(readerLocalWorkingDirectory, jwlfLog);
                }

                if (!filename.endsWith(".json")) {
                    filename += ".json";
                }
                Path filePath = Path.of(readerLocalWorkingDirectory, filename);
                String json = toPrettyJson(objectMapper, jwlfLog);
                Files.writeString(filePath, json, StandardOpenOption.CREATE);
                log.info("Pulled '{}' and saved in local reader folder", filename);
            }
            Thread.sleep(1000L);
        }
    }

    private static String toPrettyJson(ObjectMapper objectMapper, JwlfResponse jwlfLog) throws JsonProcessingException {
        DefaultPrettyPrinter.Indenter indenter = new DefaultIndenter("    ", DefaultIndenter.SYS_LF);
        DefaultPrettyPrinter printer = new DefaultPrettyPrinter();
        printer.indentObjectsWith(indenter);
        printer.indentArraysWith(indenter);
        return objectMapper.writer(printer).writeValueAsString(jwlfLog);
    }
}
