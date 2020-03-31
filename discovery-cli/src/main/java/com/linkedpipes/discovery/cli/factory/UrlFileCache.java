package com.linkedpipes.discovery.cli.factory;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.URL;
import java.util.HashMap;
import java.util.Map;

class UrlFileCache implements UrlCache {

    private static final Logger LOG =
            LoggerFactory.getLogger(UrlFileCache.class);

    private final File root;

    private Map<String, String> cache = new HashMap<>();

    public UrlFileCache(File root) {
        this.root = root;
    }

    @Override
    public void load() throws IOException {
        ObjectMapper objectMapper = new ObjectMapper();
        TypeReference<HashMap<String, String>> typeRef
                = new TypeReference<>() {};
        if (getCacheFile().exists()) {
            cache = objectMapper.readValue(getCacheFile(), typeRef);
        }
    }

    private File getCacheFile() {
        return new File(root, "cache.json");
    }

    @Override
    public InputStream open(URL url) throws IOException {
        String urlAsString = url.toString();
        if (!cache.containsKey(urlAsString)) {
            download(url);
        }
        File file = new File(cache.get(urlAsString));
        return new FileInputStream(file);
    }

    private void download(URL url) throws IOException {
        LOG.info("Downloading URL: {}", url);
        File dataDir = new File(root, "data");
        dataDir.mkdirs();
        File file = new File(dataDir, String.format("%05d", cache.size()));
        try (InputStream inputStream = url.openStream();
                OutputStream outputStream = new FileOutputStream(file)) {
            inputStream.transferTo(outputStream);
        }
        String urlAsString = url.toString();
        cache.put(urlAsString, file.getAbsolutePath());
    }

    @Override
    public void save() throws IOException {
        ObjectMapper objectMapper = new ObjectMapper();
        objectMapper.writeValue(getCacheFile(), cache);
    }

}
