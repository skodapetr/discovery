package com.linkedpipes.discovery.cli.factory;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.net.URL;

interface UrlCache {

    void load() throws IOException;

    InputStream open(URL url) throws IOException;

    void save() throws IOException;

    static UrlCache fileCache(File directory) {
        return new UrlFileCache(directory);
    }

    static UrlCache noCache() {
        return new UrlCache() {

            @Override
            public void load() {
                // No action.
            }

            @Override
            public InputStream open(URL url) throws IOException {
                return url.openStream();
            }

            @Override
            public void save() {
                // No action.
            }
        };

    }

}
