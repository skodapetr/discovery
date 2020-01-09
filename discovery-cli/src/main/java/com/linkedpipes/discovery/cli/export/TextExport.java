package com.linkedpipes.discovery.cli.export;

import com.linkedpipes.discovery.model.Application;
import com.linkedpipes.discovery.model.Transformer;
import com.linkedpipes.discovery.node.Node;

import java.io.File;
import java.io.IOException;
import java.io.PrintWriter;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;

public class TextExport {

    private static class Pipeline {

        public final List<String> transformers;

        public final String application;

        public Pipeline(List<String> transformers, String application) {
            this.transformers = transformers;
            this.application = application;
        }

    }

    public static void export(Node root, File outputFile) throws IOException {
        List<Pipeline> pipelines = new ArrayList<>();
        root.accept((node) -> {
            pipelines.addAll(nodeToPipelines(node));
        });
        try (var writer = new PrintWriter(outputFile, StandardCharsets.UTF_8)) {
            for (Pipeline pipeline : pipelines) {
                writer.write("\"");
                if (!pipeline.transformers.isEmpty()) {
                    writer.write(String.join("\",\"", pipeline.transformers));
                    writer.write("\",\"");
                }
                writer.write(pipeline.application);
                writer.write("\"\n");
            }
        }
    }

    private static List<Pipeline> nodeToPipelines(Node node) {
        if (node.getApplications().isEmpty()) {
            return Collections.emptyList();
        }
        List<Transformer> transformers = collectTransformers(node);
        return node.getApplications().stream()
                .map((app -> createPipeline(transformers, app)))
                .collect(Collectors.toList());
    }

    private static List<Transformer> collectTransformers(Node node) {
        List<Transformer> result = new ArrayList<>();
        Node prev = node;
        while (prev != null) {
            if (prev.getTransformer() != null) {
                result.add(prev.getTransformer());
            }
            prev = prev.getPrevious();
        }
        Collections.reverse(result);
        return result;
    }

    private static Pipeline createPipeline(
            List<Transformer> transformers, Application application) {
        return new Pipeline(
                transformers.stream().map(transformer -> transformer.iri)
                        .collect(Collectors.toList()),
                application.iri);
    }

}
