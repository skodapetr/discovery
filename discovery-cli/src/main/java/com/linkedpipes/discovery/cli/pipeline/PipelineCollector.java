package com.linkedpipes.discovery.cli.pipeline;

import com.linkedpipes.discovery.model.Application;
import com.linkedpipes.discovery.model.Dataset;
import com.linkedpipes.discovery.model.Transformer;
import com.linkedpipes.discovery.node.Node;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

public class PipelineCollector {

    public List<Pipeline> collect(Dataset dataset, Node root) {
        DatasetReference datasetRef = new DatasetReference(dataset);
        List<Pipeline> result = new ArrayList<>();
        root.accept((node) -> {
            if (node.getApplications().isEmpty()) {
                return;
            }
            List<Transformer> transformer = collectTransformers(node);
            for (Application application : node.getApplications()) {
                result.add(new Pipeline(datasetRef, transformer, application));
            }
        });
        return result;
    }

    public List<Transformer> collectTransformers(Node node) {
        List<Transformer> result = new ArrayList<>(node.getLevel());
        while (node != null) {
            if (node.getTransformer() != null) {
                result.add(node.getTransformer());
            }
            node = node.getPrevious();
        }
        Collections.reverse(result);
        return result;
    }

}
