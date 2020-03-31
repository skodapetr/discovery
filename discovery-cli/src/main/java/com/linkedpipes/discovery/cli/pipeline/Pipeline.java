package com.linkedpipes.discovery.cli.pipeline;

import com.linkedpipes.discovery.SuppressFBWarnings;
import com.linkedpipes.discovery.model.Application;
import com.linkedpipes.discovery.model.Transformer;

import java.util.List;

@SuppressFBWarnings(value = {"URF_UNREAD_PUBLIC_OR_PROTECTED_FIELD"})
public class Pipeline {

    public final DatasetReference dataset;

    public final List<Transformer> transformers;

    public final Application application;

    public Pipeline(
            DatasetReference dataset,
            List<Transformer> transformers,
            Application application) {
        this.dataset = dataset;
        this.transformers = transformers;
        this.application = application;
    }

}
