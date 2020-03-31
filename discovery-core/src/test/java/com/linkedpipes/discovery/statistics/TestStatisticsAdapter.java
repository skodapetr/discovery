package com.linkedpipes.discovery.statistics;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.linkedpipes.discovery.Discovery;
import com.linkedpipes.discovery.model.Application;
import com.linkedpipes.discovery.model.Transformer;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;

public class TestStatisticsAdapter {

    @Test
    public void toJsonAndBack() {
        List<Application> applications = new ArrayList<>();
        Application app = new Application();
        app.iri = "urn:app";
        applications.add(app);

        List<Transformer> transformers = new ArrayList<>();
        Transformer transformer = new Transformer();
        transformer.iri = "urn:transformer";
        transformers.add(transformer);

        Discovery discovery = new Discovery(
                null, null, applications, transformers,
                null, null, null, null, null);

        Statistics expected = new Statistics();
        expected.discoveryIri = "urn:discovery";
        Statistics.Level expLevel = new Statistics.Level();
        expLevel.applications.add(app);
        expLevel.transformers.add(transformer);
        expLevel.durationInMilliSeconds = 12;
        expLevel.expandedNodes = 1;
        expLevel.filteredNodes = 2;
        expLevel.level = 0;
        expLevel.newNodes = 3;
        expLevel.startNodes = 1;
        expected.levels.add(expLevel);


        StatisticsAdapter adapter = new StatisticsAdapter();
        Statistics actual = adapter.load(
                discovery,
                adapter.save(expected, new ObjectMapper()));

        Assertions.assertEquals(expected.discoveryIri, actual.discoveryIri);
        Assertions.assertEquals(1, actual.levels.size());

        Statistics.Level actLevel = actual.levels.get(0);
        Assertions.assertEquals(
                expLevel.durationInMilliSeconds,
                actLevel.durationInMilliSeconds);
        Assertions.assertEquals(1, actLevel.applications.size());
        Assertions.assertTrue(actLevel.applications.contains(app));
        Assertions.assertEquals(1, actLevel.transformers.size());
        Assertions.assertTrue(actLevel.transformers.contains(transformer));
        Assertions.assertEquals(expLevel.expandedNodes, actLevel.expandedNodes);
        Assertions.assertEquals(expLevel.filteredNodes, actLevel.filteredNodes);
        Assertions.assertEquals(expLevel.level, actLevel.level);
        Assertions.assertEquals(expLevel.newNodes, actLevel.newNodes);
        Assertions.assertEquals(expLevel.startNodes, actLevel.startNodes);
    }

}
