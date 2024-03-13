/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.upgrades;

import org.opensearch.client.Request;
import org.opensearch.client.Response;

import java.util.logging.LogManager;
import java.util.logging.Logger;

public class ClusterStateIT extends AbstractRollingTestCase{
    private static final Logger logger = LogManager.getLogManager().getLogger("TESSTTTTT");
    public void testTemplateMetadataUpgrades() throws Exception {
        if (CLUSTER_TYPE == ClusterType.OLD) {
            String templateName = "my_template";
            Request putIndexTemplate = new Request("PUT", "_index_template/" + templateName);
            putIndexTemplate.setJsonEntity("{\"index_patterns\": [\"pattern-1\", \"log-*\"]}");
            client().performRequest(putIndexTemplate);
            verifyTemplateMetadataInClusterState();
        } else {
            verifyTemplateMetadataInClusterState();
        }
    }

    private static void verifyTemplateMetadataInClusterState() throws Exception {
        Request request = new Request("GET", "_cluster/state/metadata");
        Response response = client().performRequest(request);

        logger.info(response.toString());
    }
}
