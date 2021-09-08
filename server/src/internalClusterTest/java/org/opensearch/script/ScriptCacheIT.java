/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.script;

import org.opensearch.action.update.UpdateResponse;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.xcontent.XContentFactory;
import org.opensearch.painless.PainlessPlugin;
import org.opensearch.plugins.Plugin;
import org.opensearch.rest.RestStatus;
import org.opensearch.test.OpenSearchIntegTestCase;

import java.util.*;
import java.util.function.Function;


public class ScriptCacheIT extends OpenSearchIntegTestCase {

    @Override
    protected Settings nodeSettings(int nodeOrdinal) {
          Settings.Builder builder = Settings.builder()
            .put(super.nodeSettings(nodeOrdinal))
            .put(ScriptService.SCRIPT_GENERAL_MAX_COMPILATIONS_RATE_SETTING.getKey(), ScriptService.USE_CONTEXT_RATE_KEY);
          for (String s:ScriptModule.CORE_CONTEXTS.keySet()) {
              builder.put("script.context."+s+".max_compilations_rate", "1/1m");
          }
          return builder.build();
    }

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        return Arrays.asList(CustomScriptPlugin.class);
    }

    public void test() throws Exception {
        client().prepareIndex("test", "1").setId("1")
            .setSource(XContentFactory.jsonBuilder().startObject().field("field", 1).endObject()).get();
        ensureGreen();
        Map<String, Object> params = new HashMap<>();
        params.put("field", "field");

        for (int i = 0; i < 10000; i++) {
            logger.info("Trying iteration for "+i+"th time");
            params.put("inc", i+1);
            Script script = new Script(ScriptType.INLINE, "mockscript", "increase_field", params);
            UpdateResponse updateResponse = client().prepareUpdate("test", "1", "1").setScript(script).execute().actionGet();
            assertEquals(updateResponse.status(), RestStatus.OK);
        }

        //        SearchResponse searchResponse = client().prepareUpdate().setQuery(scriptQuery(new Script(ScriptType.INLINE, "mockscript", "1", Collections.emptyMap()))).get();
        //System.out.println(client().admin().indices().prepareStats().get());
        //assertEquals(RestStatus.TOO_MANY_REQUESTS, searchResponse.status());
//        GetResponse getResponse = client().prepareGet("test", "1", "1").execute().get();
//        System.out.println(updateResponse.status());
    }

    public static class CustomScriptPlugin extends MockScriptPlugin {

        @Override
        protected Map<String, Function<Map<String, Object>, Object>> pluginScripts() {
            Map<String, Function<Map<String, Object>, Object>> scripts = new HashMap<>();
            scripts.put("increase_field", vars -> {
                Map<String, Object> params = (Map<String, Object>) vars.get("params");
                String fieldname = (String) vars.get("field");
                Map<String, Object> ctx = (Map<String, Object>) vars.get("ctx");
                assertNotNull(ctx);
                Map<String, Object> source = (Map<String, Object>) ctx.get("_source");
                Number currentValue = (Number) source.get(fieldname);
                Number inc = (Number) params.getOrDefault("inc", 1);
                source.put(fieldname, currentValue.longValue() + inc.longValue());
                return ctx;
            });
            return scripts;
        }
    }
}
