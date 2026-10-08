package com.carrot.hud

import org.json.JSONObject
import org.junit.Assert.*
import org.junit.Test

/** Actual backend-builder JSON with synthetic service inputs, no live phone. */
class HudDualBackendTest {
    private fun payload(name: String) = JSONObject(javaClass.getResourceAsStream("/hud_$name.json")!!
        .bufferedReader().use { it.readText() })

    @Test fun currentAppAcceptsModernPayloadAndKeepsLegacyUnknownHonest() {
        assertEquals(3, ModeApi.parseEffective(payload("modern")))
        assertNull(ModeApi.parseEffective(payload("rollback")))
        val stale = payload("modern").put("snapshotAgeMs", 2000)
        assertNull(ModeApi.parseEffective(stale))
    }

    @Test fun savingModeWorksOnBothLayoutsWithoutInventingEffectiveMode() {
        for (generation in listOf("rollback", "modern")) {
            var saved = 3
            var posts = 0
            val api = ModeApi { method, path, body ->
                when {
                    method == "GET" && path.startsWith("/api/params_bulk") ->
                        JSONObject().put("ok", true).put("values", JSONObject().put("MyDrivingMode", saved))
                    method == "POST" && path == "/api/param_set" -> {
                        val request = JSONObject(body!!)
                        assertEquals("MyDrivingMode", request.getString("name"))
                        saved = request.getInt("value"); posts++
                        JSONObject().put("ok", true).put("has_params", true).put("value", saved)
                    }
                    method == "GET" && path == "/api/live_runtime" -> payload(generation)
                    else -> throw AssertionError("Unexpected request $method $path")
                }
            }
            val result = api.cycle()
            assertEquals(1, posts); assertEquals(4, result.saved); assertTrue(result.changed)
            assertEquals(if (generation == "modern") 3 else null, result.effective)
        }
    }

    @Test fun additiveBackendCapabilitiesDoNotBreakOldUpdatePolicy() {
        val state = JSONObject().put("ok", true).put("configured", true).put("phase", "complete")
            .put("installed", JSONObject().put("sequence", 0).put("release_id", "baseline-75b5d824"))
            .put("latest", JSONObject().put("sequence", 1).put("release_id", "review-1")
                .put("bundle_sha256", "a".repeat(64)))
        val expected = OpUpdatePolicy.available(state)!!.toString()
        state.put("layout", "modern").put("nativeProtocol", 5)
        assertEquals(expected, OpUpdatePolicy.available(state)!!.toString())
        for (phase in OpUpdatePolicy.activePhases) {
            state.put("phase", phase)
            assertNull(OpUpdatePolicy.available(state))
        }
    }
}
