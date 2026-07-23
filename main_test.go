package gateway

import (
	"bytes"
	"encoding/json"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"
)

func testApp() *App                { a := &App{store: newStore(), logger: slogTestLogger()}; a.refresh(); return a }
func slogTestLogger() *slog.Logger { return slog.New(slog.NewTextHandler(io.Discard, nil)) }
func call(t *testing.T, h http.Handler, method, path string, body any) (int, map[string]any) {
	t.Helper()
	var b bytes.Buffer
	if body != nil {
		_ = json.NewEncoder(&b).Encode(body)
	}
	r := httptest.NewRequest(method, path, &b)
	w := httptest.NewRecorder()
	h.ServeHTTP(w, r)
	var out map[string]any
	_ = json.Unmarshal(w.Body.Bytes(), &out)
	return w.Code, out
}

func callWithHeaders(t *testing.T, h http.Handler, method, path string, body any, headers map[string]string) *httptest.ResponseRecorder {
	t.Helper()
	var payload bytes.Buffer
	if body != nil {
		if err := json.NewEncoder(&payload).Encode(body); err != nil {
			t.Fatal(err)
		}
	}
	request := httptest.NewRequest(method, path, &payload)
	for name, value := range headers {
		request.Header.Set(name, value)
	}
	response := httptest.NewRecorder()
	h.ServeHTTP(response, request)
	return response
}

func TestAllEndpointsAndSecretMasking(t *testing.T) {
	a := testApp()
	h := http.HandlerFunc(a.handler)
	if code, _ := call(t, h, "GET", "/healthz", nil); code != 200 {
		t.Fatal(code)
	}
	if code, _ := call(t, h, "GET", "/readyz", nil); code != 200 {
		t.Fatal(code)
	}
	code, account := call(t, h, "POST", "/admin/v1/accounts", map[string]any{"email": "test@example.com"})
	if code != 201 {
		t.Fatal(code, account)
	}
	aid := account["ID"].(string)
	code, provider := call(t, h, "POST", "/admin/v1/providers", map[string]any{"account_id": aid, "name": "mock", "base_url": "mock://test"})
	if code != 201 {
		t.Fatal(code, provider)
	}
	pid := provider["ID"].(string)
	code, key := call(t, h, "POST", "/admin/v1/keys", map[string]any{"provider_id": pid, "label": "test-key", "secret": "super-secret-value"})
	if code != 201 {
		t.Fatal(code, key)
	}
	kid := key["ID"].(string)
	if _, ok := key["Secret"]; ok {
		t.Fatal("secret returned")
	}
	code, model := call(t, h, "POST", "/admin/v1/models", map[string]any{"api_key_id": kid, "logical_name": "demo", "upstream_model": "demo-upstream"})
	if code != 201 {
		t.Fatal(code, model)
	}
	mid := model["ID"].(string)
	for _, p := range []string{"/admin/v1/accounts", "/admin/v1/providers", "/admin/v1/keys", "/admin/v1/models", "/admin/v1/routes"} {
		if code, _ = call(t, h, "GET", p, nil); code != 200 {
			t.Fatalf("list %s: %d", p, code)
		}
	}
	for _, p := range []string{"/admin/v1/accounts/" + aid, "/admin/v1/providers/" + pid, "/admin/v1/keys/" + kid, "/admin/v1/models/" + mid} {
		if code, _ = call(t, h, "GET", p, nil); code != 200 {
			t.Fatalf("get %s: %d", p, code)
		}
	}
	if code, _ = call(t, h, "PATCH", "/admin/v1/models/"+mid, map[string]any{"enabled": true}); code != 200 {
		t.Fatal("patch", code)
	}
	code, response := call(t, h, "POST", "/v1/inference", map[string]any{"messages": []any{map[string]string{"role": "user", "content": "hi"}}})
	if code != 200 {
		t.Fatal("proxy", code)
	}
	if response["model"] != "demo-upstream" {
		t.Fatalf("configured model was not selected: %#v", response)
	}
	if code, _ = call(t, h, "DELETE", "/admin/v1/models/"+mid, nil); code != 200 {
		t.Fatal("delete", code)
	}
	if code, _ = call(t, h, "POST", "/v1/inference", map[string]any{"messages": []any{"after disable"}}); code != 503 {
		t.Fatal("disabled route", code)
	}
}

func TestProtectedEndpointsRequirePasswordAndCORSAllowlist(t *testing.T) {
	a := testApp()
	a.accessPassword = "test-password"
	a.origins = parseAllowedOrigins("https://dashboard.example.com")
	handler := http.HandlerFunc(a.handler)

	if response := callWithHeaders(t, handler, http.MethodGet, "/admin/v1/routes", nil, nil); response.Code != http.StatusUnauthorized {
		t.Fatalf("unauthenticated admin status = %d, want %d", response.Code, http.StatusUnauthorized)
	}
	if response := callWithHeaders(t, handler, http.MethodPost, "/v1/inference", map[string]any{"messages": []any{"test"}}, nil); response.Code != http.StatusUnauthorized {
		t.Fatalf("unauthenticated inference status = %d, want %d", response.Code, http.StatusUnauthorized)
	}
	if response := callWithHeaders(t, handler, http.MethodGet, "/healthz", nil, nil); response.Code != http.StatusOK {
		t.Fatalf("public health status = %d, want %d", response.Code, http.StatusOK)
	}

	authorized := map[string]string{"X-Gateway-Password": "test-password"}
	if response := callWithHeaders(t, handler, http.MethodGet, "/admin/v1/routes", nil, authorized); response.Code != http.StatusOK {
		t.Fatalf("authorized admin status = %d, want %d", response.Code, http.StatusOK)
	}

	allowedPreflight := callWithHeaders(t, handler, http.MethodOptions, "/admin/v1/routes", nil, map[string]string{
		"Origin":                        "https://dashboard.example.com",
		"Access-Control-Request-Method": http.MethodGet,
	})
	if allowedPreflight.Code != http.StatusNoContent {
		t.Fatalf("allowed preflight status = %d, want %d", allowedPreflight.Code, http.StatusNoContent)
	}
	if got := allowedPreflight.Header().Get("Access-Control-Allow-Origin"); got != "https://dashboard.example.com" {
		t.Fatalf("allow origin = %q", got)
	}

	productionPreflight := callWithHeaders(t, handler, http.MethodOptions, "/admin/v1/routes", nil, map[string]string{
		"Origin":                         "https://aigw.connecto-me.com",
		"Access-Control-Request-Method":  http.MethodGet,
		"Access-Control-Request-Headers": "content-type,x-gateway-password",
	})
	if productionPreflight.Code != http.StatusNoContent {
		t.Fatalf("production preflight status = %d, want %d", productionPreflight.Code, http.StatusNoContent)
	}
	if got := productionPreflight.Header().Get("Access-Control-Allow-Origin"); got != "https://aigw.connecto-me.com" {
		t.Fatalf("production allow origin = %q", got)
	}

	blockedPreflight := callWithHeaders(t, handler, http.MethodOptions, "/admin/v1/routes", nil, map[string]string{
		"Origin":                        "https://attacker.example.com",
		"Access-Control-Request-Method": http.MethodGet,
	})
	if blockedPreflight.Code != http.StatusForbidden {
		t.Fatalf("blocked preflight status = %d, want %d", blockedPreflight.Code, http.StatusForbidden)
	}
}

func TestRefreshNormalizesOpenRouterAndGeminiModels(t *testing.T) {
	a := testApp()
	created := now()
	a.store.accounts["acct_models"] = Account{
		ID: "acct_models", Email: "models@example.com", Enabled: true, CreatedAt: created,
	}
	a.store.providers["prov_openrouter"] = Provider{
		ID: "prov_openrouter", AccountID: "acct_models", Name: "OpenRouter",
		BaseURL: "https://openrouter.ai/api/v1", AdapterType: "openai_compatible",
		Enabled: true, CreatedAt: created,
	}
	a.store.providers["prov_gemini"] = Provider{
		ID: "prov_gemini", AccountID: "acct_models", Name: "free-gemini",
		BaseURL: "https://generativelanguage.googleapis.com", AdapterType: "openai_compatible",
		Enabled: true, CreatedAt: created,
	}
	a.store.keys["key_openrouter"] = APIKey{
		ID: "key_openrouter", ProviderID: "prov_openrouter", Enabled: true,
	}
	a.store.keys["key_gemini"] = APIKey{
		ID: "key_gemini", ProviderID: "prov_gemini", Enabled: true,
	}
	a.store.models["model_openrouter"] = Model{
		ID: "model_openrouter", APIKeyID: "key_openrouter",
		LogicalName: "removed/free-model", UpstreamModel: "removed/free-model", Enabled: true,
	}
	a.store.models["model_gemini"] = Model{
		ID: "model_gemini", APIKeyID: "key_gemini",
		LogicalName: "gemini-2.0-flash", UpstreamModel: "gemini-2.0-flash", Enabled: true,
	}

	a.refresh()

	openRouter := a.store.models["model_openrouter"]
	if openRouter.LogicalName != openRouterFreeModel || openRouter.UpstreamModel != openRouterFreeModel {
		t.Fatalf("OpenRouter model = %#v, want %q", openRouter, openRouterFreeModel)
	}
	gemini := a.store.models["model_gemini"]
	if gemini.LogicalName != geminiLiteModel || gemini.UpstreamModel != geminiLiteModel {
		t.Fatalf("Gemini model = %#v, want %q", gemini, geminiLiteModel)
	}
}

func appWithUpstream(t *testing.T, upstreamURL string, logger *slog.Logger) (*App, http.Handler) {
	t.Helper()
	a := &App{store: newStore(), logger: logger}
	created := now()
	a.store.accounts["acct_test"] = Account{ID: "acct_test", Email: "test@example.com", Enabled: true, CreatedAt: created}
	a.store.providers["prov_test"] = Provider{
		ID: "prov_test", AccountID: "acct_test", Name: "test-upstream",
		BaseURL: upstreamURL, AdapterType: "openai_compatible", Enabled: true, CreatedAt: created,
	}
	a.store.keys["key_test"] = APIKey{
		ID: "key_test", ProviderID: "prov_test", Label: "test", Secret: "test-secret",
		Fingerprint: fingerprint("test-secret"), Enabled: true,
	}
	a.store.models["model_test"] = Model{
		ID: "model_test", APIKeyID: "key_test", LogicalName: "test-model",
		UpstreamModel: "test-model", Enabled: true,
	}
	a.refresh()
	return a, http.HandlerFunc(a.handler)
}

func TestEveryUpstreamHTTPErrorStartsThirtyMinuteCooldown(t *testing.T) {
	for _, status := range []int{400, 401, 403, 404, 429, 500, 503} {
		t.Run(http.StatusText(status), func(t *testing.T) {
			upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
				http.Error(w, "provider error", status)
			}))
			defer upstream.Close()

			var logs bytes.Buffer
			logger := slog.New(slog.NewJSONHandler(&logs, nil))
			a, handler := appWithUpstream(t, upstream.URL, logger)
			before := now()
			code, _ := call(t, handler, http.MethodPost, "/v1/inference", map[string]any{
				"messages": []any{map[string]string{"role": "user", "content": "test"}},
			})
			if code != status {
				t.Fatalf("response status = %d, want %d", code, status)
			}

			key := a.store.keys["key_test"]
			if key.SuspendedUntil == nil {
				t.Fatal("failing key was not suspended")
			}
			minimum := before.Add(apiKeyErrorCooldown - time.Second)
			maximum := before.Add(apiKeyErrorCooldown + time.Second)
			if key.SuspendedUntil.Before(minimum) || key.SuspendedUntil.After(maximum) {
				t.Fatalf("suspended until %v, want approximately %v", key.SuspendedUntil, before.Add(apiKeyErrorCooldown))
			}
			if _, ok := a.selectDefaultRoute(); ok {
				t.Fatal("suspended key remained eligible for routing")
			}
			logText := logs.String()
			if !strings.Contains(logText, `"msg":"upstream API key error; cooldown started"`) ||
				!strings.Contains(logText, `"cooldown_minutes":30`) ||
				!strings.Contains(logText, `"status_code":`+strings.TrimSpace(string(mustJSON(t, status)))) {
				t.Fatalf("cooldown error was not fully logged: %s", logText)
			}
			if strings.Contains(logText, "test-secret") {
				t.Fatal("API key secret leaked into logs")
			}
		})
	}
}

func mustJSON(t *testing.T, value any) []byte {
	t.Helper()
	b, err := json.Marshal(value)
	if err != nil {
		t.Fatal(err)
	}
	return b
}

func TestNetworkErrorStartsCooldownAndExpiredKeyReactivates(t *testing.T) {
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		jsonWrite(w, http.StatusOK, map[string]string{"status": "ok"})
	}))
	upstreamURL := upstream.URL
	upstream.Close()

	var logs bytes.Buffer
	a, handler := appWithUpstream(t, upstreamURL, slog.New(slog.NewJSONHandler(&logs, nil)))
	code, _ := call(t, handler, http.MethodPost, "/v1/inference", map[string]any{"messages": []any{"test"}})
	if code != http.StatusBadGateway {
		t.Fatalf("network-error response = %d, want %d", code, http.StatusBadGateway)
	}
	if a.store.keys["key_test"].SuspendedUntil == nil {
		t.Fatal("network error did not suspend key")
	}
	if !strings.Contains(logs.String(), `"error_type":"network_error"`) {
		t.Fatalf("network error was not logged: %s", logs.String())
	}

	expired := now().Add(-time.Second)
	key := a.store.keys["key_test"]
	key.SuspendedUntil = &expired
	a.store.keys["key_test"] = key
	if route, ok := a.selectDefaultRoute(); !ok || route.Key.ID != "key_test" {
		t.Fatal("expired key was not automatically returned to routing")
	}
	if a.store.keys["key_test"].SuspendedUntil != nil {
		t.Fatal("expired suspension was not cleared")
	}
}

func TestBynaraEnvironmentBootstrapIsIdempotent(t *testing.T) {
	t.Setenv("BYNARA_CONNECTO_API_KEY", "connecto-test-secret")
	t.Setenv("BYNARA_SELLERS_API_KEY", "sellers-test-secret")
	a := testApp()
	a.store.accounts["existing_account"] = Account{
		ID: "existing_account", Email: "connecto.meets@gmail.com", Enabled: true, CreatedAt: now(),
	}
	a.store.providers["existing_provider"] = Provider{
		ID: "existing_provider", AccountID: "existing_account", Name: "Bynara",
		BaseURL: "https://old.example/v1", AdapterType: "openai_compatible", Enabled: true, CreatedAt: now(),
	}

	if routes := bootstrapBynaraAccounts(a); routes != 4 {
		t.Fatalf("configured routes = %d, want 4", routes)
	}
	a.refresh()
	if got := len(a.snapshot.Load().Routes["mistral-large"]); got != 2 {
		t.Fatalf("mistral routes = %d, want 2", got)
	}
	if got := len(a.snapshot.Load().Routes["nemotron-3-ultra"]); got != 2 {
		t.Fatalf("nemotron routes = %d, want 2", got)
	}
	if _, ok := a.store.accounts["acct_bynara_connecto"]; ok {
		t.Fatal("bootstrap duplicated an account with an existing email")
	}
	if got := a.store.providers["existing_provider"].BaseURL; got != "https://router.bynara.id/v1" {
		t.Fatalf("existing provider URL = %q", got)
	}

	key := a.store.keys["key_bynara_connecto"]
	key.UsageCount = 7
	a.store.keys[key.ID] = key
	if routes := bootstrapBynaraAccounts(a); routes != 4 {
		t.Fatalf("second configured routes = %d, want 4", routes)
	}
	if got := a.store.keys[key.ID].UsageCount; got != 7 {
		t.Fatalf("bootstrap reset usage count to %d", got)
	}
	if len(a.store.accounts) != 2 || len(a.store.providers) != 2 || len(a.store.keys) != 2 || len(a.store.models) != 4 {
		t.Fatalf("bootstrap created duplicates: accounts=%d providers=%d keys=%d models=%d",
			len(a.store.accounts), len(a.store.providers), len(a.store.keys), len(a.store.models))
	}
}
