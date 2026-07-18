package gateway

import (
	"bytes"
	"encoding/json"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"testing"
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
