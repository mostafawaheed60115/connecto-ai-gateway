package gateway

import (
	"context"
	"crypto/sha256"
	"database/sql"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"net/url"
	"os"
	"regexp"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"ai-gateway/internal/config"
	"ai-gateway/internal/domain"
	"ai-gateway/internal/logging"
	"ai-gateway/internal/routing"
	_ "github.com/jackc/pgx/v5/stdlib"
	"github.com/redis/go-redis/v9"
)

type Account = domain.Account
type Provider = domain.Provider
type APIKey = domain.APIKey
type Model = domain.Model
type Route = domain.Route
type Store struct {
	mu        sync.RWMutex
	accounts  map[string]Account
	providers map[string]Provider
	keys      map[string]APIKey
	models    map[string]Model
}
type App struct {
	store      *Store
	snapshot   atomic.Pointer[routing.Snapshot]
	logger     *slog.Logger
	requestSeq atomic.Uint64
	rrMu       sync.Mutex
	db         *sql.DB
	redis      *redis.Client
}

func now() time.Time                    { return time.Now().UTC() }
func id(prefix string, n uint64) string { return fmt.Sprintf("%s_%d", prefix, n) }
func fingerprint(secret string) string {
	h := sha256.Sum256([]byte(secret))
	return hex.EncodeToString(h[:])[:12]
}
func jsonWrite(w http.ResponseWriter, status int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(v)
}
func decode(r *http.Request, v any) error {
	b, err := io.ReadAll(io.LimitReader(r.Body, 1<<20))
	if err != nil {
		return err
	}
	if len(b) == 0 {
		return errors.New("empty JSON body")
	}
	return json.Unmarshal(b, v)
}

func newStore() *Store {
	return &Store{accounts: map[string]Account{}, providers: map[string]Provider{}, keys: map[string]APIKey{}, models: map[string]Model{}}
}
func (s *Store) rebuild() *routing.Snapshot {
	s.mu.RLock()
	defer s.mu.RUnlock()
	var routes []Route
	for _, m := range s.models {
		k, ok := s.keys[m.APIKeyID]
		if !ok || !m.Enabled || !k.Enabled {
			continue
		}
		p, ok := s.providers[k.ProviderID]
		if !ok || !p.Enabled {
			continue
		}
		a, ok := s.accounts[p.AccountID]
		if !ok || !a.Enabled {
			continue
		}
		if k.SuspendedUntil != nil && k.SuspendedUntil.After(now()) {
			continue
		}
		routes = append(routes, Route{Account: a, Provider: p, Key: k, Model: m})
	}
	return routing.Build(routes, uint64(time.Now().UnixNano()))
}
func (a *App) refresh() { a.snapshot.Store(a.store.rebuild()); a.syncPersistence() }

func (a *App) syncPersistence() {
	if a.db != nil {
		ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancel()
		tx, err := a.db.BeginTx(ctx, nil)
		if err == nil {
			_, err = tx.ExecContext(ctx, `CREATE TABLE IF NOT EXISTS accounts (id text PRIMARY KEY,email text NOT NULL UNIQUE,enabled boolean NOT NULL DEFAULT true,created_at timestamptz NOT NULL DEFAULT now()); CREATE TABLE IF NOT EXISTS providers (id text PRIMARY KEY,account_id text NOT NULL REFERENCES accounts(id),name text NOT NULL,base_url text NOT NULL,adapter_type text NOT NULL DEFAULT 'openai_compatible',enabled boolean NOT NULL DEFAULT true,created_at timestamptz NOT NULL DEFAULT now()); CREATE TABLE IF NOT EXISTS api_keys (id text PRIMARY KEY,provider_id text NOT NULL REFERENCES providers(id),label text NOT NULL,secret_ciphertext text NOT NULL,fingerprint text NOT NULL,enabled boolean NOT NULL DEFAULT true,suspended_until timestamptz,usage_count bigint NOT NULL DEFAULT 0,last_used_at timestamptz); CREATE TABLE IF NOT EXISTS models (id text PRIMARY KEY,api_key_id text NOT NULL REFERENCES api_keys(id),logical_name text NOT NULL,upstream_model text NOT NULL,enabled boolean NOT NULL DEFAULT true,usage_count bigint NOT NULL DEFAULT 0,last_used_at timestamptz);`)
		}
		if err == nil {
			a.store.mu.RLock()
			defer a.store.mu.RUnlock()
			for _, v := range a.store.accounts {
				if err != nil {
					break
				}
				_, err = tx.ExecContext(ctx, "INSERT INTO accounts(id,email,enabled,created_at) VALUES($1,$2,$3,$4) ON CONFLICT(id) DO UPDATE SET email=$2,enabled=$3", v.ID, v.Email, v.Enabled, v.CreatedAt)
			}
			for _, v := range a.store.providers {
				if err != nil {
					break
				}
				_, err = tx.ExecContext(ctx, "INSERT INTO providers(id,account_id,name,base_url,adapter_type,enabled,created_at) VALUES($1,$2,$3,$4,$5,$6,$7) ON CONFLICT(id) DO UPDATE SET name=$3,base_url=$4,enabled=$6", v.ID, v.AccountID, v.Name, v.BaseURL, v.AdapterType, v.Enabled, v.CreatedAt)
			}
			for _, v := range a.store.keys {
				if err != nil {
					break
				}
				_, err = tx.ExecContext(ctx, "INSERT INTO api_keys(id,provider_id,label,secret_ciphertext,fingerprint,enabled,suspended_until,usage_count,last_used_at) VALUES($1,$2,$3,$4,$5,$6,$7,$8,$9) ON CONFLICT(id) DO UPDATE SET label=$3,secret_ciphertext=$4,enabled=$6,suspended_until=$7,usage_count=$8,last_used_at=$9", v.ID, v.ProviderID, v.Label, v.Secret, v.Fingerprint, v.Enabled, v.SuspendedUntil, v.UsageCount, v.LastUsedAt)
			}
			for _, v := range a.store.models {
				if err != nil {
					break
				}
				_, err = tx.ExecContext(ctx, "INSERT INTO models(id,api_key_id,logical_name,upstream_model,enabled,usage_count,last_used_at) VALUES($1,$2,$3,$4,$5,$6,$7) ON CONFLICT(id) DO UPDATE SET logical_name=$3,upstream_model=$4,enabled=$5,usage_count=$6,last_used_at=$7", v.ID, v.APIKeyID, v.LogicalName, v.UpstreamModel, v.Enabled, v.UsageCount, v.LastUsedAt)
			}
		}
		if err == nil {
			err = tx.Commit()
		} else {
			_ = tx.Rollback()
		}
		if err != nil {
			a.logger.Error("postgres persistence failed", "error", err)
		}
	}
	if a.redis != nil {
		if b, err := json.Marshal(a.snapshot.Load()); err == nil {
			ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
			defer cancel()
			if err = a.redis.Set(ctx, "gateway:routing:v1", b, 0).Err(); err != nil {
				a.logger.Error("redis cache update failed", "error", err)
			}
		}
	}
}
func (a *App) selectRoute(model string) (Route, bool) {
	s := a.snapshot.Load()
	if s == nil {
		return Route{}, false
	}
	rs := s.Routes[strings.ToLower(model)]
	if len(rs) == 0 {
		return Route{}, false
	}
	return s.Select(model)
}

// selectDefaultRoute selects the next eligible configured route globally.
func (a *App) selectDefaultRoute() (Route, bool) {
	s := a.snapshot.Load()
	if s == nil {
		return Route{}, false
	}
	return s.Default()
}

func (a *App) requestID(r *http.Request) string {
	if x := r.Header.Get("X-Request-ID"); x != "" {
		return x
	}
	return fmt.Sprintf("req_%d", a.requestSeq.Add(1))
}
func (a *App) routes(w http.ResponseWriter) {
	s := a.snapshot.Load()
	type safe struct{ ID, AccountID, AccountEmail, ProviderID, ProviderName, KeyID, KeyLabel, ModelID, Model string }
	var out []safe
	if s != nil {
		for _, rs := range s.Routes {
			for _, r := range rs {
				out = append(out, safe{r.Account.ID, r.Account.ID, r.Account.Email, r.Provider.ID, r.Provider.Name, r.Key.ID, r.Key.Label, r.Model.ID, r.Model.LogicalName})
			}
		}
	}
	jsonWrite(w, 200, map[string]any{"version": s.Version, "routes": out})
}

type accountInput struct {
	Email   string `json:"email"`
	Enabled *bool  `json:"enabled"`
}

func (a *App) accounts(w http.ResponseWriter, r *http.Request) {
	switch r.Method {
	case "GET":
		a.store.mu.RLock()
		out := make([]Account, 0, len(a.store.accounts))
		for _, v := range a.store.accounts {
			out = append(out, v)
		}
		a.store.mu.RUnlock()
		jsonWrite(w, 200, out)
	case "POST":
		var in accountInput
		if decode(r, &in) != nil || in.Email == "" {
			jsonWrite(w, 400, map[string]string{"error": "email is required"})
			return
		}
		en := true
		if in.Enabled != nil {
			en = *in.Enabled
		}
		v := Account{ID: id("acct", a.requestSeq.Add(1)), Email: in.Email, Enabled: en, CreatedAt: now()}
		a.store.mu.Lock()
		a.store.accounts[v.ID] = v
		a.store.mu.Unlock()
		a.refresh()
		jsonWrite(w, 201, v)
	default:
		jsonWrite(w, 405, map[string]string{"error": "method not allowed"})
	}
}
func (a *App) providers(w http.ResponseWriter, r *http.Request) {
	if r.Method == "GET" {
		a.store.mu.RLock()
		out := make([]Provider, 0)
		for _, v := range a.store.providers {
			out = append(out, v)
		}
		a.store.mu.RUnlock()
		jsonWrite(w, 200, out)
		return
	}
	if r.Method != "POST" {
		jsonWrite(w, 405, nil)
		return
	}
	var in struct {
		AccountID   string `json:"account_id"`
		Name        string `json:"name"`
		BaseURL     string `json:"base_url"`
		AdapterType string `json:"adapter_type"`
		Enabled     *bool  `json:"enabled"`
	}
	if decode(r, &in) != nil || in.AccountID == "" || in.Name == "" {
		jsonWrite(w, 400, map[string]string{"error": "account_id and name are required"})
		return
	}
	en := true
	if in.Enabled != nil {
		en = *in.Enabled
	}
	v := Provider{ID: id("prov", a.requestSeq.Add(1)), AccountID: in.AccountID, Name: in.Name, BaseURL: in.BaseURL, AdapterType: in.AdapterType, Enabled: en, CreatedAt: now()}
	if v.AdapterType == "" {
		v.AdapterType = "openai_compatible"
	}
	a.store.mu.Lock()
	if _, ok := a.store.accounts[v.AccountID]; !ok {
		a.store.mu.Unlock()
		jsonWrite(w, 404, map[string]string{"error": "account not found"})
		return
	}
	a.store.providers[v.ID] = v
	a.store.mu.Unlock()
	a.refresh()
	jsonWrite(w, 201, v)
}
func (a *App) keys(w http.ResponseWriter, r *http.Request) {
	if r.Method == "GET" {
		a.store.mu.RLock()
		out := make([]APIKey, 0)
		for _, v := range a.store.keys {
			v.Secret = ""
			v.Fingerprint = ""
			out = append(out, v)
		}
		a.store.mu.RUnlock()
		jsonWrite(w, 200, out)
		return
	}
	if r.Method != "POST" {
		jsonWrite(w, 405, nil)
		return
	}
	var in struct {
		ProviderID string `json:"provider_id"`
		Label      string `json:"label"`
		Secret     string `json:"secret"`
		Enabled    *bool  `json:"enabled"`
	}
	if decode(r, &in) != nil || in.ProviderID == "" || in.Secret == "" {
		jsonWrite(w, 400, map[string]string{"error": "provider_id and secret are required"})
		return
	}
	en := true
	if in.Enabled != nil {
		en = *in.Enabled
	}
	v := APIKey{ID: id("key", a.requestSeq.Add(1)), ProviderID: in.ProviderID, Label: in.Label, Secret: in.Secret, Fingerprint: fingerprint(in.Secret), Enabled: en}
	a.store.mu.Lock()
	if _, ok := a.store.providers[v.ProviderID]; !ok {
		a.store.mu.Unlock()
		jsonWrite(w, 404, map[string]string{"error": "provider not found"})
		return
	}
	a.store.keys[v.ID] = v
	a.store.mu.Unlock()
	v.Secret = ""
	a.refresh()
	jsonWrite(w, 201, v)
}
func (a *App) models(w http.ResponseWriter, r *http.Request) {
	if r.Method == "GET" {
		a.store.mu.RLock()
		out := make([]Model, 0)
		for _, v := range a.store.models {
			out = append(out, v)
		}
		a.store.mu.RUnlock()
		jsonWrite(w, 200, out)
		return
	}
	if r.Method != "POST" {
		jsonWrite(w, 405, nil)
		return
	}
	var in struct {
		APIKeyID      string `json:"api_key_id"`
		LogicalName   string `json:"logical_name"`
		UpstreamModel string `json:"upstream_model"`
		Enabled       *bool  `json:"enabled"`
	}
	if decode(r, &in) != nil || in.APIKeyID == "" || in.LogicalName == "" {
		jsonWrite(w, 400, map[string]string{"error": "api_key_id and logical_name are required"})
		return
	}
	en := true
	if in.Enabled != nil {
		en = *in.Enabled
	}
	if in.UpstreamModel == "" {
		in.UpstreamModel = in.LogicalName
	}
	v := Model{ID: id("model", a.requestSeq.Add(1)), APIKeyID: in.APIKeyID, LogicalName: in.LogicalName, UpstreamModel: in.UpstreamModel, Enabled: en}
	a.store.mu.Lock()
	if _, ok := a.store.keys[v.APIKeyID]; !ok {
		a.store.mu.Unlock()
		jsonWrite(w, 404, map[string]string{"error": "key not found"})
		return
	}
	a.store.models[v.ID] = v
	a.store.mu.Unlock()
	a.refresh()
	jsonWrite(w, 201, v)
}

func (a *App) item(w http.ResponseWriter, r *http.Request, kind, entityID string) {
	if entityID == "" {
		jsonWrite(w, 400, map[string]string{"error": "id is required"})
		return
	}
	if r.Method == "GET" {
		a.store.mu.RLock()
		defer a.store.mu.RUnlock()
		var v any
		switch kind {
		case "accounts":
			v = a.store.accounts[entityID]
		case "providers":
			v = a.store.providers[entityID]
		case "keys":
			x, ok := a.store.keys[entityID]
			if !ok {
				jsonWrite(w, 404, map[string]string{"error": "not found"})
				return
			}
			x.Secret = ""
			x.Fingerprint = ""
			v = x
		case "models":
			v = a.store.models[entityID]
		}
		if v == nil {
			jsonWrite(w, 404, map[string]string{"error": "not found"})
			return
		}
		jsonWrite(w, 200, v)
		return
	}
	if r.Method == "DELETE" {
		a.store.mu.Lock()
		found := false
		switch kind {
		case "accounts":
			if v, ok := a.store.accounts[entityID]; ok {
				v.Enabled = false
				a.store.accounts[entityID] = v
				found = true
			}
		case "providers":
			if v, ok := a.store.providers[entityID]; ok {
				v.Enabled = false
				a.store.providers[entityID] = v
				found = true
			}
		case "keys":
			if v, ok := a.store.keys[entityID]; ok {
				v.Enabled = false
				a.store.keys[entityID] = v
				found = true
			}
		case "models":
			if v, ok := a.store.models[entityID]; ok {
				v.Enabled = false
				a.store.models[entityID] = v
				found = true
			}
		}
		a.store.mu.Unlock()
		if !found {
			jsonWrite(w, 404, map[string]string{"error": "not found"})
			return
		}
		a.refresh()
		jsonWrite(w, 200, map[string]string{"status": "disabled", "id": entityID})
		return
	}
	if r.Method == "PATCH" {
		var in map[string]any
		if decode(r, &in) != nil {
			jsonWrite(w, 400, map[string]string{"error": "invalid JSON"})
			return
		}
		a.store.mu.Lock()
		found := false
		enabled, hasEnabled := in["enabled"].(bool)
		switch kind {
		case "accounts":
			if v, ok := a.store.accounts[entityID]; ok {
				if x, ok := in["email"].(string); ok && x != "" {
					v.Email = x
				}
				if hasEnabled {
					v.Enabled = enabled
				}
				a.store.accounts[entityID] = v
				found = true
			}
		case "providers":
			if v, ok := a.store.providers[entityID]; ok {
				if x, ok := in["name"].(string); ok && x != "" {
					v.Name = x
				}
				if x, ok := in["base_url"].(string); ok {
					v.BaseURL = x
				}
				if hasEnabled {
					v.Enabled = enabled
				}
				a.store.providers[entityID] = v
				found = true
			}
		case "keys":
			if v, ok := a.store.keys[entityID]; ok {
				if x, ok := in["label"].(string); ok && x != "" {
					v.Label = x
				}
				if x, ok := in["secret"].(string); ok && x != "" {
					v.Secret = x
					v.Fingerprint = fingerprint(x)
				}
				if hasEnabled {
					v.Enabled = enabled
				}
				a.store.keys[entityID] = v
				found = true
			}
		case "models":
			if v, ok := a.store.models[entityID]; ok {
				if x, ok := in["logical_name"].(string); ok && x != "" {
					v.LogicalName = x
				}
				if x, ok := in["upstream_model"].(string); ok && x != "" {
					v.UpstreamModel = x
				}
				if hasEnabled {
					v.Enabled = enabled
				}
				a.store.models[entityID] = v
				found = true
			}
		}
		a.store.mu.Unlock()
		if !found {
			jsonWrite(w, 404, map[string]string{"error": "not found"})
			return
		}
		a.refresh()
		a.item(w, &http.Request{Method: "GET"}, kind, entityID)
		return
	}
	jsonWrite(w, 405, map[string]string{"error": "method not allowed"})
}

func (a *App) suspend(keyID string) {
	until := now().Add(time.Hour)
	a.store.mu.Lock()
	if k, ok := a.store.keys[keyID]; ok {
		k.SuspendedUntil = &until
		a.store.keys[keyID] = k
	}
	a.store.mu.Unlock()
	a.refresh()
}
func (a *App) proxy(w http.ResponseWriter, r *http.Request) {
	reqID := a.requestID(r)
	var in struct {
		Messages []any `json:"messages"`
		Stream   bool  `json:"stream"`
	}
	if decode(r, &in) != nil || len(in.Messages) == 0 {
		jsonWrite(w, 400, map[string]any{"error": map[string]string{"code": "invalid_request", "message": "messages are required"}, "request_id": reqID})
		return
	}
	route, ok := a.selectDefaultRoute()
	if !ok {
		jsonWrite(w, 503, map[string]any{"error": map[string]string{"code": "no_eligible_route", "message": "no enabled route is available"}, "request_id": reqID})
		return
	}
	a.store.mu.Lock()
	k := a.store.keys[route.Key.ID]
	k.UsageCount++
	t := now()
	k.LastUsedAt = &t
	a.store.keys[k.ID] = k
	m := a.store.models[route.Model.ID]
	m.UsageCount++
	m.LastUsedAt = &t
	a.store.models[m.ID] = m
	a.store.mu.Unlock()
	// Log stable identifiers only; never emit key labels, fingerprints, or secrets.
	a.logger.Info("proxy route selected", "request_id", reqID, "account", route.Account.ID, "provider", route.Provider.Name, "key_id", route.Key.ID, "model", route.Model.LogicalName)
	// A provider with no base URL is useful for local smoke tests and returns a deterministic response.
	if route.Provider.BaseURL == "" || strings.HasPrefix(route.Provider.BaseURL, "mock://") {
		jsonWrite(w, 200, map[string]any{"id": reqID, "object": "inference", "model": route.Model.UpstreamModel, "choices": []any{map[string]any{"index": 0, "message": map[string]string{"role": "assistant", "content": "mock response"}, "finish_reason": "stop"}}})
		return
	}
	b, _ := json.Marshal(map[string]any{"model": route.Model.UpstreamModel, "messages": in.Messages, "stream": in.Stream})
	ctx, cancel := context.WithTimeout(r.Context(), 60*time.Second)
	defer cancel()
	up, err := http.NewRequestWithContext(ctx, http.MethodPost, strings.TrimRight(route.Provider.BaseURL, "/")+"/chat/completions", strings.NewReader(string(b)))
	if err != nil {
		jsonWrite(w, 502, map[string]string{"error": "upstream request failed"})
		return
	}
	up.Header.Set("Content-Type", "application/json")
	secret := route.Key.Secret
	if strings.HasPrefix(strings.ToLower(secret), "key:") {
		secret = strings.TrimSpace(secret[len("key:"):])
	}
	up.Header.Set("Authorization", "Bearer "+secret)
	resp, err := http.DefaultClient.Do(up)
	if err != nil {
		jsonWrite(w, 502, map[string]string{"error": "upstream unavailable"})
		return
	}
	defer resp.Body.Close()
	if resp.StatusCode == 429 {
		a.suspend(route.Key.ID)
	}
	w.Header().Set("Content-Type", resp.Header.Get("Content-Type"))
	w.WriteHeader(resp.StatusCode)
	_, _ = io.Copy(w, io.LimitReader(resp.Body, 10<<20))
}

func (a *App) reloadRouting(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		jsonWrite(w, http.StatusMethodNotAllowed, map[string]string{"error": "method not allowed"})
		return
	}
	if a.db == nil {
		jsonWrite(w, http.StatusServiceUnavailable, map[string]string{"error": "postgres is unavailable"})
		return
	}
	loadFromPostgres(a)
	a.refresh()
	s := a.snapshot.Load()
	routes := 0
	version := uint64(0)
	if s != nil {
		version = s.Version
		for _, rs := range s.Routes {
			routes += len(rs)
		}
	}
	jsonWrite(w, http.StatusOK, map[string]any{"status": "reloaded", "version": version, "routes": routes})
}

func (a *App) handler(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("X-Request-ID", a.requestID(r))
	p := strings.TrimSuffix(r.URL.Path, "/")
	switch {
	case p == "/healthz":
		jsonWrite(w, 200, map[string]string{"status": "ok"})
	case p == "/readyz":
		if a.snapshot.Load() == nil {
			jsonWrite(w, 503, map[string]string{"status": "not_ready"})
		} else {
			jsonWrite(w, 200, map[string]string{"status": "ready"})
		}
	case p == "/admin/v1/accounts":
		a.accounts(w, r)
	case strings.HasPrefix(p, "/admin/v1/accounts/"):
		a.item(w, r, "accounts", strings.TrimPrefix(p, "/admin/v1/accounts/"))
	case p == "/admin/v1/providers":
		a.providers(w, r)
	case strings.HasPrefix(p, "/admin/v1/providers/"):
		a.item(w, r, "providers", strings.TrimPrefix(p, "/admin/v1/providers/"))
	case p == "/admin/v1/keys":
		a.keys(w, r)
	case strings.HasPrefix(p, "/admin/v1/keys/"):
		a.item(w, r, "keys", strings.TrimPrefix(p, "/admin/v1/keys/"))
	case p == "/admin/v1/models":
		a.models(w, r)
	case strings.HasPrefix(p, "/admin/v1/models/"):
		a.item(w, r, "models", strings.TrimPrefix(p, "/admin/v1/models/"))
	case p == "/admin/v1/routes":
		a.routes(w)
	case p == "/admin/v1/routing/reload":
		a.reloadRouting(w, r)
	case p == "/v1/inference" && r.Method == "POST":
		a.proxy(w, r)
	default:
		jsonWrite(w, 404, map[string]string{"error": "not found"})
	}
}

var providerLine = regexp.MustCompile(`^\s*([^:]+):\s*(.+)$`)

func importInventory(a *App, path string) int {
	f, err := os.Open(path)
	if err != nil {
		return 0
	}
	defer f.Close()
	a.store.mu.Lock()
	var acct Account
	for _, existing := range a.store.accounts {
		if existing.Email == "imported-account" {
			acct = existing
			break
		}
	}
	if acct.ID == "" {
		acct = Account{ID: id("acct", a.requestSeq.Add(1)), Email: "imported-account", Enabled: true, CreatedAt: now()}
		a.store.accounts[acct.ID] = acct
	}
	count := 0
	buf := make([]byte, 1<<20)
	n, _ := f.Read(buf)
	for _, line := range strings.Split(string(buf[:n]), "\n") {
		m := providerLine.FindStringSubmatch(line)
		if len(m) != 3 {
			continue
		}
		label, val := strings.TrimSpace(m[1]), strings.TrimSpace(m[2])
		if len(val) < 8 {
			continue
		}
		name := strings.TrimSpace(strings.SplitN(label, "(", 2)[0])
		if name == "" {
			continue
		}
		var p Provider
		for _, x := range a.store.providers {
			if x.Name == name && x.AccountID == acct.ID {
				p = x
			}
		}
		if p.ID == "" {
			p = Provider{ID: id("prov", a.requestSeq.Add(1)), AccountID: acct.ID, Name: name, BaseURL: "mock://" + name, AdapterType: "openai_compatible", Enabled: true, CreatedAt: now()}
			a.store.providers[p.ID] = p
		}
		fp := fingerprint(val)
		duplicate := false
		for _, existing := range a.store.keys {
			if existing.ProviderID == p.ID && existing.Fingerprint == fp {
				duplicate = true
				break
			}
		}
		if duplicate {
			continue
		}
		k := APIKey{ID: id("key", a.requestSeq.Add(1)), ProviderID: p.ID, Label: label, Secret: val, Fingerprint: fp, Enabled: true}
		a.store.keys[k.ID] = k
		model := strings.TrimSpace(label)
		if i := strings.Index(model, "("); i >= 0 {
			model = strings.TrimSuffix(model[i+1:], ")")
		}
		if model == "" {
			model = "default"
		}
		modelID := id("model", a.requestSeq.Add(1))
		a.store.models[modelID] = Model{ID: modelID, APIKeyID: k.ID, LogicalName: model, UpstreamModel: model, Enabled: true}
		count++
	}
	a.store.mu.Unlock()
	a.refresh()
	return count
}

// importModels adds explicit provider-model mappings without putting model
// names into credential records. The first enabled key for each provider is
// used unless a model mapping already exists.
func importModels(a *App, path string) int {
	b, err := os.ReadFile(path)
	if err != nil {
		return 0
	}
	count := 0
	a.store.mu.Lock()
	defer a.store.mu.Unlock()
	// A reload must remove deleted rows as well as loading new and updated rows.
	a.store.accounts = map[string]Account{}
	a.store.providers = map[string]Provider{}
	a.store.keys = map[string]APIKey{}
	a.store.models = map[string]Model{}
	for _, line := range strings.Split(string(b), "\n") {
		line = strings.TrimSpace(line)
		if line == "" || strings.HasPrefix(line, "#") {
			continue
		}
		parts := strings.SplitN(line, "=", 2)
		if len(parts) != 2 {
			continue
		}
		providerName, modelName := strings.TrimSpace(parts[0]), strings.TrimSpace(parts[1])
		if providerName == "" || modelName == "" {
			continue
		}
		for _, p := range a.store.providers {
			if !p.Enabled || !strings.EqualFold(p.Name, providerName) {
				continue
			}
			var key APIKey
			for _, k := range a.store.keys {
				if k.ProviderID == p.ID && k.Enabled {
					key = k
					break
				}
			}
			if key.ID == "" {
				continue
			}
			duplicate := false
			for _, m := range a.store.models {
				if m.APIKeyID == key.ID && strings.EqualFold(m.LogicalName, modelName) {
					duplicate = true
					break
				}
			}
			if duplicate {
				continue
			}
			modelID := id("model", a.requestSeq.Add(1))
			a.store.models[modelID] = Model{ID: modelID, APIKeyID: key.ID, LogicalName: modelName, UpstreamModel: modelName, Enabled: true}
			count++
			break
		}
	}
	return count
}

func connectBackend(logger *slog.Logger) (*sql.DB, *redis.Client) {
	host := os.Getenv("DB_HOST")
	if host == "" {
		host = "127.0.0.1"
	}
	port := os.Getenv("DB_PORT")
	if port == "" {
		port = "5432"
	}
	user := os.Getenv("DB_USER")
	password := os.Getenv("DB_PASSWORD")
	schema := os.Getenv("DB_SCHEMA")
	if schema == "" {
		schema = "public"
	}
	dbname := os.Getenv("DB_NAME")
	if dbname == "" {
		dbname = "postgres"
	}
	dsn := fmt.Sprintf("postgres://%s@%s:%s/%s?sslmode=disable&search_path=%s", url.UserPassword(user, password).String(), host, port, dbname, url.QueryEscape(schema))
	db, err := sql.Open("pgx", dsn)
	pingCtx, pingCancel := context.WithTimeout(context.Background(), 3*time.Second)
	if err == nil {
		err = db.PingContext(pingCtx)
	}
	pingCancel()
	if err != nil {
		logger.Warn("postgres unavailable; using memory mode", "reason", err.Error())
		if db != nil {
			_ = db.Close()
		}
		db = nil
	}
	redisAddr := os.Getenv("REDIS_ADDR")
	if redisAddr == "" {
		redisAddr = host + ":6379"
	}
	rc := redis.NewClient(&redis.Options{Addr: redisAddr})
	redisCtx, redisCancel := context.WithTimeout(context.Background(), 3*time.Second)
	redisErr := rc.Ping(redisCtx).Err()
	redisCancel()
	if redisErr != nil {
		logger.Warn("redis unavailable; continuing without shared cache", "reason", redisErr.Error())
		_ = rc.Close()
		rc = nil
	}
	return db, rc
}

func loadFromPostgres(a *App) {
	if a.db == nil {
		return
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	a.store.mu.Lock()
	defer a.store.mu.Unlock()
	rows, err := a.db.QueryContext(ctx, "SELECT id,email,enabled,created_at FROM accounts")
	if err != nil {
		return
	}
	for rows.Next() {
		var v Account
		if rows.Scan(&v.ID, &v.Email, &v.Enabled, &v.CreatedAt) == nil {
			a.store.accounts[v.ID] = v
		}
	}
	rows.Close()
	rows, err = a.db.QueryContext(ctx, "SELECT id,account_id,name,base_url,adapter_type,enabled,created_at FROM providers")
	if err != nil {
		return
	}
	for rows.Next() {
		var v Provider
		if rows.Scan(&v.ID, &v.AccountID, &v.Name, &v.BaseURL, &v.AdapterType, &v.Enabled, &v.CreatedAt) == nil {
			a.store.providers[v.ID] = v
		}
	}
	rows.Close()
	rows, err = a.db.QueryContext(ctx, "SELECT id,provider_id,label,secret_ciphertext,fingerprint,enabled,suspended_until,usage_count,last_used_at FROM api_keys")
	if err != nil {
		return
	}
	for rows.Next() {
		var v APIKey
		var su, lu sql.NullTime
		if rows.Scan(&v.ID, &v.ProviderID, &v.Label, &v.Secret, &v.Fingerprint, &v.Enabled, &su, &v.UsageCount, &lu) == nil {
			if su.Valid {
				v.SuspendedUntil = &su.Time
			}
			if lu.Valid {
				v.LastUsedAt = &lu.Time
			}
			a.store.keys[v.ID] = v
		}
	}
	rows.Close()
	rows, err = a.db.QueryContext(ctx, "SELECT id,api_key_id,logical_name,upstream_model,enabled,usage_count,last_used_at FROM models")
	if err != nil {
		return
	}
	for rows.Next() {
		var v Model
		var lu sql.NullTime
		if rows.Scan(&v.ID, &v.APIKeyID, &v.LogicalName, &v.UpstreamModel, &v.Enabled, &v.UsageCount, &lu) == nil {
			if lu.Valid {
				v.LastUsedAt = &lu.Time
			}
			a.store.models[v.ID] = v
		}
	}
	rows.Close()
}

func Run() {
	if envFile := os.Getenv("ENV_FILE"); envFile != "" {
		if err := config.LoadDotEnv(envFile); err != nil {
			fmt.Fprintln(os.Stderr, "failed to load ENV_FILE:", err)
			os.Exit(1)
		}
	} else {
		_ = config.LoadDotEnv(".env")
	}
	level := new(slog.LevelVar)
	level.Set(slog.LevelInfo)
	logDir := os.Getenv("LOG_DIR")
	if logDir == "" {
		logDir = "..\\logs"
	}
	daily, err := logging.NewDailyWriter(logDir)
	if err != nil {
		slog.New(slog.NewTextHandler(os.Stderr, nil)).Error("log file initialization failed", "error", err)
		os.Exit(1)
	}
	defer daily.Close()
	logger := slog.New(slog.NewJSONHandler(logging.Multi(os.Stdout, daily), &slog.HandlerOptions{Level: level}))
	app := &App{store: newStore(), logger: logger}
	app.db, app.redis = connectBackend(logger)
	loadFromPostgres(app)
	app.refresh()
	addr := os.Getenv("ADDR")
	if addr == "" {
		addr = ":8080"
	}
	logger.Info("gateway listening", "addr", addr)
	if err := http.ListenAndServe(addr, http.HandlerFunc(app.handler)); err != nil {
		logger.Error("server stopped", "error", err)
		os.Exit(1)
	}
}
