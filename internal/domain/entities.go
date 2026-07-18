package domain

import "time"

// Core entities shared by transport, services, and persistence layers.
type Account struct {
	ID, Email string
	Enabled   bool
	CreatedAt time.Time
}
type Provider struct {
	ID, AccountID, Name, BaseURL, AdapterType string
	Enabled                                   bool
	CreatedAt                                 time.Time
}
type APIKey struct {
	ID, ProviderID, Label, Fingerprint string
	Secret                             string `json:"secret,omitempty"`
	Enabled                            bool
	SuspendedUntil                     *time.Time
	UsageCount                         int64
	LastUsedAt                         *time.Time
}
type Model struct {
	ID, APIKeyID, LogicalName, UpstreamModel string
	Enabled                                  bool
	UsageCount                               int64
	LastUsedAt                               *time.Time
}
type Route struct {
	Account  Account
	Provider Provider
	Key      APIKey
	Model    Model
}
