package postgres

import (
	"ai-gateway/internal/domain"
	"context"
	"database/sql"
)

type Repository struct{ DB *sql.DB }

func (r *Repository) Load(ctx context.Context) ([]domain.Account, []domain.Provider, []domain.APIKey, []domain.Model, error) {
	var accounts []domain.Account
	rows, err := r.DB.QueryContext(ctx, "SELECT id,email,enabled,created_at FROM accounts")
	if err != nil {
		return nil, nil, nil, nil, err
	}
	for rows.Next() {
		var v domain.Account
		if err = rows.Scan(&v.ID, &v.Email, &v.Enabled, &v.CreatedAt); err != nil {
			rows.Close()
			return nil, nil, nil, nil, err
		}
		accounts = append(accounts, v)
	}
	rows.Close()
	var providers []domain.Provider
	rows, err = r.DB.QueryContext(ctx, "SELECT id,account_id,name,base_url,adapter_type,enabled,created_at FROM providers")
	if err != nil {
		return nil, nil, nil, nil, err
	}
	for rows.Next() {
		var v domain.Provider
		if err = rows.Scan(&v.ID, &v.AccountID, &v.Name, &v.BaseURL, &v.AdapterType, &v.Enabled, &v.CreatedAt); err != nil {
			rows.Close()
			return nil, nil, nil, nil, err
		}
		providers = append(providers, v)
	}
	rows.Close()
	var keys []domain.APIKey
	rows, err = r.DB.QueryContext(ctx, "SELECT id,provider_id,label,secret_ciphertext,fingerprint,enabled,suspended_until,usage_count,last_used_at FROM api_keys")
	if err != nil {
		return nil, nil, nil, nil, err
	}
	for rows.Next() {
		var v domain.APIKey
		var su, lu sql.NullTime
		if err = rows.Scan(&v.ID, &v.ProviderID, &v.Label, &v.Secret, &v.Fingerprint, &v.Enabled, &su, &v.UsageCount, &lu); err != nil {
			rows.Close()
			return nil, nil, nil, nil, err
		}
		if su.Valid {
			v.SuspendedUntil = &su.Time
		}
		if lu.Valid {
			v.LastUsedAt = &lu.Time
		}
		keys = append(keys, v)
	}
	rows.Close()
	var models []domain.Model
	rows, err = r.DB.QueryContext(ctx, "SELECT id,api_key_id,logical_name,upstream_model,enabled,usage_count,last_used_at FROM models")
	if err != nil {
		return nil, nil, nil, nil, err
	}
	for rows.Next() {
		var v domain.Model
		var lu sql.NullTime
		if err = rows.Scan(&v.ID, &v.APIKeyID, &v.LogicalName, &v.UpstreamModel, &v.Enabled, &v.UsageCount, &lu); err != nil {
			rows.Close()
			return nil, nil, nil, nil, err
		}
		if lu.Valid {
			v.LastUsedAt = &lu.Time
		}
		models = append(models, v)
	}
	rows.Close()
	return accounts, providers, keys, models, nil
}

// Save is transactional; callers own encryption of APIKey.Secret before persistence.
func (r *Repository) Save(ctx context.Context, accounts []domain.Account, providers []domain.Provider, keys []domain.APIKey, models []domain.Model) error {
	tx, err := r.DB.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer func() {
		if err != nil {
			_ = tx.Rollback()
		}
	}()
	for _, v := range accounts {
		_, err = tx.ExecContext(ctx, "INSERT INTO accounts(id,email,enabled,created_at) VALUES($1,$2,$3,$4) ON CONFLICT(id) DO UPDATE SET email=$2,enabled=$3", v.ID, v.Email, v.Enabled, v.CreatedAt)
		if err != nil {
			return err
		}
	}
	for _, v := range providers {
		_, err = tx.ExecContext(ctx, "INSERT INTO providers(id,account_id,name,base_url,adapter_type,enabled,created_at) VALUES($1,$2,$3,$4,$5,$6,$7) ON CONFLICT(id) DO UPDATE SET name=$3,base_url=$4,enabled=$6", v.ID, v.AccountID, v.Name, v.BaseURL, v.AdapterType, v.Enabled, v.CreatedAt)
		if err != nil {
			return err
		}
	}
	for _, v := range keys {
		_, err = tx.ExecContext(ctx, "INSERT INTO api_keys(id,provider_id,label,secret_ciphertext,fingerprint,enabled,suspended_until,usage_count,last_used_at) VALUES($1,$2,$3,$4,$5,$6,$7,$8,$9) ON CONFLICT(id) DO UPDATE SET label=$3,secret_ciphertext=$4,enabled=$6,suspended_until=$7,usage_count=$8,last_used_at=$9", v.ID, v.ProviderID, v.Label, v.Secret, v.Fingerprint, v.Enabled, v.SuspendedUntil, v.UsageCount, v.LastUsedAt)
		if err != nil {
			return err
		}
	}
	for _, v := range models {
		_, err = tx.ExecContext(ctx, "INSERT INTO models(id,api_key_id,logical_name,upstream_model,enabled,usage_count,last_used_at) VALUES($1,$2,$3,$4,$5,$6,$7) ON CONFLICT(id) DO UPDATE SET logical_name=$3,upstream_model=$4,enabled=$5,usage_count=$6,last_used_at=$7", v.ID, v.APIKeyID, v.LogicalName, v.UpstreamModel, v.Enabled, v.UsageCount, v.LastUsedAt)
		if err != nil {
			return err
		}
	}
	return tx.Commit()
}
