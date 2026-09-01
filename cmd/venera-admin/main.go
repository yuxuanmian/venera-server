package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"strconv"
	"strings"
	"time"

	"venera-server/internal/v2config"
	"venera-server/internal/v2crypto"
	"venera-server/internal/v2domain"
	"venera-server/internal/v2manifest"
	"venera-server/internal/v2store"
)

var (
	errCLIUsage            = errors.New("invalid venera-admin command")
	errCLIClientRequired   = errors.New("client is required")
	errCLIRevisionRequired = errors.New("expected revision is required")
	errCLINotQuarantined   = errors.New("source is not quarantined")
)

func main() {
	if err := run(os.Args[1:], os.Stdout, os.Stderr); err != nil {
		// Errors intentionally contain command-safe text only.  Enrollment
		// codes are written by the successful command path exactly once.
		_, _ = fmt.Fprintln(os.Stderr, cliErrorMessage(err))
		os.Exit(1)
	}
}

func run(args []string, stdout, stderr io.Writer) error {
	_ = stderr
	cfg, err := v2config.LoadFromEnv()
	if err != nil {
		return errors.New("v2 configuration is invalid")
	}
	return runWithConfig(context.Background(), cfg, args, stdout)
}

func runWithConfig(ctx context.Context, cfg v2config.Config, args []string, stdout io.Writer) error {
	if ctx == nil {
		ctx = context.Background()
	}
	if len(args) == 0 {
		return errCLIUsage
	}
	keys, err := v2crypto.DeriveKeys(cfg.RootSecret)
	if err != nil {
		return errors.New("v2 keys are invalid")
	}
	db, err := v2store.Open(cfg.DBPath)
	if err != nil {
		return errors.New("cannot open v2 database")
	}
	defer db.Close()
	repo := v2store.NewRepository(db, keys)

	switch {
	case len(args) >= 3 && args[0] == "client" && args[1] == "enrollment" && args[2] == "create":
		ttl, err := parseDurationFlag(args[3:], "--ttl")
		if err != nil {
			return err
		}
		if ttl <= 0 || ttl > 24*time.Hour {
			return errors.New("enrollment TTL is outside the allowed range")
		}
		code, err := repo.CreateEnrollmentCode(ctx, ttl)
		if err != nil {
			return errors.New("cannot create enrollment code")
		}
		_, err = fmt.Fprintln(stdout, code.Code)
		return err
	case len(args) == 2 && args[0] == "client" && args[1] == "list":
		clients, err := listClients(ctx, db)
		if err != nil {
			return errors.New("cannot list clients")
		}
		return writeJSON(stdout, clients)
	case len(args) >= 2 && args[0] == "client" && args[1] == "revoke":
		clientID, err := parseStringOption(args[2:], "--client")
		if err != nil {
			return errors.Join(errCLIClientRequired, err)
		}
		revision, err := parseInt64Option(args[2:], "--expected-revision")
		if err != nil || revision < 1 {
			return errCLIRevisionRequired
		}
		key := "cli-revoke:" + clientID + ":" + strconv.FormatInt(revision, 10)
		if err := repo.RevokeClient(ctx, clientID, v2domain.Revision(revision), key); err != nil {
			return errors.New("cannot revoke client")
		}
		client, err := repo.GetClient(ctx, clientID)
		if err != nil {
			return errors.New("cannot read revoked client")
		}
		return writeJSON(stdout, map[string]any{"clientId": client.ID, "state": client.State, "revision": client.Revision})
	case len(args) == 2 && args[0] == "manifest" && args[1] == "check-now":
		manifest, err := v2manifest.NewManager(cfg, repo, nil).FetchAndActivate(ctx)
		if err != nil {
			return errors.New("manifest check failed")
		}
		return writeJSON(stdout, map[string]any{"catalogId": manifest.CatalogID, "catalogSequence": manifest.CatalogSequence})
	case len(args) == 2 && args[0] == "source" && args[1] == "list":
		sources, err := listSources(ctx, db)
		if err != nil {
			return errors.New("cannot list sources")
		}
		return writeJSON(stdout, sources)
	case len(args) >= 2 && args[0] == "source" && args[1] == "unquarantine":
		artifactID, err := parseStringOption(args[2:], "--artifact")
		if err != nil {
			return err
		}
		if err := unquarantineSource(ctx, db, artifactID); err != nil {
			return err
		}
		return writeJSON(stdout, map[string]any{"artifactId": artifactID, "runtimeState": "healthy"})
	case len(args) == 2 && args[0] == "db" && args[1] == "check":
		if err := db.ValidateReady(ctx); err != nil {
			return errors.New("v2 database check failed")
		}
		return writeJSON(stdout, map[string]string{"status": "ok"})
	default:
		return errCLIUsage
	}
}

type cliClient struct {
	ClientID   string `json:"clientId"`
	State      string `json:"state"`
	Platform   string `json:"platform"`
	AppVersion string `json:"appVersion"`
	Revision   int64  `json:"revision"`
	LastSeenAt string `json:"lastSeenAt,omitempty"`
}

func listClients(ctx context.Context, db *v2store.DB) ([]cliClient, error) {
	result := []cliClient{}
	err := db.ReadTx(ctx, func(tx *v2store.Tx) error {
		rows, err := tx.QueryContext(ctx, `SELECT client_id, state, platform, app_version, revision, COALESCE(last_seen_at, '') FROM client_installations ORDER BY created_at, client_id`)
		if err != nil {
			return err
		}
		defer rows.Close()
		for rows.Next() {
			var item cliClient
			if err := rows.Scan(&item.ClientID, &item.State, &item.Platform, &item.AppVersion, &item.Revision, &item.LastSeenAt); err != nil {
				return err
			}
			result = append(result, item)
		}
		return rows.Err()
	})
	return result, err
}

type cliSource struct {
	ArtifactID   string `json:"artifactId"`
	ManagedState string `json:"managedState"`
	RuntimeState string `json:"runtimeState"`
	Quarantined  bool   `json:"quarantined"`
}

func listSources(ctx context.Context, db *v2store.DB) ([]cliSource, error) {
	result := []cliSource{}
	err := db.ReadTx(ctx, func(tx *v2store.Tx) error {
		rows, err := tx.QueryContext(ctx, `SELECT a.artifact_id, a.managed_state, COALESCE(rs.state, 'healthy') FROM source_artifacts a LEFT JOIN source_runtime_state rs ON rs.artifact_id = a.artifact_id ORDER BY a.artifact_id`)
		if err != nil {
			return err
		}
		defer rows.Close()
		for rows.Next() {
			var item cliSource
			if err := rows.Scan(&item.ArtifactID, &item.ManagedState, &item.RuntimeState); err != nil {
				return err
			}
			item.Quarantined = item.RuntimeState == "quarantined"
			result = append(result, item)
		}
		return rows.Err()
	})
	return result, err
}

func unquarantineSource(ctx context.Context, db *v2store.DB, artifactID string) error {
	if artifactID == "" {
		return errors.New("artifact is required")
	}
	err := db.WriteTx(ctx, func(tx *v2store.Tx) error {
		result, err := tx.ExecContext(ctx, `UPDATE source_runtime_state SET state = 'healthy', failure_streak = 0, last_error_code = NULL, revision = revision + 1, updated_at = ? WHERE artifact_id = ? AND state = 'quarantined'`, db.Now().Format(time.RFC3339Nano), artifactID)
		if err != nil {
			return err
		}
		count, err := result.RowsAffected()
		if err != nil {
			return err
		}
		if count != 1 {
			return errCLINotQuarantined
		}
		return nil
	})
	return err
}

func parseDurationFlag(args []string, name string) (time.Duration, error) {
	value, err := parseOptionValue(args, name)
	if err != nil {
		return 0, err
	}
	return time.ParseDuration(value)
}

func parseStringOption(args []string, name string) (string, error) {
	value, err := parseOptionValue(args, name)
	if err != nil || strings.TrimSpace(value) == "" {
		return "", errCLIUsage
	}
	return strings.TrimSpace(value), nil
}

func parseInt64Option(args []string, name string) (int64, error) {
	value, err := parseOptionValue(args, name)
	if err != nil {
		return 0, err
	}
	return strconv.ParseInt(value, 10, 64)
}

func parseFlagValue(args []string, name string) (string, error) {
	if len(args) != 2 {
		return "", errCLIUsage
	}
	if strings.HasPrefix(args[0], name+"=") {
		return strings.TrimPrefix(args[0], name+"="), nil
	}
	if args[0] != name || args[1] == "" {
		return "", errCLIUsage
	}
	return args[1], nil
}

func parseOptionValue(args []string, name string) (string, error) {
	if len(args) == 0 {
		return "", errCLIUsage
	}
	var value string
	found := false
	for index := 0; index < len(args); index++ {
		argument := args[index]
		if strings.HasPrefix(argument, name+"=") {
			if found {
				return "", errCLIUsage
			}
			value = strings.TrimPrefix(argument, name+"=")
			found = true
			continue
		}
		if argument == name {
			if found || index+1 >= len(args) || strings.HasPrefix(args[index+1], "-") {
				return "", errCLIUsage
			}
			value = args[index+1]
			found = true
			index++
			continue
		}
		if strings.HasPrefix(argument, "--") {
			if !strings.Contains(argument, "=") {
				if index+1 >= len(args) || strings.HasPrefix(args[index+1], "-") {
					return "", errCLIUsage
				}
				index++
			}
			continue
		}
		return "", errCLIUsage
	}
	if !found || strings.TrimSpace(value) == "" {
		return "", errCLIUsage
	}
	return value, nil
}

func writeJSON(writer io.Writer, value any) error {
	encoder := json.NewEncoder(writer)
	encoder.SetEscapeHTML(true)
	return encoder.Encode(value)
}

func cliErrorMessage(err error) string {
	switch {
	case errors.Is(err, errCLIUsage):
		return "invalid venera-admin command"
	case errors.Is(err, errCLINotQuarantined):
		return "source is not quarantined"
	case errors.Is(err, errCLIClientRequired):
		return "client is required"
	case errors.Is(err, errCLIRevisionRequired):
		return "expected revision is required"
	default:
		return "venera-admin command failed"
	}
}
