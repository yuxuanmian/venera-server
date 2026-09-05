package api

import (
	"crypto/rand"
	"crypto/sha256"
	"database/sql"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"math/big"
	"net/http"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"time"

	"venera-server/internal/cryptoutil"
	"venera-server/internal/store"
)

const maxScriptSize = 5 << 20 // 5MB

func validRFC3339(s string) bool {
	if s == "" {
		return true
	}
	_, err := time.Parse(time.RFC3339, s)
	return err == nil
}

type registerRequest struct {
	DeviceID string        `json:"device_id"`
	JoinCode string        `json:"join_code"`
	TZ       string        `json:"tz"`
	Locale   string        `json:"locale"`
	Sources  []sourceInput `json:"sources"`
	Comics   []comicInput  `json:"comics"`
}

type sourceInput struct {
	Source     string `json:"source"`
	Cookie     string `json:"cookie"`
	UA         string `json:"ua"`
	Proxy      string `json:"proxy"`
	ScriptHash string `json:"script_hash"`
	InitJSHash string `json:"init_js_hash"`
}

type comicInput struct {
	Source         string `json:"source"`
	ComicID        string `json:"comic_id"`
	DueAt          string `json:"due_at"`
	LastCheckTime  string `json:"last_check_time"`
	LastUpdateTime string `json:"last_update_time"`
	Priority       string `json:"priority"`
}

type syncRequest struct {
	Events []syncEvent `json:"events"`
}

type syncEvent struct {
	EventID    string      `json:"event_id"`
	Type       string      `json:"type"`
	OccurredAt string      `json:"occurred_at"`
	Source     string      `json:"source"`
	ComicID    string      `json:"comic_id"`
	Comic      *comicInput `json:"comic,omitempty"`
}

type clientResultsRequest struct {
	Results []clientResultInput `json:"results"`
}

type clientResultInput struct {
	ClientResultID string          `json:"client_result_id"`
	Source         string          `json:"source"`
	ComicID        string          `json:"comic_id"`
	ScanTime       string          `json:"scan_time"`
	ScriptHash     string          `json:"script_hash"`
	InitJSHash     string          `json:"init_js_hash"`
	CookieHash     string          `json:"cookie_hash"`
	Outcome        string          `json:"outcome"`
	Payload        json.RawMessage `json:"payload"`
	Detail         json.RawMessage `json:"detail"`
}

type scriptRequest struct {
	Kind    string `json:"kind"`
	Hash    string `json:"hash"`
	Content string `json:"content"`
}

// --- register ---

func (s *Server) registerHandler(w http.ResponseWriter, r *http.Request) {
	var req registerRequest
	if !decodeJSON(w, r, &req) {
		return
	}
	if req.TZ == "" {
		writeError(w, http.StatusUnprocessableEntity, "invalid_key", "tz is required")
		return
	}
	if len(req.Comics) > 1000 {
		writeError(w, http.StatusUnprocessableEntity, "invalid_key", "too many comics: max 1000 per register")
		return
	}

	token := bearerToken(r)
	if s.cfg.DebugOpenAuth {
		// Keep registration on the single deterministic debug principal. Any
		// client-supplied bearer value is intentionally ignored in this mode.
		if err := s.ensureDebugPrincipal(); err != nil {
			writeError(w, http.StatusInternalServerError, "internal", "initialize debug principal failed")
			return
		}
		token = debugOpenAuthToken
	}
	if token == "" {
		writeError(w, http.StatusUnauthorized, "auth_error", "missing bearer token")
		return
	}
	tokenHash := hashToken(token)

	deviceID := req.DeviceID
	existingDev, devErr := s.store.GetDeviceByTokenHash(tokenHash)
	if devErr == nil {
		deviceID = existingDev.DeviceID
	} else if !errors.Is(devErr, store.ErrNotFound) {
		writeError(w, http.StatusInternalServerError, "internal", "device lookup failed")
		return
	} else {
		// New device: device_id must be provided and not already bound to another token.
		if deviceID == "" {
			writeError(w, http.StatusUnprocessableEntity, "invalid_key", "device_id is required for new device")
			return
		}
		if byID, err := s.store.GetDeviceByID(deviceID); err == nil {
			_ = byID
			writeError(w, http.StatusUnauthorized, "auth_error", "device_id already registered with another token")
			return
		} else if !errors.Is(err, store.ErrNotFound) {
			writeError(w, http.StatusInternalServerError, "internal", "device lookup failed")
			return
		}
	}

	userID := deviceID
	if req.JoinCode != "" {
		if existingDev != nil {
			writeError(w, http.StatusConflict, "pairing_not_allowed", "device already registered")
			return
		}
		targetUser, _, err := s.store.GetPairingCode(hashToken(req.JoinCode))
		if err != nil {
			writeError(w, http.StatusUnprocessableEntity, "pairing_code_invalid", "invalid pairing code")
			return
		}
		userID = targetUser
	}

	// TZ consistency check.
	if u, err := s.store.GetUser(userID); err == nil {
		if u.TZ != req.TZ {
			writeError(w, http.StatusUnprocessableEntity, "tz_mismatch", "timezone mismatch")
			return
		}
	} else if !errors.Is(err, store.ErrNotFound) {
		writeError(w, http.StatusInternalServerError, "internal", "user lookup failed")
		return
	}

	if err := s.store.UpsertUser(userID, req.TZ, req.Locale); err != nil {
		writeError(w, http.StatusInternalServerError, "internal", "upsert user failed")
		return
	}
	if err := s.store.UpsertDevice(deviceID, userID, tokenHash); err != nil {
		writeError(w, http.StatusInternalServerError, "internal", "upsert device failed")
		return
	}
	if req.JoinCode != "" {
		if err := s.store.UsePairingCode(hashToken(req.JoinCode)); err != nil {
			writeError(w, http.StatusInternalServerError, "internal", "use pairing code failed")
			return
		}
	}

	var scriptHashes []string
	var initJSHashes []string
	for _, src := range req.Sources {
		if src.Source == "" {
			writeError(w, http.StatusUnprocessableEntity, "invalid_key", "source is required")
			return
		}
		encCookie, err := cryptoutil.Encrypt(src.Cookie, s.cookieKey)
		if err != nil {
			writeError(w, http.StatusInternalServerError, "internal", "encrypt cookie failed")
			return
		}
		sess := store.SourceSession{
			UserID:          userID,
			Source:          src.Source,
			CookieEncrypted: encCookie,
			CookieHash:      sha256Hex(src.Cookie),
			UA:              src.UA,
			Proxy:           src.Proxy,
			ScriptHash:      src.ScriptHash,
			InitJSHash:      src.InitJSHash,
			UpdatedAt:       time.Now().UTC().Format(time.RFC3339),
		}
		if err := s.store.UpsertSourceSession(sess); err != nil {
			writeError(w, http.StatusInternalServerError, "internal", "upsert source session failed")
			return
		}
		if _, err := s.store.ResetNeedsResubmitJobs(userID, src.Source); err != nil {
			writeError(w, http.StatusInternalServerError, "internal", "reset needs_resubmit jobs failed")
			return
		}
		if src.ScriptHash != "" {
			scriptHashes = append(scriptHashes, src.ScriptHash)
		}
		if src.InitJSHash != "" {
			initJSHashes = append(initJSHashes, src.InitJSHash)
		}
	}

	for _, c := range req.Comics {
		if c.Source == "" || c.ComicID == "" {
			writeError(w, http.StatusUnprocessableEntity, "invalid_key", "source and comic_id are required")
			return
		}
		if !validRFC3339(c.DueAt) || !validRFC3339(c.LastCheckTime) || !validRFC3339(c.LastUpdateTime) {
			writeError(w, http.StatusUnprocessableEntity, "invalid_key", "timestamps must be RFC3339")
			return
		}
		hasTomb, err := s.store.HasTombstone(userID, c.Source, c.ComicID)
		if err != nil {
			writeError(w, http.StatusInternalServerError, "internal", "tombstone check failed")
			return
		}
		if hasTomb {
			continue
		}
		priority := c.Priority
		if priority == "" {
			priority = "normal"
		}
		m := store.Mirror{
			UserID:         userID,
			Source:         c.Source,
			ComicID:        c.ComicID,
			DueAt:          c.DueAt,
			LastCheckTime:  c.LastCheckTime,
			LastUpdateTime: c.LastUpdateTime,
			Priority:       priority,
			UpdatedAt:      time.Now().UTC().Format(time.RFC3339),
		}
		if err := s.store.MergeMirror(m); err != nil {
			writeError(w, http.StatusInternalServerError, "internal", "merge mirror failed")
			return
		}
	}

	missing, err := s.store.FindMissingScriptHashes(scriptHashes)
	if err != nil {
		writeError(w, http.StatusInternalServerError, "internal", "missing scripts check failed")
		return
	}
	missingInit, err := s.store.FindMissingScriptHashes(initJSHashes)
	if err != nil {
		writeError(w, http.StatusInternalServerError, "internal", "missing init scripts check failed")
		return
	}

	writeJSON(w, http.StatusOK, map[string]any{
		"user_id":                userID,
		"server_time":            time.Now().UTC().Format(time.RFC3339),
		"missing_script_hashes":  missing,
		"missing_init_js_hashes": missingInit,
	})
}

// --- sync ---

func (s *Server) syncHandler(w http.ResponseWriter, r *http.Request) {
	userID := userIDFrom(r.Context())
	deviceID := deviceIDFrom(r.Context())
	var req syncRequest
	if !decodeJSON(w, r, &req) {
		return
	}
	if len(req.Events) > 100 {
		writeError(w, http.StatusUnprocessableEntity, "invalid_key", "too many events")
		return
	}
	acked := make([]string, 0, len(req.Events))
	for _, ev := range req.Events {
		if ev.EventID == "" || ev.Type == "" {
			writeError(w, http.StatusUnprocessableEntity, "invalid_key", "event_id and type are required")
			return
		}
		ok, err := s.store.InsertSyncEvent(ev.EventID, userID, deviceID)
		if err != nil {
			writeError(w, http.StatusInternalServerError, "internal", "insert sync event failed")
			return
		}
		if !ok {
			acked = append(acked, ev.EventID)
			continue
		}
		if err := s.applySyncEvent(userID, ev); err != nil {
			writeError(w, http.StatusUnprocessableEntity, "invalid_key", err.Error())
			return
		}
		acked = append(acked, ev.EventID)
	}
	writeJSON(w, http.StatusOK, map[string]any{
		"acked_event_ids":       acked,
		"missing_script_hashes": []string{},
	})
}

func (s *Server) applySyncEvent(userID string, ev syncEvent) error {
	if ev.OccurredAt != "" && !validRFC3339(ev.OccurredAt) {
		return fmt.Errorf("occurred_at must be RFC3339")
	}
	if ev.Comic != nil && (!validRFC3339(ev.Comic.DueAt) || !validRFC3339(ev.Comic.LastCheckTime) || !validRFC3339(ev.Comic.LastUpdateTime)) {
		return fmt.Errorf("comic timestamps must be RFC3339")
	}
	switch ev.Type {
	case "add":
		if ev.Comic == nil || ev.Comic.Source == "" || ev.Comic.ComicID == "" {
			return fmt.Errorf("add requires comic.source and comic.comic_id")
		}
		if err := s.store.ClearTombstone(userID, ev.Comic.Source, ev.Comic.ComicID); err != nil {
			return err
		}
		priority := ev.Comic.Priority
		if priority == "" {
			priority = "normal"
		}
		return s.store.MergeMirror(store.Mirror{
			UserID:         userID,
			Source:         ev.Comic.Source,
			ComicID:        ev.Comic.ComicID,
			DueAt:          ev.Comic.DueAt,
			LastCheckTime:  ev.Comic.LastCheckTime,
			LastUpdateTime: ev.Comic.LastUpdateTime,
			Priority:       priority,
			UpdatedAt:      time.Now().UTC().Format(time.RFC3339),
		})
	case "remove":
		if ev.Source == "" || ev.ComicID == "" {
			return fmt.Errorf("remove requires source and comic_id")
		}
		if err := s.store.UpsertTombstone(userID, ev.Source, ev.ComicID); err != nil {
			return err
		}
		if err := s.store.DeleteMirror(userID, ev.Source, ev.ComicID); err != nil {
			return err
		}
		return s.store.CancelPendingJobs(userID, ev.Source, ev.ComicID)
	case "update_state":
		if ev.Comic == nil || ev.Comic.Source == "" || ev.Comic.ComicID == "" {
			return fmt.Errorf("update_state requires comic.source and comic.comic_id")
		}
		return s.store.UpdateMirrorState(userID, ev.Comic.Source, ev.Comic.ComicID, ev.Comic.DueAt, ev.Comic.Priority)
	case "check_now":
		if ev.Source == "" || ev.ComicID == "" {
			return fmt.Errorf("check_now requires source and comic_id")
		}
		active, err := s.store.HasActiveJob(userID, ev.Source, ev.ComicID)
		if err != nil {
			return err
		}
		if active {
			return nil
		}
		return s.store.InsertJob(store.Job{
			UserID:    userID,
			Source:    ev.Source,
			ComicID:   ev.ComicID,
			State:     "pending",
			Priority:  "urgent",
			DueAt:     sql.NullString{String: time.Now().UTC().Format(time.RFC3339), Valid: true},
			CreatedAt: time.Now().UTC().Format(time.RFC3339),
		})
	default:
		return fmt.Errorf("unknown event type %q", ev.Type)
	}
}

// --- client-results ---

func (s *Server) clientResultsHandler(w http.ResponseWriter, r *http.Request) {
	userID := userIDFrom(r.Context())
	var req clientResultsRequest
	if !decodeJSON(w, r, &req) {
		return
	}
	if len(req.Results) > 100 {
		writeError(w, http.StatusUnprocessableEntity, "invalid_key", "too many results")
		return
	}
	accepted := make([]string, 0, len(req.Results))
	rejected := make([]map[string]any, 0)
	for _, res := range req.Results {
		if res.ClientResultID == "" || res.Source == "" || res.ComicID == "" || res.ScanTime == "" {
			writeError(w, http.StatusUnprocessableEntity, "invalid_key", "client_result_id/source/comic_id/scan_time required")
			return
		}
		if !validRFC3339(res.ScanTime) {
			writeError(w, http.StatusUnprocessableEntity, "invalid_key", "scan_time must be RFC3339")
			return
		}
		sess, err := s.store.GetSourceSession(userID, res.Source)
		if err != nil {
			rejected = append(rejected, map[string]any{"client_result_id": res.ClientResultID, "reason": "source_not_found"})
			continue
		}
		if sess.ScriptHash != res.ScriptHash || sess.InitJSHash != res.InitJSHash || sess.CookieHash != res.CookieHash {
			rejected = append(rejected, map[string]any{"client_result_id": res.ClientResultID, "reason": "version_rejected"})
			continue
		}
		payload := string(res.Payload)
		detail := string(res.Detail)
		inserted, err := s.store.InsertResult(store.Result{
			UserID:         userID,
			Source:         res.Source,
			ComicID:        res.ComicID,
			ClientResultID: sql.NullString{String: res.ClientResultID, Valid: true},
			ScanTime:       res.ScanTime,
			ScriptHash:     res.ScriptHash,
			InitJSHash:     res.InitJSHash,
			CookieHash:     res.CookieHash,
			Outcome:        res.Outcome,
			Payload:        sql.NullString{String: payload, Valid: payload != ""},
			Detail:         sql.NullString{String: detail, Valid: detail != ""},
		})
		if err != nil {
			writeError(w, http.StatusInternalServerError, "internal", "insert result failed")
			return
		}
		if !inserted {
			accepted = append(accepted, res.ClientResultID)
			continue
		}
		_ = s.store.PrunePayloads(userID, res.Source, res.ComicID)
		// Update mirror with newer scan_time.
		if m, err := s.store.GetMirror(userID, res.Source, res.ComicID); err == nil {
			if storeTimeAfter(res.ScanTime, m.LastCheckTime) {
				_ = s.store.MergeMirror(store.Mirror{
					UserID:         userID,
					Source:         res.Source,
					ComicID:        res.ComicID,
					DueAt:          m.DueAt,
					LastCheckTime:  res.ScanTime,
					LastUpdateTime: m.LastUpdateTime,
					Priority:       m.Priority,
					UpdatedAt:      time.Now().UTC().Format(time.RFC3339),
				})
			}
		}
		accepted = append(accepted, res.ClientResultID)
	}
	writeJSON(w, http.StatusOK, map[string]any{
		"accepted": accepted,
		"rejected": rejected,
	})
}

// --- scripts ---

func (s *Server) scriptsHandler(w http.ResponseWriter, r *http.Request) {
	var req scriptRequest
	if !decodeJSON(w, r, &req) {
		return
	}
	if req.Kind != "comic_source" && req.Kind != "init_js" {
		writeError(w, http.StatusUnprocessableEntity, "invalid_key", "kind must be comic_source or init_js")
		return
	}
	if req.Hash == "" {
		writeError(w, http.StatusUnprocessableEntity, "invalid_key", "hash is required")
		return
	}
	if len(req.Content) > maxScriptSize {
		writeError(w, http.StatusRequestEntityTooLarge, "invalid_key", "script too large")
		return
	}
	if sha256Hex(req.Content) != req.Hash {
		writeError(w, http.StatusUnprocessableEntity, "hash_mismatch", "content hash mismatch")
		return
	}
	exists, err := s.store.ScriptExists(req.Hash)
	if err != nil {
		writeError(w, http.StatusInternalServerError, "internal", "script check failed")
		return
	}
	if exists {
		writeJSON(w, http.StatusOK, map[string]any{"hash": req.Hash, "stored": true})
		return
	}
	dir := filepath.Join(s.cfg.DataDir, "scripts", req.Kind)
	if err := os.MkdirAll(dir, 0o755); err != nil {
		writeError(w, http.StatusInternalServerError, "internal", "mkdir scripts failed")
		return
	}
	path := filepath.Join(dir, req.Hash+".js")
	if err := os.WriteFile(path, []byte(req.Content), 0o644); err != nil {
		writeError(w, http.StatusInternalServerError, "internal", "write script failed")
		return
	}
	if err := s.store.InsertScript(req.Hash, req.Kind, path, int64(len(req.Content))); err != nil {
		writeError(w, http.StatusInternalServerError, "internal", "insert script failed")
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"hash": req.Hash, "stored": true})
}

// --- results ---

func (s *Server) resultsHandler(w http.ResponseWriter, r *http.Request) {
	userID := userIDFrom(r.Context())
	cursor, _ := strconv.ParseInt(r.URL.Query().Get("cursor"), 10, 64)
	limit, _ := strconv.Atoi(r.URL.Query().Get("limit"))
	if limit <= 0 {
		limit = 500
	}
	if limit > 1000 {
		limit = 1000
	}
	results, err := s.store.ListResults(userID, cursor, limit)
	if err != nil {
		writeError(w, http.StatusInternalServerError, "internal", "list results failed")
		return
	}
	out := make([]map[string]any, 0, len(results))
	nextCursor := cursor
	for _, res := range results {
		nextCursor = res.ResultID
		var payload, detail any
		if res.Payload.Valid {
			payload = json.RawMessage(res.Payload.String)
		}
		if res.Detail.Valid {
			detail = json.RawMessage(res.Detail.String)
		}
		out = append(out, map[string]any{
			"result_id":    res.ResultID,
			"source":       res.Source,
			"comic_id":     res.ComicID,
			"scan_time":    res.ScanTime,
			"script_hash":  res.ScriptHash,
			"init_js_hash": res.InitJSHash,
			"outcome":      res.Outcome,
			"payload":      payload,
			"detail":       detail,
		})
	}
	writeJSON(w, http.StatusOK, map[string]any{
		"cursor":   nextCursor,
		"has_more": len(results) == limit,
		"results":  out,
	})
}

// --- refresh ---

func (s *Server) refreshHandler(w http.ResponseWriter, r *http.Request) {
	userID := userIDFrom(r.Context())
	var req struct {
		Scope string `json:"scope"`
	}
	if !decodeJSON(w, r, &req) {
		return
	}
	if req.Scope != "all" {
		writeError(w, http.StatusUnprocessableEntity, "invalid_key", "scope must be all")
		return
	}
	mirrors, err := s.store.ListMirror(userID)
	if err != nil {
		writeError(w, http.StatusInternalServerError, "internal", "list mirror failed")
		return
	}
	queued := 0
	for _, m := range mirrors {
		active, err := s.store.HasActiveJob(userID, m.Source, m.ComicID)
		if err != nil {
			continue
		}
		if active {
			continue
		}
		now := time.Now().UTC().Format(time.RFC3339)
		_ = s.store.InsertJob(store.Job{
			UserID:      userID,
			Source:      m.Source,
			ComicID:     m.ComicID,
			State:       "pending",
			Priority:    "urgent",
			DueAt:       sql.NullString{String: now, Valid: true},
			ScheduledAt: sql.NullString{String: now, Valid: true},
			CreatedAt:   now,
		})
		queued++
	}
	writeJSON(w, http.StatusAccepted, map[string]any{"queued": queued})
}

// --- pairing codes ---

var codeAlphabet = []byte("ABCDEFGHJKLMNPQRSTUVWXYZ23456789")

func (s *Server) pairingCodesHandler(w http.ResponseWriter, r *http.Request) {
	userID := userIDFrom(r.Context())
	deviceID := deviceIDFrom(r.Context())
	code, err := randomCode(8)
	if err != nil {
		writeError(w, http.StatusInternalServerError, "internal", "generate code failed")
		return
	}
	expiresAt := time.Now().UTC().Add(10 * time.Minute).Format(time.RFC3339)
	if err := s.store.InvalidateDeviceCodes(deviceID); err != nil {
		writeError(w, http.StatusInternalServerError, "internal", "invalidate old codes failed")
		return
	}
	if err := s.store.CreatePairingCode(hashToken(code), userID, deviceID, expiresAt); err != nil {
		writeError(w, http.StatusInternalServerError, "internal", "create pairing code failed")
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"code": code, "expires_at": expiresAt})
}

// --- devices ---

func (s *Server) devicesHandler(w http.ResponseWriter, r *http.Request) {
	userID := userIDFrom(r.Context())
	devices, err := s.store.ListDevices(userID)
	if err != nil {
		writeError(w, http.StatusInternalServerError, "internal", "list devices failed")
		return
	}
	currentDeviceID := deviceIDFrom(r.Context())
	out := make([]map[string]any, 0, len(devices))
	for _, d := range devices {
		out = append(out, map[string]any{
			"device_id":    d.DeviceID,
			"last_seen_at": d.LastSeenAt,
			"current":      d.DeviceID == currentDeviceID,
		})
	}
	writeJSON(w, http.StatusOK, map[string]any{"devices": out})
}

func (s *Server) deleteDeviceHandler(w http.ResponseWriter, r *http.Request) {
	userID := userIDFrom(r.Context())
	target := r.PathValue("device_id")
	if target == "" {
		writeError(w, http.StatusUnprocessableEntity, "invalid_key", "device_id required")
		return
	}
	if err := s.store.DeleteDevice(target, userID); err != nil {
		if errors.Is(err, store.ErrNotFound) {
			writeError(w, http.StatusNotFound, "invalid_key", "device not found")
			return
		}
		writeError(w, http.StatusInternalServerError, "internal", "delete device failed")
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"deleted": target})
}

// --- helpers ---

func decodeJSON(w http.ResponseWriter, r *http.Request, dst any) bool {
	dec := json.NewDecoder(http.MaxBytesReader(w, r.Body, 10<<20))
	dec.DisallowUnknownFields()
	if err := dec.Decode(dst); err != nil {
		writeError(w, http.StatusBadRequest, "invalid_json", err.Error())
		return false
	}
	return true
}

func bearerToken(r *http.Request) string {
	auth := r.Header.Get("Authorization")
	if strings.HasPrefix(auth, "Bearer ") {
		return strings.TrimSpace(strings.TrimPrefix(auth, "Bearer "))
	}
	return ""
}

func sha256Hex(s string) string {
	sum := sha256.Sum256([]byte(s))
	return hex.EncodeToString(sum[:])
}

func storeTimeAfter(a, b string) bool {
	if b == "" {
		return true
	}
	ta, err1 := time.Parse(time.RFC3339, a)
	tb, err2 := time.Parse(time.RFC3339, b)
	if err1 != nil || err2 != nil {
		return a > b
	}
	return ta.After(tb)
}

func randomCode(n int) (string, error) {
	b := make([]byte, n)
	for i := range b {
		idx, err := rand.Int(rand.Reader, big.NewInt(int64(len(codeAlphabet))))
		if err != nil {
			return "", err
		}
		b[i] = codeAlphabet[idx.Int64()]
	}
	return string(b), nil
}

func missingScriptHashesByKind(sources []sourceInput, missing []string) []string {
	set := map[string]bool{}
	for _, h := range missing {
		set[h] = true
	}
	var out []string
	for _, src := range sources {
		if src.InitJSHash != "" && set[src.InitJSHash] {
			out = append(out, src.InitJSHash)
		}
	}
	return out
}
