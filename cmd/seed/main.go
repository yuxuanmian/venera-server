package main

import (
	"database/sql"
	"flag"
	"fmt"
	"log"
	"os"
	"path/filepath"
	"time"

	_ "modernc.org/sqlite"

	"venera-server/internal/store"
)

func main() {
	var (
		user       = flag.String("user", "test-user", "user_id for seeded mirrors")
		source     = flag.String("source", "", "only seed this source (empty = all)")
		offset     = flag.Duration("offset", -time.Minute, "due_at offset from now (negative = already due)")
		cachePath  = flag.String("cache", "", "path to network_favorite_cache.db (default APPDATA venera)")
		dataDir    = flag.String("data", "data", "server data dir")
		clearFirst = flag.Bool("clear", false, "clear mirror/jobs/chunks for the target user before seeding")
	)
	flag.Parse()

	if *cachePath == "" {
		*cachePath = filepath.Join(os.Getenv("APPDATA"), "com.github.wgh136", "venera", "network_favorite_cache.db")
	}

	st, err := store.Open(*dataDir)
	if err != nil {
		log.Fatalf("open store: %v", err)
	}
	defer st.Close()

	if *clearFirst {
		if err := clearUserData(st, *user); err != nil {
			log.Fatalf("clear user data: %v", err)
		}
	}

	cache, err := sql.Open("sqlite", "file:"+filepath.ToSlash(*cachePath)+"?mode=ro")
	if err != nil {
		log.Fatalf("open cache: %v", err)
	}
	defer cache.Close()

	query := `SELECT source_key, comic_id, last_update_time, last_check_time FROM favorite_items`
	args := []any{}
	if *source != "" {
		query += ` WHERE source_key = ?`
		args = append(args, *source)
	}
	rows, err := cache.Query(query, args...)
	if err != nil {
		log.Fatalf("query favorite_items: %v", err)
	}
	defer rows.Close()

	due := time.Now().Add(*offset).UTC().Format(time.RFC3339)
	now := time.Now().UTC().Format(time.RFC3339)
	count := 0
	for rows.Next() {
		var src, comicID string
		var lastUpdate sql.NullString
		var lastCheck sql.NullInt64
		if err := rows.Scan(&src, &comicID, &lastUpdate, &lastCheck); err != nil {
			log.Fatalf("scan row: %v", err)
		}
		lastCheckTime := ""
		if lastCheck.Valid && lastCheck.Int64 > 0 {
			lastCheckTime = time.UnixMilli(lastCheck.Int64).UTC().Format(time.RFC3339)
		}
		m := store.Mirror{
			UserID:         *user,
			Source:         src,
			ComicID:        comicID,
			DueAt:          due,
			LastCheckTime:  lastCheckTime,
			LastUpdateTime: lastUpdate.String,
			Priority:       "normal",
			UpdatedAt:      now,
		}
		if err := st.MergeMirror(m); err != nil {
			log.Fatalf("merge mirror %s/%s: %v", src, comicID, err)
		}
		count++
	}
	if err := rows.Err(); err != nil {
		log.Fatalf("rows err: %v", err)
	}
	fmt.Printf("seeded %d mirrors (user=%s source=%q offset=%s)\n", count, *user, *source, *offset)
}

func clearUserData(st *store.Store, user string) error {
	if _, err := st.DB().Exec(`DELETE FROM jobs WHERE user_id=?`, user); err != nil {
		return err
	}
	if _, err := st.DB().Exec(`DELETE FROM chunks WHERE user_id=?`, user); err != nil {
		return err
	}
	if _, err := st.DB().Exec(`DELETE FROM mirror WHERE user_id=?`, user); err != nil {
		return err
	}
	return nil
}
