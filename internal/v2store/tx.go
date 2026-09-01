package v2store

import (
	"context"
	"database/sql"
	"fmt"
	"time"
)

type Tx struct {
	db     *DB
	conn   *sql.Conn
	closed bool
}

func (db *DB) ReadTx(ctx context.Context, fn func(*Tx) error) error {
	if ctx == nil {
		ctx = context.Background()
	}
	return db.withConn(ctx, func(conn *sql.Conn) error {
		if err := configureConn(ctx, conn); err != nil {
			return err
		}
		if _, err := conn.ExecContext(ctx, "BEGIN"); err != nil {
			return fmt.Errorf("begin read transaction: %w", err)
		}
		tx := &Tx{db: db, conn: conn}
		return finishTx(ctx, tx, fn)
	})
}

func (db *DB) WriteTx(ctx context.Context, fn func(*Tx) error) error {
	if ctx == nil {
		ctx = context.Background()
	}
	db.writeMu.Lock()
	defer db.writeMu.Unlock()

	return db.withConn(ctx, func(conn *sql.Conn) error {
		if err := configureConn(ctx, conn); err != nil {
			return err
		}
		if err := beginImmediate(ctx, conn, db.busyRetry); err != nil {
			return fmt.Errorf("begin write transaction: %w", err)
		}
		tx := &Tx{db: db, conn: conn}
		return finishTx(ctx, tx, fn)
	})
}

func finishTx(ctx context.Context, tx *Tx, fn func(*Tx) error) error {
	if fn == nil {
		_ = tx.Rollback()
		return fmt.Errorf("transaction callback is required")
	}
	if err := fn(tx); err != nil {
		_ = tx.Rollback()
		return err
	}
	if err := tx.db.inject("transaction.commit"); err != nil {
		_ = tx.Rollback()
		return err
	}
	return tx.Commit()
}

func (tx *Tx) ExecContext(ctx context.Context, query string, args ...any) (sql.Result, error) {
	return tx.conn.ExecContext(ctx, query, args...)
}

func (tx *Tx) QueryContext(ctx context.Context, query string, args ...any) (*sql.Rows, error) {
	return tx.conn.QueryContext(ctx, query, args...)
}

func (tx *Tx) QueryRowContext(ctx context.Context, query string, args ...any) *sql.Row {
	return tx.conn.QueryRowContext(ctx, query, args...)
}

func (tx *Tx) Commit() error {
	if tx.closed {
		return nil
	}
	_, err := tx.conn.ExecContext(context.Background(), "COMMIT")
	tx.closed = true
	return err
}

func (tx *Tx) Rollback() error {
	if tx.closed {
		return nil
	}
	_, err := tx.conn.ExecContext(context.Background(), "ROLLBACK")
	tx.closed = true
	return err
}

func (tx *Tx) Now() time.Time {
	return tx.db.Now()
}

func (tx *Tx) NewID(prefix string) string {
	return tx.db.NewID(prefix)
}
