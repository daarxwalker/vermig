package vermig

import (
	"context"
	"embed"
	"errors"
	"fmt"
	"io/fs"
	"log"
	"strconv"
	"strings"

	"github.com/Masterminds/squirrel"
	"github.com/georgysavva/scany/v2/pgxscan"
	"github.com/jackc/pgx/v5"
)

type Vermig struct {
	db DB
	fs embed.FS
}

func New(ctx context.Context, options ...Option) (*Vermig, error) {
	m := new(Vermig)
	for _, option := range options {
		option(m)
	}
	migrationsTableExists, getMigrationsTableExistErr := m.migrationsTableExists(ctx)
	if getMigrationsTableExistErr != nil {
		return nil, fmt.Errorf("get migrations table exitence failed: %w", getMigrationsTableExistErr)
	}
	if migrationsTableExists {
		return m, nil
	}
	if err := m.createTableIfNotExists(ctx); err != nil {
		return nil, fmt.Errorf("create migrations table failed: %w", err)
	}
	return m, nil
}

func (m *Vermig) Migrate(ctx context.Context, version string) error {
	pv, parseVersionErr := m.parseVersion(version)
	if parseVersionErr != nil {
		return fmt.Errorf("parse version failed: %w", parseVersionErr)
	}
	higherMigrations, findMigrationsErr := m.findHigherVersionMigrations(ctx, m.db, pv[0], pv[1], pv[2])
	if findMigrationsErr != nil {
		return fmt.Errorf("find higher version migrations failed: %w", findMigrationsErr)
	}
	tx, beginErr := m.db.Begin(ctx)
	if beginErr != nil {
		return fmt.Errorf("begin migrations failed: %w", beginErr)
	}
	if len(higherMigrations) > 0 {
		if migrateDownErr := m.migrateDown(ctx, tx, higherMigrations); migrateDownErr != nil {
			if rollbackErr := tx.Rollback(ctx); rollbackErr != nil {
				return errors.Join(
					fmt.Errorf("rollback while downgrade db failed: %w", rollbackErr),
					fmt.Errorf("downgrade db failed: %w", migrateDownErr),
				)
			}
			return fmt.Errorf("downgrade db failed: %w", migrateDownErr)
		}
	}
	if len(higherMigrations) == 0 {
		if migrateUpErr := m.migrateUp(ctx, tx, pv[0], pv[1], pv[2]); migrateUpErr != nil {
			if rollbackErr := tx.Rollback(ctx); rollbackErr != nil {
				return errors.Join(
					fmt.Errorf("rollback while upgrade db failed: %w", rollbackErr),
					fmt.Errorf("upgrade db failed: %w", migrateUpErr),
				)
			}
			return fmt.Errorf("upgrade db failed: %w", migrateUpErr)
		}
	}
	if commitErr := tx.Commit(ctx); commitErr != nil {
		if rollbackErr := tx.Rollback(ctx); rollbackErr != nil {
			return errors.Join(
				fmt.Errorf("rollback while commit migrations failed: %w", rollbackErr),
				fmt.Errorf("commit migrations failed: %w", commitErr),
			)
		}
		return fmt.Errorf("commit migrations failed: %w", commitErr)
	}
	log.Println("migrator status: ✅")
	return nil
}

func (m *Vermig) migrateUp(
	ctx context.Context, tx pgx.Tx,
	currentMajorVersion, currentMinorVersion, currentPatchVersion int,
) error {
	if err := fs.WalkDir(
		m.fs, "migrations", func(path string, entry fs.DirEntry, err error) error {
			if entry.IsDir() {
				return nil
			}
			name := entry.Name()
			if strings.HasSuffix(name, "_down.sql") {
				return nil
			}
			downPath := strings.Replace(path, "_up.sql", "_down.sql", 1)
			version := name[:strings.Index(name, "_")]
			pv, parseVersionErr := m.parseVersion(version)
			if parseVersionErr != nil {
				return fmt.Errorf("parse version failed: %w", parseVersionErr)
			}
			if pv[0] < currentMajorVersion ||
				(pv[0] == currentMajorVersion && pv[1] < currentMinorVersion) ||
				(pv[0] == currentMajorVersion && pv[1] == currentMinorVersion && pv[2] < currentPatchVersion) {
				return nil
			}
			scope := strings.TrimPrefix(path, "migrations/")
			scope = strings.TrimSuffix(scope, "/"+name)
			migrationExists, _ := m.migrationExists(ctx, tx, name, scope)
			if migrationExists {
				return nil
			}
			fileBytes, readMigrationUp := m.fs.ReadFile(path)
			if readMigrationUp != nil {
				return fmt.Errorf("read migration file failed: %w", readMigrationUp)
			}
			var queryDown string
			if downFileBytes, readMigrationDown := m.fs.ReadFile(downPath); readMigrationDown == nil {
				queryDown = string(downFileBytes)
			}
			queryUp := string(fileBytes)
			if _, execErr := tx.Exec(ctx, queryUp); execErr != nil {
				return fmt.Errorf("run migration up failed: %w", execErr)
			}
			log.Printf("🔼 %s/%s: ✅\n", scope, name)
			if insertMigrationErr := m.insertMigration(
				ctx, tx, Migration{
					Name:         name,
					MajorVersion: pv[0],
					MinorVersion: pv[1],
					PatchVersion: pv[2],
					Scope:        scope,
					QueryUp:      queryUp,
					QueryDown:    queryDown,
				},
			); insertMigrationErr != nil {
				return fmt.Errorf("insert migration failed: %w", insertMigrationErr)
			}
			return nil
		},
	); err != nil {
		return fmt.Errorf("scan migrations failed: %w", err)
	}
	return nil
}

func (m *Vermig) migrateDown(ctx context.Context, tx pgx.Tx, migrations []Migration) error {
	ids := make([]string, len(migrations))
	for i, migration := range migrations {
		if _, execErr := tx.Exec(ctx, migration.QueryDown); execErr != nil {
			return fmt.Errorf("run migration down failed: %w", execErr)
		}
		log.Printf("🔽 %s/%s: ✅\n", migration.Scope, migration.Name)
		ids[i] = migration.Id
	}
	if deleteMigrationsErr := m.deleteMigrations(ctx, tx, ids...); deleteMigrationsErr != nil {
		return fmt.Errorf("delete migrations failed: %w", deleteMigrationsErr)
	}
	return nil
}

func (m *Vermig) migrationsTableExists(ctx context.Context) (bool, error) {
	query := `SELECT EXISTS (
    SELECT FROM
        pg_tables
    WHERE
        schemaname = 'public' AND
        tablename  = 'migrations'
    );`
	var exists bool
	if err := pgxscan.Get(ctx, m.db, &exists, query); err != nil {
		return false, fmt.Errorf("check migrations table existence failed: %w", err)
	}
	return exists, nil
}

func (m *Vermig) createTableIfNotExists(ctx context.Context) error {
	query := `CREATE TABLE IF NOT EXISTS migrations (
  id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
	name VARCHAR(255) NOT NULL,
	major_version INT NOT NULL,
	minor_version INT NOT NULL,
	patch_version INT NOT NULL,
	scope VARCHAR(255) NOT NULL,
	query_up TEXT NOT NULL,
	query_down TEXT NOT NULL,
	created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
	CONSTRAINT uq_scope_version UNIQUE (scope, major_version, minor_version, patch_version)
);
CREATE INDEX IF NOT EXISTS idx_migrations_name ON migrations (name);
CREATE INDEX IF NOT EXISTS idx_migrations_major_version ON migrations (major_version);
CREATE INDEX IF NOT EXISTS idx_migrations_minor_version ON migrations (minor_version);
CREATE INDEX IF NOT EXISTS idx_migrations_patch_version ON migrations (patch_version);
CREATE INDEX IF NOT EXISTS idx_migrations_scope ON migrations (scope);`
	if _, err := m.db.Exec(ctx, query); err != nil {
		return fmt.Errorf("create migrations table failed: %w", err)
	}
	return nil
}

func (m *Vermig) findHigherVersionMigrations(
	ctx context.Context, db DB, majorVersion, minorVersion, patchVersion int,
) ([]Migration, error) {
	sql, args, createSqlErr := squirrel.Select().
		Columns("id", "name", "major_version", "minor_version", "patch_version", "scope", "query_up", "query_down").
		From("migrations").
		Where("(major_version, minor_version, patch_version) > (?, ?, ?)", majorVersion, minorVersion, patchVersion).
		OrderBy("major_version DESC", "minor_version DESC", "patch_version DESC").
		PlaceholderFormat(squirrel.Dollar).
		ToSql()
	if createSqlErr != nil {
		return nil, fmt.Errorf("create higher version migrations sql failed: %w", createSqlErr)
	}
	rows, queryErr := db.Query(ctx, sql, args...)
	if queryErr != nil {
		return nil, fmt.Errorf("find higher version migrations failed: %w", queryErr)
	}
	defer rows.Close()
	var result []Migration
	if scanErr := pgxscan.ScanAll(&result, rows); scanErr != nil {
		return nil, fmt.Errorf("scan higher version migrations failed: %w", scanErr)
	}
	return result, nil
}

func (m *Vermig) migrationExists(ctx context.Context, db DB, name, scope string) (bool, error) {
	sql, args, createSqlErr := squirrel.Select().
		Columns("true").
		From("migrations").
		Where(squirrel.Eq{"name": name, "scope": scope}).
		PlaceholderFormat(squirrel.Dollar).
		ToSql()
	if createSqlErr != nil {
		return false, fmt.Errorf("create migration exists sql failed: %w", createSqlErr)
	}
	var exists bool
	if scanErr := pgxscan.Get(ctx, db, &exists, sql, args...); scanErr != nil {
		return false, fmt.Errorf("get migration exists failed: %w", scanErr)
	}
	return exists, nil
}

func (m *Vermig) insertMigration(ctx context.Context, db DB, migration Migration) error {
	sql, args, createSqlErr := squirrel.Insert("migrations").
		Columns("name", "major_version", "minor_version", "patch_version", "scope", "query_up", "query_down").
		Values(
			migration.Name, migration.MajorVersion, migration.MinorVersion, migration.PatchVersion, migration.Scope,
			migration.QueryUp, migration.QueryDown,
		).
		Suffix("RETURNING \"id\"").
		PlaceholderFormat(squirrel.Dollar).
		ToSql()
	if createSqlErr != nil {
		return fmt.Errorf("create migration insert sql failed: %w", createSqlErr)
	}
	if _, execErr := db.Exec(ctx, sql, args...); execErr != nil {
		return fmt.Errorf("insert migration failed: %w", execErr)
	}
	return nil
}

func (m *Vermig) deleteMigrations(ctx context.Context, db DB, ids ...string) error {
	sql, args, createSqlErr := squirrel.Delete("migrations").
		Where(squirrel.Eq{"id": ids}).
		PlaceholderFormat(squirrel.Dollar).
		ToSql()
	if createSqlErr != nil {
		return fmt.Errorf("create migration insert sql failed: %w", createSqlErr)
	}
	if _, execErr := db.Exec(ctx, sql, args...); execErr != nil {
		return fmt.Errorf("insert migration failed: %w", execErr)
	}
	return nil
}

func (m *Vermig) parseVersion(version string) ([3]int, error) {
	result := [3]int{-1, -1, -1}
	splittedVersion := strings.Split(version, ".")
	majorVersion, err := strconv.Atoi(splittedVersion[0])
	minorVersion, err := strconv.Atoi(splittedVersion[1])
	patchVersion, err := strconv.Atoi(splittedVersion[2])
	if err != nil {
		return result, fmt.Errorf("convert string to int failed: %w", err)
	}
	result[0] = majorVersion
	result[1] = minorVersion
	result[2] = patchVersion
	return result, nil
}
