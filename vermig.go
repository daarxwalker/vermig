package vermig

import (
	"context"
	"embed"
	"errors"
	"fmt"
	"io/fs"
	"log"
	"sort"
	"strconv"
	"strings"

	"github.com/Masterminds/semver"
	"github.com/Masterminds/squirrel"
	"github.com/georgysavva/scany/v2/pgxscan"
	"github.com/jackc/pgx/v5"
)

type Vermig struct {
	db             DB
	fs             embed.FS
	allowDowngrade bool
	files          []File
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

func (m *Vermig) MigrateLatest(ctx context.Context) error {
	candidates := make([]*semver.Version, 0)
	if err := fs.WalkDir(
		m.fs, ".", func(path string, entry fs.DirEntry, err error) error {
			if path == "" || entry.IsDir() {
				return nil
			}
			name := entry.Name()
			if strings.HasSuffix(name, "_down.sql") {
				return nil
			}
			rawVersion := name[:strings.Index(name, "_")]
			version, parseVersionErr := semver.NewVersion(rawVersion)
			if parseVersionErr != nil {
				return fmt.Errorf("parse version failed: %w", parseVersionErr)
			}
			candidates = append(candidates, version)
			return nil
		},
	); err != nil {
		return fmt.Errorf("find latest migration failed: %w", err)
	}
	if len(candidates) == 0 {
		return fmt.Errorf("no migrations found")
	}
	sort.Sort(semver.Collection(candidates))
	return m.Migrate(ctx, candidates[len(candidates)-1].String())
}

func (m *Vermig) Migrate(ctx context.Context, version string) error {
	pv, parseVersionErr := semver.NewVersion(version)
	if parseVersionErr != nil {
		return fmt.Errorf("parse version failed: %w", parseVersionErr)
	}
	higherMigrations, findMigrationsErr := m.findHigherVersionMigrations(
		ctx, m.db, pv,
	)
	if findMigrationsErr != nil {
		return fmt.Errorf("find higher version migrations failed: %w", findMigrationsErr)
	}
	tx, beginErr := m.db.Begin(ctx)
	if beginErr != nil {
		return fmt.Errorf("begin migrations failed: %w", beginErr)
	}
	if err := m.collectFiles(); err != nil {
		return fmt.Errorf("collect migrations failed: %w", err)
	}
	if err := m.verifyIntegrity(ctx, tx); err != nil {
		if rollbackErr := tx.Rollback(ctx); rollbackErr != nil {
			return errors.Join(
				fmt.Errorf("rollback while verify integrity failed: %w", rollbackErr),
				fmt.Errorf("verify integrity failed: %w", err),
			)
		}
		return fmt.Errorf("verify integrity failed: %w", err)
	}
	if !m.allowDowngrade && higherMigrations != nil && len(higherMigrations) > 0 {
		log.Printf("⚠️ downgrade not enabled\n")
	}
	if m.allowDowngrade && higherMigrations != nil && len(higherMigrations) > 0 {
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
	if higherMigrations == nil || len(higherMigrations) == 0 {
		if migrateUpErr := m.migrateUp(
			ctx, tx, pv,
		); migrateUpErr != nil {
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
	targetVersion *semver.Version,
) error {
	for _, file := range m.files {
		if file.Version.GreaterThan(targetVersion) {
			continue
		}
		migrationExists, existsErr := m.migrationExists(ctx, tx, file.Name, file.Scope)
		if existsErr != nil {
			return fmt.Errorf("verify migration existence failed: %w", existsErr)
		}
		if migrationExists {
			continue
		}
		fileBytes, readMigrationUp := m.fs.ReadFile(file.UpPath)
		if readMigrationUp != nil {
			return fmt.Errorf("read migration file failed: %w", readMigrationUp)
		}
		var queryDown string
		if downFileBytes, readMigrationDown := m.fs.ReadFile(file.DownPath); readMigrationDown == nil {
			queryDown = string(downFileBytes)
		}
		queryUp := string(fileBytes)
		if _, execErr := tx.Exec(ctx, queryUp); execErr != nil {
			return fmt.Errorf("run migration up failed: %w", execErr)
		}
		log.Printf("🔼 %s/%s: ✅\n", file.Scope, file.Name)
		if insertMigrationErr := m.insertMigration(
			ctx, tx, Migration{
				Name:       file.Name,
				Version:    file.Version.String(),
				Major:      file.Version.Major(),
				Minor:      file.Version.Minor(),
				Patch:      file.Version.Patch(),
				Prerelease: file.Version.Prerelease(),
				Scope:      file.Scope,
				Up:         queryUp,
				Down:       queryDown,
				Checksum:   createChecksum(queryUp, queryDown),
			},
		); insertMigrationErr != nil {
			return fmt.Errorf("insert migration failed: %w", insertMigrationErr)
		}
	}
	return nil
}

func (m *Vermig) migrateDown(ctx context.Context, tx pgx.Tx, migrations []Migration) error {
	ids := make([]string, len(migrations))
	for i, migration := range migrations {
		if _, execErr := tx.Exec(ctx, migration.Down); execErr != nil {
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
	version VARCHAR(64) NOT NULL,
	major INT NOT NULL,
	minor INT NOT NULL,
	patch INT NOT NULL,
	prerelease VARCHAR(128) NOT NULL,
	scope VARCHAR(255) NOT NULL,
	up TEXT NOT NULL,
	down TEXT NOT NULL,
	checksum TEXT NOT NULL,
	created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
	CONSTRAINT uq_scope_version UNIQUE (scope, version, major, minor, patch, prerelease)
);
CREATE INDEX IF NOT EXISTS idx_migrations_name ON migrations (name);
CREATE INDEX IF NOT EXISTS idx_migrations_version ON migrations (version);
CREATE INDEX IF NOT EXISTS idx_migrations_major_version ON migrations (major);
CREATE INDEX IF NOT EXISTS idx_migrations_minor_version ON migrations (minor);
CREATE INDEX IF NOT EXISTS idx_migrations_patch_version ON migrations (patch);
CREATE INDEX IF NOT EXISTS idx_migrations_prerelease ON migrations (prerelease);
CREATE INDEX IF NOT EXISTS idx_migrations_scope ON migrations (scope);`
	if _, err := m.db.Exec(ctx, query); err != nil {
		return fmt.Errorf("create migrations table failed: %w", err)
	}
	return nil
}

func (m *Vermig) verifyIntegrity(
	ctx context.Context, db DB,
) error {
	migrations, findErr := m.findAllMigrations(ctx, db)
	if findErr != nil {
		return fmt.Errorf("find all migrations failed: %w", findErr)
	}
	if len(migrations) == 0 {
		return nil
	}
	migrationChecksums := make(map[string]string, len(migrations))
	for _, migration := range migrations {
		migrationChecksums[migration.Scope+"/"+migration.Name] = migration.Checksum
	}
	for _, file := range m.files {
		var queryDown string
		downFileBytes, readMigrationDownErr := m.fs.ReadFile(file.DownPath)
		if readMigrationDownErr == nil {
			queryDown = string(downFileBytes)
		}
		if m.allowDowngrade && readMigrationDownErr != nil {
			return fmt.Errorf("%s migration missing: %w", file.DownPath, readMigrationDownErr)
		}
		fileBytes, readMigrationUp := m.fs.ReadFile(file.UpPath)
		if readMigrationUp != nil {
			return fmt.Errorf("read migration file failed: %w", readMigrationUp)
		}
		queryUp := string(fileBytes)
		checksum := createChecksum(queryUp, queryDown)
		storedChecksum, exists := migrationChecksums[file.Scope+"/"+file.Name]
		if !exists {
			continue
		}
		if checksum != storedChecksum {
			return fmt.Errorf("corrupted migration: %s", file.Name)
		}
	}
	return nil
}

func (m *Vermig) collectFiles() error {
	m.files = m.files[:0]
	if err := fs.WalkDir(
		m.fs, ".", func(path string, entry fs.DirEntry, err error) error {
			if path == "" || entry.IsDir() {
				return nil
			}
			name := entry.Name()
			if strings.HasSuffix(name, "_down.sql") {
				return nil
			}
			downPath := strings.Replace(path, "_up.sql", "_down.sql", 1)
			version := name[:strings.Index(name, "_")]
			pv, parseVersionErr := semver.NewVersion(version)
			if parseVersionErr != nil {
				return fmt.Errorf("parse version failed: %w", parseVersionErr)
			}
			scope := strings.TrimSuffix(path, "/"+name)
			m.files = append(
				m.files, File{
					Priority: m.parsePriority(path),
					Version:  pv,
					Scope:    scope,
					Name:     name,
					UpPath:   path,
					DownPath: downPath,
				},
			)
			return nil
		},
	); err != nil {
		return fmt.Errorf("scan migrations failed: %w", err)
	}
	m.sortFiles()
	return nil
}

func (m *Vermig) sortFiles() {
	sort.Slice(
		m.files, func(i, j int) bool {
			pi, pj := m.files[i].Priority, m.files[j].Priority
			for k := 0; k < min(len(pi), len(pj)); k++ {
				if pi[k] != pj[k] {
					return pi[k] < pj[k]
				}
			}
			if len(pi) != len(pj) {
				return len(pi) < len(pj)
			}
			if !m.files[i].Version.Equal(m.files[j].Version) {
				return m.files[i].Version.LessThan(m.files[j].Version)
			}
			return m.files[i].Name < m.files[j].Name
		},
	)
}

func (m *Vermig) parsePriority(path string) []int {
	parts := strings.Split(path, "/")
	var p []int
	for _, part := range parts {
		if n, _, ok := strings.Cut(part, "_"); ok {
			if v, err := strconv.Atoi(n); err == nil {
				p = append(p, v)
			}
		}
	}
	return p
}

func (m *Vermig) findAllMigrations(ctx context.Context, db DB) ([]Migration, error) {
	sql, args, createSqlErr := squirrel.Select().
		Columns(
			"id", "name", "version", "major", "minor", "patch", "prerelease", "scope", "up",
			"down", "checksum",
		).
		From("migrations").
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

func (m *Vermig) findHigherVersionMigrations(
	ctx context.Context, db DB, currentVersion *semver.Version,
) ([]Migration, error) {
	sql, args, createSqlErr := squirrel.Select().
		Columns(
			"id", "name", "version", "major", "minor", "patch", "prerelease", "scope", "up",
			"down", "checksum",
		).
		From("migrations").
		Where(
			"(major, minor, patch) > (?, ?, ?)", currentVersion.Major(), currentVersion.Minor(), currentVersion.Patch(),
		).
		OrderBy("major DESC", "minor DESC", "patch DESC").
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
	if len(result) == 0 {
		return nil, nil
	}
	var higherVersionMigrations []Migration
	for _, row := range result {
		version, parseErr := semver.NewVersion(row.Version)
		if parseErr != nil {
			return nil, fmt.Errorf("parse version failed: %w", parseErr)
		}
		if version == nil || !version.GreaterThan(currentVersion) {
			continue
		}
		higherVersionMigrations = append(higherVersionMigrations, row)
	}
	return higherVersionMigrations, nil
}

func (m *Vermig) migrationExists(ctx context.Context, db DB, name, scope string) (bool, error) {
	sql, args, createSqlErr := squirrel.Select().
		Columns("true").
		From("migrations").
		Where(squirrel.Eq{"name": name, "scope": scope}).
		PlaceholderFormat(squirrel.Dollar).
		ToSql()
	if createSqlErr != nil && !errors.Is(createSqlErr, pgx.ErrNoRows) {
		return false, fmt.Errorf("create migration exists sql failed: %w", createSqlErr)
	}
	var exists bool
	if scanErr := pgxscan.Get(ctx, db, &exists, sql, args...); scanErr != nil && !errors.Is(scanErr, pgx.ErrNoRows) {
		return false, fmt.Errorf("get migration exists failed: %w", scanErr)
	}
	return exists, nil
}

func (m *Vermig) insertMigration(ctx context.Context, db DB, migration Migration) error {
	sql, args, createSqlErr := squirrel.Insert("migrations").
		Columns(
			"name", "version", "major", "minor", "patch", "prerelease", "scope", "up", "down", "checksum",
		).
		Values(
			migration.Name, migration.Version, migration.Major, migration.Minor, migration.Patch, migration.Prerelease,
			migration.Scope, migration.Up, migration.Down, migration.Checksum,
		).
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
