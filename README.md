# VERMIG
> Minimalistic version-based PostgreSQL migration tool for Go — no external CLI, just pure Go.

```go
package migrations

import (
	"context"
	"log"
	
	"github.com/daarxwalker/vermig"
	"github.com/jackc/pgx/v5/pgxpool"
)

//go:embed migrations/**/*.sql
var migrations embed.FS

func main() {
    ctx := context.Background()
    config, parseUriErr := pgxpool.ParseConfig("<DB_URI>")
    if parseUriErr != nil {
        log.Fatalf("parse db uri failed: %s\n", parseUriErr)
    }
    db, connectErr := pgxpool.NewWithConfig(ctx, config)
    if connectErr != nil {
        log.Fatalf("db connect failed: %s\n", connectErr)
    }
    mg, createMigratorErr := vermig.New(
        ctx,
        vermig.WithDB(db),
        vermig.WithFS(migrations),
    )
    if createMigratorErr != nil {
        log.Fatalf("create migrator failed: %s\n", createMigratorErr)
    }
    if migrateErr := mg.Migrate(ctx, "1.0.0"); migrateErr != nil {
        log.Fatalf("migrate failed: %s\n", migrateErr)
    }
}

```

<br>

## Files organization
### Example migrator package
project/
├── main.go
├── go.mod
└── migrations/
└── schema/
└── 00_users/
├── 1.0.0_create-users_up.sql
└── 1.0.0_create-users_down.sql

<br>

## Scope
> Nested folders are automatically used as migration scopes. <br>
> For example: schema/00_users becomes the scope name schema.00_users. <br>

<br>

## SQL filename conventions
- The version must follow semantic versioning.
- Three sections must be splitted by underscore (_).
- 1.0.0: version
- create-users: The name must use dashes instead of underscores.
- up/down: run upgrade or downgrade by version
```
<version>_<name>_<direction>.sql

1.0.0_create-users_up.sql
1.0.0_create-users_down.sql
```