package vermig

import "time"

type Migration struct {
	Id         string    `json:"id"`
	Name       string    `db:"name"`
	Version    string    `db:"version"`
	Major      int64     `db:"major"`
	Minor      int64     `db:"minor"`
	Patch      int64     `db:"patch"`
	Prerelease string    `db:"prelease"`
	Scope      string    `db:"scope"`
	Up         string    `db:"up"`
	Down       string    `db:"down"`
	Checksum   string    `db:"checksum"`
	CreatedAt  time.Time `db:"created_at"`
}
