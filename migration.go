package vermig

import "time"

type Migration struct {
	Id           string    `json:"id"`
	Name         string    `db:"name"`
	MajorVersion int       `db:"major_version"`
	MinorVersion int       `db:"minor_version"`
	PatchVersion int       `db:"patch_version"`
	Scope        string    `db:"scope"`
	QueryUp      string    `db:"query_up"`
	QueryDown    string    `db:"query_down"`
	CreatedAt    time.Time `db:"created_at"`
}
