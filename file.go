package vermig

import (
	"github.com/Masterminds/semver"
)

type File struct {
	Priority []int
	Version  *semver.Version
	Scope    string
	Name     string
	UpPath   string
	DownPath string
}
