package vermig

import "embed"

type Option func(*Vermig)

func WithDB(db DB) Option {
	return func(v *Vermig) {
		v.db = db
	}
}

func WithFS(fs embed.FS) Option {
	return func(v *Vermig) {
		v.fs = fs
	}
}

func WithAllowDowngrade(allowDowngrade bool) Option {
	return func(v *Vermig) {
		v.allowDowngrade = allowDowngrade
	}
}
