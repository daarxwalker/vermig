package vermig

import "embed"

type Option func(*Vermig)

func WithDB(db DB) Option {
	return func(vermig *Vermig) {
		vermig.db = db
	}
}

func WithFS(fs embed.FS) Option {
	return func(vermig *Vermig) {
		vermig.fs = fs
	}
}
