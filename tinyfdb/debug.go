package tinyfdb

import (
	"io"
	"os"
)

type DBDebug database

func (d *DBDebug) PrintRaceStacks(w io.Writer) {
	dd := (*database)(d)
	dd.mu.Lock()
	dd.raceStacks = w
	dd.mu.Unlock()
}

func defaultPrintRaceStacks() io.Writer {
	if os.Getenv("TINYFDB_RACE_TRACEBACK") != "" {
		return os.Stderr
	}
	return nil
}
