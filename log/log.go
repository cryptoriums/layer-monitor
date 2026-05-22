package log

import (
	"os"
	"time"

	"github.com/rs/zerolog"

	"cosmossdk.io/log"
)

func New() log.Logger {
	zerolog.TimeFieldFormat = time.StampMilli

	return log.NewLogger(
		os.Stderr,
		log.LevelOption(zerolog.DebugLevel),
		log.ColorOption(false),
		log.TimeFormatOption(time.StampMilli),
	)
}
