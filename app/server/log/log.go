package log

import "github.com/sirupsen/logrus"

var logger *logrus.Logger

func init() {
	logger = logrus.New()
    logger.Formatter = &logrus.TextFormatter{FullTimestamp: true}
}

func Logg(goroutine string) *logrus.Entry {
    return logger.WithField("goroutine", goroutine)
}
