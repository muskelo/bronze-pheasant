package log

import "github.com/sirupsen/logrus"

var logger *logrus.Logger

func init() {
	logger = logrus.New()
    logger.Formatter = &logrus.TextFormatter{FullTimestamp: true, TimestampFormat: "2006-01-02T15:04:05"}
}

func G(goroutine string) *logrus.Entry {
    return logger.WithField("goroutine", goroutine)
}
