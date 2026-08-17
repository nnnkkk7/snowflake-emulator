package query

type ExecutionContext struct {
	SessionID     string
	Database      string
	CurrentSchema string
}
