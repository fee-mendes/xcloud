module github.com/feemendes/xcloud

go 1.20

require github.com/gocql/gocql v1.7.0

require (
	github.com/google/uuid v1.6.0 // indirect
	github.com/klauspost/compress v1.17.9 // indirect
	gopkg.in/inf.v0 v0.9.1 // indirect
)

replace github.com/gocql/gocql => github.com/scylladb/gocql v1.15.1
