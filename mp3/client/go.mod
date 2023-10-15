module mp3/client

go 1.21.2

replace mp3/common => ../common

require (
	github.com/sirupsen/logrus v1.9.3
	mp3/common v0.0.0-00010101000000-000000000000
)

require golang.org/x/sys v0.0.0-20220715151400-c0bba94af5f8 // indirect
