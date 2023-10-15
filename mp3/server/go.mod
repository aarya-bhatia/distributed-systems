module mp3/server

go 1.21.2

replace mp3/common => ../common

require (
	github.com/jedib0t/go-pretty/v6 v6.4.8
	github.com/sirupsen/logrus v1.9.3
	mp3/common v0.0.0-00010101000000-000000000000
)

require (
	github.com/mattn/go-runewidth v0.0.13 // indirect
	github.com/rivo/uniseg v0.2.0 // indirect
	golang.org/x/sys v0.1.0 // indirect
)
