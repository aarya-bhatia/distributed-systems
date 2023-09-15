package server

import (
	"fmt"
)

type Host struct {
	Address string
	Port    string
	ID      string
}

func NewHost(Address string, Port string, ID string) Host {
	return Host{Address, Port, ID}
}

func (host Host) GetSignature() string {
	return fmt.Sprintf("%s:%s:%s", host.Address, host.Port, host.ID)
}
