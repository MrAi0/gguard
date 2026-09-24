package mysql

import "fmt"

// Command is the first payload byte of a client packet that starts a new
// command (the protocol's COM_* byte).
type Command byte

const (
	ComQuit            Command = 0x01
	ComInitDB          Command = 0x02
	ComQuery           Command = 0x03
	ComFieldList       Command = 0x04
	ComPing            Command = 0x0e
	ComChangeUser      Command = 0x11
	ComStmtPrepare     Command = 0x16
	ComStmtExecute     Command = 0x17
	ComStmtClose       Command = 0x19
	ComResetConnection Command = 0x1f
)

var commandNames = map[Command]string{
	ComQuit:            "COM_QUIT",
	ComInitDB:          "COM_INIT_DB",
	ComQuery:           "COM_QUERY",
	ComFieldList:       "COM_FIELD_LIST",
	ComPing:            "COM_PING",
	ComChangeUser:      "COM_CHANGE_USER",
	ComStmtPrepare:     "COM_STMT_PREPARE",
	ComStmtExecute:     "COM_STMT_EXECUTE",
	ComStmtClose:       "COM_STMT_CLOSE",
	ComResetConnection: "COM_RESET_CONNECTION",
}

func (c Command) String() string {
	if name, ok := commandNames[c]; ok {
		return name
	}
	return fmt.Sprintf("COM_UNKNOWN(0x%02x)", byte(c))
}

// ParseCommand reports the command carried by a client->server packet and
// its argument bytes (the SQL text for COM_QUERY, for example).
//
// Every command resets the sequence id to 0, so packets with any other id
// (handshake and auth responses, continuations of payloads over 16MB,
// LOAD DATA file contents) are not command starts and ok is false.
func ParseCommand(p Packet) (cmd Command, arg []byte, ok bool) {
	payload := p.Payload()
	if p.Seq() != 0 || len(payload) == 0 {
		return 0, nil, false
	}
	return Command(payload[0]), payload[1:], true
}
