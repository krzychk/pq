package pq

import (
	"bytes"
	"database/sql/driver"
	"encoding/binary"
	"errors"
	"fmt"
	"strings"
	"time"
)

var (
	errReplicationConnClosed = errors.New("pq: Recplication connection has been closed")
	errReplicationConnReplicating = errors.New("pq: Replication connection is in replication state")
	errReplicationConnNotReplicating = errors.New("pq: Replication connection is not replicating")
)

// ReplicationConn represents PostgreSQL connection in walsender mode.
type ReplicationConn struct {
	cn *conn
	isOpened bool
	isReplicating bool
	msgsChan chan *ReplicationMsg
	quitReplicating chan bool
}

// PostgreSQL response to IDENTIFY_SYSTEM walsender command.
type SystemInfo struct {
	// systemid - The unique system identifier identifying the cluster.
	SystemId string

	// timeline - Current TimelineID.
	Timeline int64

	// xlogpos - Current xlog write location.
	XLogPos  string

	// dbname - Database connected to.
	DBName   string
}

const (
	MSG_X_LOG_DATA           byte = 'w'
	MSG_KEEPALIVE            byte = 'k'
	MSG_STATUS_UPDATE        byte = 'r'
	MSG_HOT_STANDBY_FEEDBACK byte = 'h'

)

// ReplicationMsg represents any message exchanged between server and frontend
// in COPY BOTH mode (after START_REPLICATION).
type ReplicationMsg struct {
	// Type of message indicating contents of Data field
	Type byte

	//Actual message (XLogDataMsg, KeepaliveMsg, StatusUpdateMsg or HotStandbyFeedbackMsg)
	Data interface{}
}

// XLogDataMsg contains replication data and corresponding log positions
type XLogDataMsg struct {
	// The starting point of the WAL data in this message.
	StartXLogPos   int64

	// The current end of WAL on the server.
	CurrentXLogPos int64

	// The server's system clock at the time of transmission, as microseconds since midnight on 2000-01-01.
	Time           int64

  // Replication data
	Payload        []byte
}

type KeepaliveMsg struct {
	// The current end of WAL on the server.
	CurrentXLogPos int64

	// The server's system clock at the time of transmission, as microseconds since midnight on 2000-01-01.
	Time           int64

	// Indicates if client should reply to this message to avoid timeout.
	Reply          bool
}

// StatusUpdateMsg informs server about fronteds log position status.
// Can be used as a reply to KeepaliveMsg.
type StatusUpdateMsg struct {
	// The location of the last WAL byte + 1 received and written to disk in the standby.
	ReceivedXLogPos int64

	// The location of the last WAL byte + 1 flushed to disk in the standby.
	FlushedXLogPos  int64

	// The location of the last WAL byte + 1 applied in the standby.
	AppliedXLogPos  int64

	// The client's system clock at the time of transmission, as microseconds since midnight on 2000-01-01.
	Time            int64

	// If 1, the client requests the server to reply to this message immediately.
	Reply           byte
}

// Can be used as a reply to KeepaliveMsg.
type HotStandbyFeedbackMsg struct {
	// The client's system clock at the time of transmission, as microseconds since midnight on 2000-01-01.
	Time  int64

	// The standby's current xmin.
	XMin  int32

	// The standby's current epoch.
	Epoch int32
}

// NewReplicationConn creates new database connection in walsender mode.
// connectionString doesn't have to contain "database=" part.
func NewReplicationConn(connectionString string) (*ReplicationConn, error) {
	cn, err := Open(withReplication(connectionString))
	if err != nil {
		return nil, err
	}
	return &ReplicationConn{
		cn: cn.(*conn), 
		isOpened: true, 
		msgsChan: make(chan *ReplicationMsg, 256), 
		quitReplicating: make(chan bool),
	}, nil
}

// Informs backend to start streaming logical replication messages.
// In streaming mode only RecvMessage, SendMessage and StopReplication are allowed.
// Returns error if connection is already in streaming mode.
func (self *ReplicationConn) StartLogicalReplication(slot string, xLogPos string) error {
	q := fmt.Sprintf("START_REPLICATION SLOT %s LOGICAL %s", slot, xLogPos)
	return self.startReplication(q)
}

// Informs backend to start streaming physical replication messages.
// In streaming mode only RecvMessage, SendMessage and StopReplication are allowed.
// Returns error if connection is already in streaming mode.
func (self *ReplicationConn) StartPhysicalReplication(slot string, xLogPos string, timeline int64) error {
	var timelinePart string
	if timeline > 0 {
		timelinePart = fmt.Sprintf(" TIMELINE %d", timeline)
	} else {
		timelinePart = ""
	}
	q := fmt.Sprintf("START_REPLICATION SLOT %s PHYSICAL %s%s", slot, xLogPos, timelinePart)
	return self.startReplication(q)
}

func (self *ReplicationConn) startReplication(q string) error {
	if !self.isOpened {
		return errReplicationConnClosed
	}
	if self.isReplicating {
		return errReplicationConnReplicating
	}

	b := self.cn.writeBuf('Q')
	b.string(q)
	self.cn.send(b)
	
	typ, m := self.cn.recv1()
	if typ != 'W' {
		return errors.New(fmt.Sprintf("pq: Expected Copy Both mode; got %c, %v", typ, string(*m)))
	}

	self.isReplicating = true

	go self.recvMessages()

	return nil
}

func (self *ReplicationConn) recvMessages() {
	r := &readBuf{}
	recvMessagesLoop:
	for {
		typ, err := self.cn.recvMessage(r)
		if err != nil {
			break recvMessagesLoop
		}
		switch typ {
		case 'C':
		case 'd':
			msg := parseMessage(r)
			self.msgsChan <-msg
		case 'Z', 'c':
			if self.isReplicating {
				self.isReplicating = false
			} else {
				break recvMessagesLoop
			}
		case 'E':
			break recvMessagesLoop
		}
	}
	self.isReplicating = false
	select {
	case self.quitReplicating <-true:
	default:
	}
	select {
	case self.msgsChan <-nil:
	default:
	}
}

// Informs server to exit replication streaming mode.
// Returns error if connection is not in streaming mode.
func (self *ReplicationConn) StopReplication() error {
	if !self.isReplicating {
		return errReplicationConnNotReplicating
	}

	err := self.cn.sendSimpleMessage('c')
	if err != nil {
		return err
	}

	<-self.quitReplicating
	return nil
}

// Closes database connection.
func (self *ReplicationConn) Close() error {
	if !self.isOpened {
		return errReplicationConnClosed
	}

	self.cn.Close()
	self.isOpened = false
	self.isReplicating = false

	return nil
}

// Requests the server to identify itself.
// Returns error if connection is in walsender mode.
func (self *ReplicationConn) IdentifySystem() (*SystemInfo, error) {
	if !self.isOpened {
		return nil, errReplicationConnClosed
	}
	if self.isReplicating {
		return nil, errReplicationConnReplicating
	}

	rows, err := self.cn.simpleQuery("IDENTIFY_SYSTEM")
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	values := make([]driver.Value, 4)

	err = rows.Next(values)
	if err != nil {
		return nil, err
	}

	systemInfo := &SystemInfo{
		SystemId: string(values[0].([]byte)),
		Timeline: values[1].(int64),
		XLogPos:  string(values[2].([]byte)),
		DBName:   string(values[3].([]byte)),
	}

	return systemInfo, nil
}

// Creates physical replication slot of given name.
// Returns error if connection is in walsender mode.
func (self *ReplicationConn) CreatePhysicalReplicationSlot(name string) error {
	return self.createReplicationSlot(fmt.Sprintf("CREATE_REPLICATION_SLOT %s PHYSICAL", name))
}

// Creates physical replication slot of given name using speciefied output plugin.
// Returns error if connection is in walsender mode.
func (self *ReplicationConn) CreateLogicalReplicationSlot(name string, outputPlugin string) error {
	return self.createReplicationSlot(fmt.Sprintf("CREATE_REPLICATION_SLOT %s LOGICAL %s", name, outputPlugin))
}

func (self *ReplicationConn) createReplicationSlot(q string) error {
	if !self.isOpened {
		return errReplicationConnClosed
	}
	if self.isReplicating {
		return errReplicationConnReplicating
	}
	rows, err := self.cn.simpleQuery(q)
	if err != nil {
		return err
	}
	rows.Close()
	return nil
}

// Drops replication slot.
// Returns error if connection is in walsender mode.
func (self *ReplicationConn) DropReplicationSlot(name string) error {
	if !self.isOpened {
		return errReplicationConnClosed
	}
	if self.isReplicating {
		return errReplicationConnReplicating
	}
	rows, err := self.cn.simpleQuery(fmt.Sprintf("DROP_REPLICATION_SLOT %s", name))
	if err != nil {
		return err
	}
	rows.Close()
	return nil
}

func (self *ReplicationConn) writeCopyData(data []byte) error {
	buffer := self.cn.writeBuf('d')
	buffer.string(string(data))
	self.cn.send(buffer)
	return nil
}

// Sends update status message to server.
// Create message using NewStatusUpdateMsg or NewHotStandbyFeedbackMsg.
// Returns error if connection is not in walsender mode.
func (self *ReplicationConn) SendMessage(msg *ReplicationMsg) error {
	if !self.isReplicating {
		return errReplicationConnNotReplicating
	}
	buffer := new(bytes.Buffer)
	err := binary.Write(buffer, binary.BigEndian, msg.Type)
	if err != nil {
		return err
	}
	err = binary.Write(buffer, binary.BigEndian, msg.Data)
	if err != nil {
		return err
	}
	return self.writeCopyData(buffer.Bytes())
}

// Receives the replication message (XLogDataMsg or KeepaliveMsg) from server.
// Returns error if connection is not in walsender mode and there is no messages left in the buffer.
func (self *ReplicationConn) RecvMessage() (*ReplicationMsg, error) {
	var msg *ReplicationMsg
	select {
	case msg = <-self.msgsChan:
	default:
		if !self.isReplicating {
			return nil, errReplicationConnNotReplicating
		} else {
			msg = <-self.msgsChan
		}
	}
	if msg == nil {
		return nil, errReplicationConnNotReplicating
	}
	return msg, nil
}

func parseMessage(buf *readBuf) *ReplicationMsg {
	switch (*buf)[0] {
	case MSG_X_LOG_DATA:
		return newXLogDataMsg(buf)
	case MSG_KEEPALIVE:
		return newKeepaliveMsg(buf)
	}
	panic(fmt.Sprintf("Unknown message type: %c", (*buf)[0]))
}

// Transforms int64 log position value into its string representation.
func XLogPosIntToStr(xLogPos int64) string {
	high := uint32(xLogPos >> 32)
	low := uint32(xLogPos)
	return fmt.Sprintf("%X/%X", high, low)
}

// Transforms string representation of log position value into int64.
func XLogPosStrToInt(xLogPos string) int64 {
	var high, low uint32
	fmt.Sscanf(xLogPos, "%X/%X", &high, &low)
	return (int64(high) << 32) | int64(low)
}

// Returns number of microseconds since 2000-01-01 00:00:00.
// This date format is required by status update messages.
func GetCurrentTimestamp() int64 {
	return time.Now().UnixNano() / 1000 - 946684800000000 //microseconds since 2000-01-01 00:00:00
}

// Creates new status update message.
func NewStatusUpdateMsg(receivedXLogPos int64, flushedXLogPos int64, appliedXLogPos int64, time int64, reply bool) *ReplicationMsg {
	msg := &ReplicationMsg{Type: MSG_STATUS_UPDATE}
	data := &StatusUpdateMsg{
		ReceivedXLogPos: receivedXLogPos,
		FlushedXLogPos:  flushedXLogPos,
		AppliedXLogPos:  appliedXLogPos,
		Time:            time,
	}
	if reply {
		data.Reply = 1
	} else {
		data.Reply = 0
	}
	msg.Data = data
	return msg
}

// Creates new hot standby feedback message.
func NewHotStandbyFeedbackMsg(time int64, xMin int32, epoch int32) *ReplicationMsg {
	msg := &ReplicationMsg{Type: MSG_HOT_STANDBY_FEEDBACK}
	data := &HotStandbyFeedbackMsg{
		Time:  time,
		XMin:  xMin,
		Epoch: epoch,
	}
	msg.Data = data
	return msg
}

func newXLogDataMsg(payload *readBuf) *ReplicationMsg {
	msg := &ReplicationMsg{Type: MSG_X_LOG_DATA}
	data := &XLogDataMsg{}
	msg.Data = data
	buf := bytes.NewReader((*payload)[1:])
	binary.Read(buf, binary.BigEndian, &data.StartXLogPos)
	binary.Read(buf, binary.BigEndian, &data.CurrentXLogPos)
	binary.Read(buf, binary.BigEndian, &data.Time)
	data.Payload = make([]byte, buf.Len())
	buf.Read(data.Payload)
	return msg
}

func newKeepaliveMsg(payload *readBuf) *ReplicationMsg {
	msg := &ReplicationMsg{Type: MSG_KEEPALIVE}
	data := &KeepaliveMsg{}
	msg.Data = data
	buf := bytes.NewReader((*payload)[1:])
	binary.Read(buf, binary.BigEndian, &data.CurrentXLogPos)
	binary.Read(buf, binary.BigEndian, &data.Time)
	var reply byte
	binary.Read(buf, binary.BigEndian, &reply)
	data.Reply = reply != 0
	return msg
}

func withReplication(connectionString string) string {
	if strings.Contains(connectionString, "replication=") {
		return connectionString
	} 
	return connectionString + " replication=database"
}
