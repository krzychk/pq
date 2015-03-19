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

var errReplicationConnClosed = errors.New("pq: Recplication connection has been closed")
var errReplicationConnReplicating = errors.New("pq: Replication connection is in replication state")
var errReplicationConnNotReplicating = errors.New("pq: Replication connection is not replicating")

type ReplicationConn struct {
	cn *conn
	isOpened bool
	isReplicating bool
	msgsChan chan *ReplicationMsg
	quitReplicating chan bool
}

type SystemInfo struct {
	SystemId string
	Timeline int64
	XLogPos  string
	DBName   string
}

const (
	MSG_X_LOG_DATA           byte = 'w'
	MSG_KEEPALIVE            byte = 'k'
	MSG_STATUS_UPDATE        byte = 'r'
	MSG_HOT_STANDBY_FEEDBACK byte = 'h'

)

type ReplicationMsg struct {
	Type byte
	Data interface{}
}

type XLogDataMsg struct {
	StartXLogPos   int64
	CurrentXLogPos int64
	Time           int64
	Payload        []byte
}

type KeepaliveMsg struct {
	CurrentXLogPos int64
	Time           int64
	Reply          bool
}

type StatusUpdateMsg struct {
	ReceivedXLogPos int64
	FlushedXLogPos  int64
	AppliedXLogPos  int64
	Time            int64
	Reply           byte
}

type HotStandbyFeedbackMsg struct {
	Time  int64
	XMin  int32
	Epoch int32
}

func NewReplicationConn(name string) (*ReplicationConn, error) {
	cn, err := Open(nameWithReplication(name))
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

func (self *ReplicationConn) StartLogicalReplication(slot string, xLogPos string) error {
	q := fmt.Sprintf("START_REPLICATION SLOT %s LOGICAL %s", slot, xLogPos)
	return self.startReplication(q)
}

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

func (self *ReplicationConn) Close() error {
	if !self.isOpened {
		return errReplicationConnClosed
	}

	self.cn.Close()
	self.isOpened = false

	return nil
}

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

func (self *ReplicationConn) CreatePhysicalReplicationSlot(name string) error {
	return self.createReplicationSlot(fmt.Sprintf("CREATE_REPLICATION_SLOT %s PHYSICAL", name))
}

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

func (self *ReplicationConn) RecvMessage() (*ReplicationMsg, error) {
	if !self.isReplicating {
		return nil, errReplicationConnNotReplicating
	}
	msg := <-self.msgsChan
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

func XLogPosIntToStr(xLogPos int64) string {
	high := uint32(xLogPos >> 32)
	low := uint32(xLogPos)
	return fmt.Sprintf("%X/%X", high, low)
}

func XLogPosStrToInt(xLogPos string) int64 {
	var high, low uint32
	fmt.Sscanf(xLogPos, "%X/%X", &high, &low)
	return (int64(high) << 32) | int64(low)
}

func GetCurrentTimestamp() int64 {
	t := time.Now().UnixNano() / 1000
	return t - (((2451545 - 2440588) * 86400) * 1000000)
}

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

func nameWithReplication(name string) string {
	if strings.Contains(name, "replication=database") {
		return name
	}
	return name + " replication=database"
}
