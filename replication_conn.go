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

type ReplicationConn struct {
	cn *conn
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
	return &ReplicationConn{cn: cn.(*conn)}, nil
}

func (self *ReplicationConn) StartReplication(slot string, xLogPos string) error {
	q := fmt.Sprintf("START_REPLICATION SLOT %s LOGICAL %s", slot, xLogPos)
	return self.startReplication(q)
}

func (self *ReplicationConn) startReplication(q string) error {
	_, err := self.cn.simpleQuery(q)
	if err != nil {
		return err
	}
	typ, _ := self.cn.recv1()
	if typ != 'W' {
		return errors.New("pq: Expected Copy Both mode")
	}
	return nil
}

func (self *ReplicationConn) IdentifySystem() (*SystemInfo, error) {
	rows, err := self.cn.simpleQuery("IDENTIFY_SYSTEM")
	if err != nil {
		return nil, err
	}
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
	_, err := self.cn.simpleQuery(q)
	if err != nil {
		return err
	}
	return nil
}

func (self *ReplicationConn) DropReplicationSlot(name string) error {
	_, err := self.cn.simpleQuery(fmt.Sprintf("DROP_REPLICATION_SLOT %s", name))
	if err != nil {
		return err
	}
	return nil
}

func (self *ReplicationConn) writeCopyData(data []byte) error {
	buffer := self.cn.writeBuf('d')
	buffer.string(string(data))
	self.cn.send(buffer)
	return nil
}

func (self *ReplicationConn) SendMessage(msg *ReplicationMsg) error {
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
	typ, buf := self.cn.recv1()
	if typ != 'd' {
		return nil, errors.New("pq: Expected COPY message, got " + string(typ) + "with " + string(*buf))
	}
	switch (*buf)[0] {
	case MSG_X_LOG_DATA:
		return newXLogDataMsg(buf), nil
	case MSG_KEEPALIVE:
		return newKeepaliveMsg(buf), nil
	default:
		return nil, errors.New("pq: Unknown replication message")
	}
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
