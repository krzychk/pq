package pq

import (
  "testing"
  "os"
  "regexp"
  "time"
  "database/sql"
)

var replicationSlot = "replication_test"

func newTestReplicationConn(t *testing.T) *ReplicationConn {
  datname := os.Getenv("PGDATABASE")
  sslmode := os.Getenv("PGSSLMODE")

  if datname == "" {
    os.Setenv("PGDATABASE", "pqgotest")
  }

  if sslmode == "" {
    os.Setenv("PGSSLMODE", "disable")
  }

  r, err := NewReplicationConn("")
  if err != nil {
    t.Fatal(err)
  }

  return r
}

func openTestConnWithTable(t *testing.T) *sql.DB {
  db := openTestConn(t)
  _, err := db.Exec("CREATE TABLE IF NOT EXISTS repl (a int)")
  if err != nil {
    t.Fatal(err)
  }
  return db
}

func closeTestConnWithTable(t *testing.T, db  *sql.DB) {
  db.Exec("DROP TABLE repl")
  db.Close()
}

func createTestReplicationSlot(t *testing.T, r *ReplicationConn) {
  r.DropReplicationSlot(replicationSlot)
  err := r.CreateLogicalReplicationSlot(replicationSlot, "test_decoding")
  if err != nil {
    t.Fatal(err)
  }
}

func dropTestReplicationSlot(t *testing.T, r *ReplicationConn) {
  err := r.DropReplicationSlot(replicationSlot)
  if err != nil {
    t.Fatal(err)
  }
}

func expectValidPositionFormat(t *testing.T, p string) {
  m, _ := regexp.MatchString("^[0-9A-F]{1,8}/[0-9A-F]{1,8}$", p)
  if !m {
    t.Fatalf("Expected valid log position string; got %v", p)
  }
} 

func expectConnClosedError(t *testing.T, err error) {
  if err != errReplicationConnClosed {
    t.Fatalf("expected errReplicationConnClosed; got %v", err)
  }
}

func expectCloseWithoutError(t *testing.T, r *ReplicationConn) {
  if err := r.Close(); err != nil {
    t.Fatal(err)
  }
}

func expectValidStatusUpdateSend(t *testing.T, r *ReplicationConn, xLogPos string, reply bool) {
  xLogPosInt := XLogPosStrToInt(xLogPos)
  msg := NewStatusUpdateMsg(xLogPosInt, xLogPosInt, xLogPosInt, GetCurrentTimestamp(), reply)
  err := r.SendMessage(msg)
  if err != nil {
    t.Fatal(err)
  }
}

func expectXLogDataMsgReceived(t *testing.T, r *ReplicationConn) *XLogDataMsg {
  received := make(chan *XLogDataMsg)
  errorChan := make(chan error)
  timeout := make(chan bool)
  go func(){
    time.Sleep(1 * time.Second)
    timeout <-true
  }()
  go func(){
    for {
      msg, err := r.RecvMessage()
      if err != nil {
        errorChan <-err
        return
      } else if msg.Type == MSG_X_LOG_DATA  {
        received <-msg.Data.(*XLogDataMsg)
        return
      }
    }
  }()
  select {
  case msg := <-received:
    return msg
  case err := <-errorChan:
    t.Fatal(err)
  case <-timeout:
    t.Fatal("Expected XLogData message received")
  }
  return nil
}

func startReplication(t *testing.T, r *ReplicationConn) string {
  systemInfo, err := r.IdentifySystem()
  if err != nil {
    t.Fatal(err)
  }

  err = r.StartReplication(replicationSlot, systemInfo.XLogPos)
  if err != nil {
    t.Fatal(err)
  }

  return systemInfo.XLogPos
}

func TestNewReplicationConn(t *testing.T) {
  r := newTestReplicationConn(t)
  defer r.Close()
}

func TestReplicationConnClose(t *testing.T) {
  r := newTestReplicationConn(t)
  defer r.Close()

  expectCloseWithoutError(t, r)

  err := r.Close()
  expectConnClosedError(t, err)
}


func TestIdentifySystem(t *testing.T) {
  r := newTestReplicationConn(t)
  defer r.Close()

  systemInfo, err := r.IdentifySystem()
  if err != nil {
    t.Fatal(err)
  }

  expectValidPositionFormat(t, systemInfo.XLogPos)

  expectCloseWithoutError(t, r)

  _, err = r.IdentifySystem()
  expectConnClosedError(t, err)
}

func TestCreateReplicationSlot(t *testing.T) {
  r := newTestReplicationConn(t)
  defer r.Close()

  createTestReplicationSlot(t, r)
  defer dropTestReplicationSlot(t, r)
}

func TestStartReplication(t *testing.T) {
  r := newTestReplicationConn(t)
  defer r.Close()

  createTestReplicationSlot(t, r)
  defer dropTestReplicationSlot(t, r)

  systemInfo, err := r.IdentifySystem()
  if err != nil {
    t.Fatal(err)
  }

  err = r.StartReplication(replicationSlot, systemInfo.XLogPos)
  if err != nil {
    t.Fatal(err)
  }
  defer r.StopReplication()

  err = r.StartReplication(replicationSlot, systemInfo.XLogPos)
  if err != errReplicationConnAlreadyReplication {
    t.Fatalf("expected errReplicationConnAlreadyReplication; got %v", err)
  }

  err = r.StopReplication()
  if err != nil {
    t.Fatal(err)
  }

  err = r.StopReplication()
  if err != errReplicationConnNotReplicating {
    t.Fatalf("expected errReplicationConnNotReplicating; got %v", err)
  }
}

func TestXLogPosConversions(t *testing.T) {
  conversions := make(map[string]int64)
  conversions["0/243C4C60"] = int64(607931488)
  conversions["A1/243C4C60"] = int64(692097666144)
  
  for k, v := range conversions {
    rv := XLogPosStrToInt(k)
    if rv != v {
      t.Fatalf("expected XLogPosStrToInt(%s) to equal %d; got %v", k, v, rv)
    }

    rk := XLogPosIntToStr(v)
    if rk != k {
      t.Fatalf("expected XLogPosIntToStr(%d) to equal %s; got %v", v, k, rk)
    }
  }
}

func TestReplicationMessages(t *testing.T) {
  db := openTestConnWithTable(t)
  defer closeTestConnWithTable(t, db)

  r := newTestReplicationConn(t)
  defer r.Close()

  createTestReplicationSlot(t, r)
  defer dropTestReplicationSlot(t, r)

  xLogPos := startReplication(t, r)
  defer r.StopReplication()

  expectValidStatusUpdateSend(t, r, xLogPos, false)

  db.Exec("INSERT INTO repl VALUES (1)")

  msg := expectXLogDataMsgReceived(t, r)

  if msg.CurrentXLogPos <= XLogPosStrToInt(xLogPos) {
    t.Fatal("Non linear position received")
  }
}
