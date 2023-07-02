package recovery

import (
	"bufio"
	"os"
	"strconv"
	"strings"
	"tp1/common/message"
)

const (
	beginTag  = "BEGIN"
	commitTag = "COMMIT"
	abortTag  = "ABORT"
	writeTag  = "W"
)

type StorageManager struct {
	files map[string]*os.File
	dir   string
}

func NewStorageManager(dir string) (*StorageManager, error) {
	files, err := openFiles(dir, os.O_APPEND|os.O_CREATE|os.O_WRONLY)
	if err != nil {
		return nil, err
	}
	return &StorageManager{
		files: files,
		dir:   dir,
	}, nil
}

func (m *StorageManager) Store(msg message.Message, shouldStoreData bool) error {
	var err error
	var transaction string
	instanceID := strconv.Itoa(msg.InstanceID)
	transaction += "BEGIN " + msg.MsgType + "," + msg.ID + "," + instanceID + "," + msg.City + "\n"
	if !msg.IsEOF() && shouldStoreData {
		for _, line := range msg.Batch {
			transaction += "W " + line + "\n"
		}
	}
	transaction += "COMMIT\n"

	fileName := msg.ClientID
	f, ok := m.files[msg.ClientID]
	if !ok {

		f, err = os.OpenFile("recovery_files/"+fileName, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0666)
		if err != nil {
			return err
		}
		m.files[fileName] = f
	}
	_, err = f.Write([]byte(transaction))
	if err != nil {
		return err
	}
	return f.Sync()
}

func (m *StorageManager) Close() error {
	for _, f := range m.files {
		err := f.Close()
		if err != nil {
			return err
		}
	}
	return nil
}

func (m *StorageManager) Delete(clientID string) error {
	if clientID == message.AllClients {
		for _, f := range m.files {
			err := f.Close()
			if err != nil {
				return err
			}
		}
		_ = os.RemoveAll(m.dir)
		m.files = make(map[string]*os.File)
	} else {
		f := m.files[clientID]
		err := f.Close()
		if err != nil {
			return err
		}
		delete(m.files, clientID)
		_ = os.Remove(m.dir + "/" + clientID)
	}
	return nil
}

func Recover(dir string, callback func(msg message.Message) error) error {
	files, err := openFiles(dir, os.O_RDWR)
	if err != nil {
		return err
	}
	for name, f := range files {
		err = recoverFile(name, f, callback)
		if err != nil {
			return err
		}
	}
	for _, f := range files {
		err = f.Close()
		if err != nil {
			return err
		}
	}
	return nil
}

func recoverFile(name string, f *os.File, callback func(msg message.Message) error) error {
	var begin bool
	var invalidState bool
	var currentLines []string

	scanner := bufio.NewScanner(f)
	for {
		if scanner.Scan() {
			line := scanner.Text()
			tag := strings.Split(line, " ")[0]
			switch tag {
			case beginTag:
				if begin {
					invalidState = true
					break
				}
				currentLines = append(currentLines, line)
				begin = true
			case writeTag:
				if !begin {
					invalidState = true
					break
				}
				currentLines = append(currentLines, line)
			case commitTag:
				if !begin {
					invalidState = true
					break
				}
				msg := parseCurrentLines(currentLines, name)
				if err := callback(msg); err != nil {
					return err
				}
				currentLines = []string{}
				begin = false
			case abortTag:
				currentLines = []string{}
				begin = false
				invalidState = false
			default:
				invalidState = true
			}
		} else {
			break
		}
	}
	if invalidState || begin {
		_, err := f.Write([]byte("\nABORT\n"))
		if err != nil {
			return err
		}
		err = f.Sync()
		if err != nil {
			return err
		}
	}
	return nil
}

func parseCurrentLines(lines []string, fileName string) message.Message {
	msgType, msgID, instanceIDString, city := parseBeginLine(lines[0])

	var msgLines []string
	for _, line := range lines[1:] {
		msgLine := parseWriteLine(line)
		msgLines = append(msgLines, msgLine)
	}

	instanceID, _ := strconv.Atoi(instanceIDString)
	return message.Message{
		MsgType:    msgType,
		ID:         msgID,
		ClientID:   fileName,
		City:       city,
		Batch:      msgLines,
		InstanceID: instanceID,
	}
}

func parseBeginLine(line string) (msgType string, msgID string, instanceID string, city string) {
	tagAndData := strings.Split(line, " ")
	dataFields := strings.Split(tagAndData[1], ",")
	msgType = dataFields[0]
	msgID = dataFields[1]
	instanceID = dataFields[2]
	city = dataFields[3]
	return
}

func parseWriteLine(line string) string {
	return line[2:]
}

func openFiles(dir string, flag int) (map[string]*os.File, error) {
	files := make(map[string]*os.File)
	entries, err := os.ReadDir(dir)
	if err != nil {
	}
	for _, entry := range entries {
		if !entry.IsDir() {
			name := entry.Name()
			f, err := os.OpenFile(dir+"/"+name, flag, 0666)
			if err != nil {
				return nil, err
			}
			files[name] = f
		}
	}
	return files, nil
}
