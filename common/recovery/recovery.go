package recovery

import (
	"bufio"
	"os"
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
}

func NewStorageManager(dir string) (*StorageManager, error) {
	files, err := openFiles(dir, os.O_APPEND|os.O_CREATE|os.O_WRONLY)
	if err != nil {
		return nil, err
	}
	return &StorageManager{
		files: files,
	}, nil
}

func (m *StorageManager) Store(msg message.Message) error {
	var err error
	var transaction string
	transaction += "BEGIN " + msg.MsgType + "," + msg.ID + "," + msg.City + "\n"
	if !msg.IsEOF() {
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

func (m *StorageManager) Close() {
	for _, f := range m.files {
		_ = f.Close()
	}
}

func Recover(dir string, callback func(msg message.Message)) error {
	files, err := openFiles(dir, os.O_RDWR)
	if err != nil {
		return err
	}
	for name, f := range files {
		recoverFile(name, f, callback)
	}
	for _, f := range files {
		_ = f.Close()
	}
	return nil
}

func recoverFile(name string, f *os.File, callback func(msg message.Message)) {
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
				callback(msg)
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
		f.Write([]byte("\nABORT\n"))
		f.Sync()
	}
}

func parseCurrentLines(lines []string, fileName string) message.Message {
	msgType, msgID, city := parseBeginLine(lines[0])

	var msgLines []string
	for _, line := range lines[1:] {
		msgLine := parseWriteLine(line)
		msgLines = append(msgLines, msgLine)
	}

	return message.Message{
		MsgType:  msgType,
		ID:       msgID,
		ClientID: fileName,
		City:     city,
		Batch:    msgLines,
	}
}

func parseBeginLine(line string) (msgType string, msgID string, city string) {
	tagAndData := strings.Split(line, " ")
	dataFields := strings.Split(tagAndData[1], ",")
	msgType = dataFields[0]
	msgID = dataFields[1]
	city = dataFields[2]
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
