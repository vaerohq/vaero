package gocdk

import (
	"log"
	"os"
	"encoding/gob"
)


type LocalFileCursor struct {
	keys map[string]any
	filename string
}

func NewLocalFileCursor(filename string) *LocalFileCursor {
	return &LocalFileCursor{
		keys: make(map[string]any),
		filename: filename,
	}
}

// check if a saved cursor exists
func (ac *LocalFileCursor) CheckIfSavedCursorExists() bool {
	_, err := os.Stat("./" + ac.filename + ".gob")
	if err != nil && !os.IsNotExist(err) {
		log.Fatal("ERROR: stating " + ac.filename + ".gob: ", err)
	}

	if err == nil {
		return true
	}

	return false
}	

// set a key
func (ac *LocalFileCursor) Set(key string, value any) {
	ac.keys[key] = value
}

func (ac *LocalFileCursor) Get(key string) any {
	return ac.keys[key]
}

// get string
func (ac *LocalFileCursor) GetString(key string) string {
	return ac.keys[key].(string)
}

// get int
func (ac *LocalFileCursor) GetInt(key string) int {
	return ac.keys[key].(int)
}

// get unsigned int
func (ac *LocalFileCursor) GetUint(key string) uint {
	return ac.keys[key].(uint)
}

// get bool
func (ac *LocalFileCursor) GetBool(key string) bool {
	return ac.keys[key].(bool)
}

// serialize using gob
func (ac *LocalFileCursor) Serialize() {
	file, err := os.Create("./" + ac.filename + ".gob")
	if err != nil {
		log.Fatal(err)
	}

	encoder := gob.NewEncoder(file)
	if err = encoder.Encode(ac.keys); err != nil {
		log.Fatal(err)
	}
	
	file.Close()
}

// deserialize using gob
func (ac *LocalFileCursor) Deserialize() {
	file, err := os.Open("./" + ac.filename + ".gob")
	if err != nil {
		log.Fatal(err)
	}

	decoder := gob.NewDecoder(file)
	if err = decoder.Decode(&ac.keys); err != nil {
		log.Fatal(err)
	}

	file.Close()
}


// register a type for gob
func (ac *LocalFileCursor) RegisterType(t any) {
	gob.Register(t)
}