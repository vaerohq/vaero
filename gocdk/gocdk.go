package gocdk

import "fmt"

type ConnectorConfig struct {
	service string
	name string

	awsRegion string

	userName string
	password string

	token string

	streamName string
	arn 	   string
	shardID	   string
	kinesisAllShards bool
}

// initialize a new Connector Config
func NewConnectorConfig() *ConnectorConfig {
	return &ConnectorConfig{}
}

// set the service
func (cc *ConnectorConfig) Service(service string) *ConnectorConfig {
	cc.service = service
	return cc
}

// set the name
func (cc *ConnectorConfig) Name(name string) *ConnectorConfig {
	cc.name = name
	return cc
}

// set the aws region
func (cc *ConnectorConfig) AWSRegion(awsRegion string) *ConnectorConfig {
	cc.awsRegion = awsRegion
	return cc
}

// set an ARN
func (cc *ConnectorConfig) ARN(arn string) *ConnectorConfig {
	cc.arn = arn
	return cc
}

// set a shard id
func (cc *ConnectorConfig) ShardID(shardID string) *ConnectorConfig {
	cc.shardID = shardID
	return cc
}

// set all shards
func (cc *ConnectorConfig) KinesisAllShards(value bool) *ConnectorConfig {
	cc.kinesisAllShards = value
	return cc
}

// set the username
func (cc *ConnectorConfig) UserName(userName string) *ConnectorConfig {
	cc.userName = userName
	return cc
}

// set the password
func (cc *ConnectorConfig) Password(password string) *ConnectorConfig {
	cc.password = password
	return cc
}

// set the token
func (cc *ConnectorConfig) Token(token string) *ConnectorConfig {
	cc.token = token
	return cc
}

// get the service
func (cc *ConnectorConfig) GetService() string {
	return cc.service
}

// get a public dict (essentially)
func (cc *ConnectorConfig) GetPublic() *ConnectorConfigPublic {
	return &ConnectorConfigPublic{
		Service: cc.service,
		Name: cc.name,
		AWSRegion: cc.awsRegion,
		ARN: cc.arn,
		ShardID: cc.shardID,
		KinesisAllShards: cc.kinesisAllShards,
		UserName: cc.userName,
		Password: cc.password,
		Token: cc.token,

	}
}


type ConnectorConfigPublic struct {
	Service    string
	Name	   string

	AWSRegion string

	ARN 	   string
	ShardID	   string
	KinesisAllShards bool

	UserName string
	Password   string	
	Token string
}

// 
type ConnectorCursor interface {
	CheckIfSavedCursorExists() bool
	Serialize()
	Deserialize()

	Set(key string, value any)
	Get(key string) any
	GetString(key string) string
	GetInt(key string) int
	GetUint(key string) int
 	GetBool(key string) bool
}

type HTTPConnector struct {
	config *ConnectorConfig
	service Service
}

// initialize a new Connector
func NewHTTPConnector() *HTTPConnector {
	return &HTTPConnector{}
}


// set the config
func (h *HTTPConnector) Config(config *ConnectorConfig) *HTTPConnector {
	h.config = config

	// instantiate the connector kind here
	fmt.Println(h.config.GetService())
	switch h.config.GetService() {
		case "kinesis":
			h.service = new(KinesisService)

		case "github":
			fmt.Println("github")
		case "":
			fmt.Println("ERROR")
	}
	return h
}

func(h *HTTPConnector) Earl() *KinesisService {
	return h.service.(*KinesisService)
}

func (h* HTTPConnector) Connect() bool {
	return h.service.CheckValidConfig(h.config)	&& h.service.Authorize()
}






type Record struct {
	payload string
}

type StreamSink interface {
	// write a record to the stream
	Write(record Record)
}

// a sink that writes to stdout
type StdOutSink struct {
}
func NewStdOutSink() *StdOutSink {
	return &StdOutSink{}
}
func (sos *StdOutSink) Write(record Record) {
	fmt.Println(record)
}


 type Service interface {
	 CheckValidConfig(cc *ConnectorConfig) bool
	 Authorize() bool
	 LoadCursor(forceRestart bool)
	 SaveCursor()
	 ReadStream(sink StreamSink)
 }