package sources

type Source interface {
	CleanUp()
	Read() []string
	Type() string
}
