package sources

type Source interface {
	Read() []string
}
