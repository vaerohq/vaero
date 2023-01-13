/*
Copyright © 2023 Vaero Inc. (https://www.vaero.co/)
*/
package sources

type Source interface {
	CleanUp()
	Read() []string
	Type() string
}
