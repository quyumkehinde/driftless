//go:build !crashpoint

package crashpoint

// Maybe compiles to nothing outside crashpoint builds.
func Maybe(string) {}
