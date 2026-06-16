package sqlutil

import (
	"database/sql/driver"
	"errors"
	"fmt"
	"net/url"
)

// HTTPURL represents a HTTP(S) URL.
type HTTPURL struct {
	u url.URL
}

// NewHTTPURL returns a new HTTPURL.
func NewHTTPURL(u *url.URL) (HTTPURL, error) {
	var hu HTTPURL
	if err := hu.setURL(u); err != nil {
		return HTTPURL{}, err
	}

	return hu, nil
}

// MustNewHTTPURL panics if the input is invalid.
func MustNewHTTPURL(u *url.URL) HTTPURL {
	hu, err := NewHTTPURL(u)
	if err != nil {
		panic(err)
	}

	return hu
}

func (hu *HTTPURL) setURL(u *url.URL) error {
	if u == nil {
		return errors.New("invalid url.URL: nil")
	}
	if u.Host == "" {
		return errors.New("invalid url.URL: invalid host: empty")
	}
	if u.Scheme != "http" && u.Scheme != "https" {
		return errors.New("invalid url.URL: invalid scheme: must be http or https")
	}

	hu.u = *u

	return nil
}

// NewHTTPURLFromString returns a new HTTPURL from a string.
func NewHTTPURLFromString(s string) (HTTPURL, error) {
	var hu HTTPURL
	if err := hu.setString(s); err != nil {
		return HTTPURL{}, err
	}

	return hu, nil
}

// MustNewHTTPURLFromString panics if the input is invalid.
func MustNewHTTPURLFromString(s string) HTTPURL {
	hu, err := NewHTTPURLFromString(s)
	if err != nil {
		panic(err)
	}

	return hu
}

func (hu *HTTPURL) setString(s string) error {
	if len(s) == 0 {
		return errors.New("invalid URL string: empty")
	}

	u, err := url.Parse(s)
	if err != nil {
		return fmt.Errorf("invalid URL string: %w", err)
	}

	return hu.setURL(u)
}

// URL returns a copy of the underlying url.URL.
func (hu HTTPURL) URL() *url.URL {
	u := hu.u

	return &u
}

// String implements fmt.Stringer.
// It returns the value as a string.
func (hu HTTPURL) String() string {
	return hu.u.String()
}

// Value implements driver.Valuer.
// It returns the value as a string.
func (hu HTTPURL) Value() (driver.Value, error) {
	return hu.String(), nil
}

// Scan implements sql.Scanner.
// It accepts a string or []byte.
func (hu *HTTPURL) Scan(src any) error {
	if src == nil {
		return errors.New("invalid source: nil")
	}

	var s string
	{
		switch v := src.(type) {
		case string:
			s = v
		case []byte:
			s = string(v)
		default:
			return fmt.Errorf("unsupported source type: %T", src)
		}
	}

	if err := hu.setString(s); err != nil {
		return fmt.Errorf("invalid source: %w", err)
	}

	return nil
}
