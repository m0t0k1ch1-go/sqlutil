package sqlutil_test

import (
	"database/sql"
	"database/sql/driver"
	"fmt"
	"net/url"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/m0t0k1ch1-go/sqlutil/v3"
)

func TestHTTPURL(t *testing.T) {
	var hu sqlutil.HTTPURL
	require.Implements(t, (*fmt.Stringer)(nil), &hu)
	require.Implements(t, (*driver.Valuer)(nil), &hu)
	require.Implements(t, (*sql.Scanner)(nil), &hu)
}

func TestNewHTTPURL(t *testing.T) {
	t.Run("failure", func(t *testing.T) {
		tcs := []struct {
			name string
			in   *url.URL
			want string
		}{
			{
				"nil",
				nil,
				"invalid url.URL: nil",
			},
			{
				"invalid host: empty",
				&url.URL{},
				"invalid url.URL: invalid host: empty",
			},
			{
				"invalid scheme: ftp",
				&url.URL{
					Scheme: "ftp",
					Host:   "m0t0k1ch1.com",
				},
				"invalid url.URL: invalid scheme: must be http or https",
			},
		}

		for _, tc := range tcs {
			t.Run(tc.name, func(t *testing.T) {
				_, err := sqlutil.NewHTTPURL(tc.in)
				require.ErrorContains(t, err, tc.want)
			})
		}
	})

	t.Run("success", func(t *testing.T) {
		tcs := []struct {
			name string
			in   *url.URL
			want string
		}{
			{
				"http",
				&url.URL{
					Scheme: "http",
					Host:   "m0t0k1ch1.com",
				},
				"http://m0t0k1ch1.com",
			},
			{
				"https",
				&url.URL{
					Scheme: "https",
					Host:   "m0t0k1ch1.com",
				},
				"https://m0t0k1ch1.com",
			},
		}

		for _, tc := range tcs {
			t.Run(tc.name, func(t *testing.T) {
				hu, err := sqlutil.NewHTTPURL(tc.in)
				require.NoError(t, err)
				require.Equal(t, tc.want, hu.String())
			})
		}
	})

	t.Run("success: no aliasing", func(t *testing.T) {
		u := &url.URL{
			Scheme: "http",
			Host:   "m0t0k1ch1.com",
		}
		hu, err := sqlutil.NewHTTPURL(u)
		require.NoError(t, err)
		require.Equal(t, "http://m0t0k1ch1.com", hu.String())

		u.Scheme = "https"
		u.Host = "m0t0k1ch2.com"
		require.Equal(t, "https://m0t0k1ch2.com", u.String())

		require.Equal(t, "http://m0t0k1ch1.com", hu.String())
	})
}

func TestMustNewHTTPURL(t *testing.T) {
	t.Run("panic", func(t *testing.T) {
		tcs := []struct {
			name string
			in   *url.URL
			want string
		}{
			{
				"nil",
				nil,
				"invalid url.URL: nil",
			},
		}

		for _, tc := range tcs {
			t.Run(tc.name, func(t *testing.T) {
				require.PanicsWithError(t, tc.want, func() {
					sqlutil.MustNewHTTPURL(tc.in)
				})
			})
		}
	})

	t.Run("success", func(t *testing.T) {
		tcs := []struct {
			name string
			in   *url.URL
			want string
		}{
			{
				"http",
				&url.URL{
					Scheme: "http",
					Host:   "m0t0k1ch1.com",
				},
				"http://m0t0k1ch1.com",
			},
		}

		for _, tc := range tcs {
			t.Run(tc.name, func(t *testing.T) {
				hu := sqlutil.MustNewHTTPURL(tc.in)
				require.Equal(t, tc.want, hu.String())
			})
		}
	})
}

func TestNewHTTPURLFromString(t *testing.T) {
	t.Run("failure", func(t *testing.T) {
		tcs := []struct {
			name string
			in   string
			want string
		}{
			{
				"empty",
				"",
				"invalid URL string: empty",
			},
		}

		for _, tc := range tcs {
			t.Run(tc.name, func(t *testing.T) {
				_, err := sqlutil.NewHTTPURLFromString(tc.in)
				require.ErrorContains(t, err, tc.want)
			})
		}
	})

	t.Run("success", func(t *testing.T) {
		tcs := []struct {
			name string
			in   string
			want string
		}{
			{
				"http",
				"http://m0t0k1ch1.com",
				"http://m0t0k1ch1.com",
			},
			{
				"https",
				"https://m0t0k1ch1.com",
				"https://m0t0k1ch1.com",
			},
		}

		for _, tc := range tcs {
			t.Run(tc.name, func(t *testing.T) {
				hu, err := sqlutil.NewHTTPURLFromString(tc.in)
				require.NoError(t, err)
				require.Equal(t, tc.want, hu.String())
			})
		}
	})
}

func TestMustNewHTTPURLFromString(t *testing.T) {
	t.Run("panic", func(t *testing.T) {
		tcs := []struct {
			name string
			in   string
			want string
		}{
			{
				"empty",
				"",
				"invalid URL string: empty",
			},
		}

		for _, tc := range tcs {
			t.Run(tc.name, func(t *testing.T) {
				require.PanicsWithError(t, tc.want, func() {
					sqlutil.MustNewHTTPURLFromString(tc.in)
				})
			})
		}
	})

	t.Run("success", func(t *testing.T) {
		tcs := []struct {
			name string
			in   string
			want string
		}{
			{
				"http",
				"http://m0t0k1ch1.com",
				"http://m0t0k1ch1.com",
			},
		}

		for _, tc := range tcs {
			t.Run(tc.name, func(t *testing.T) {
				hu := sqlutil.MustNewHTTPURLFromString(tc.in)
				require.Equal(t, tc.want, hu.String())
			})
		}
	})
}

func TestHTTPURL_URL(t *testing.T) {
	t.Run("success", func(t *testing.T) {
		tcs := []struct {
			name string
			in   sqlutil.HTTPURL
			want string
		}{
			{
				"http",
				sqlutil.MustNewHTTPURLFromString("http://m0t0k1ch1.com"),
				"http://m0t0k1ch1.com",
			},
			{
				"https",
				sqlutil.MustNewHTTPURLFromString("https://m0t0k1ch1.com"),
				"https://m0t0k1ch1.com",
			},
		}

		for _, tc := range tcs {
			t.Run(tc.name, func(t *testing.T) {
				u := tc.in.URL()
				require.Equal(t, tc.want, u.String())
			})
		}
	})

	t.Run("success: no aliasing", func(t *testing.T) {
		hu := sqlutil.MustNewHTTPURLFromString("http://m0t0k1ch1.com")
		u := hu.URL()
		require.Equal(t, "http://m0t0k1ch1.com", u.String())

		u.Scheme = "https"
		u.Host = "m0t0k1ch2.com"
		require.Equal(t, "https://m0t0k1ch2.com", u.String())

		require.Equal(t, "http://m0t0k1ch1.com", hu.String())
	})
}

func TestHTTPURL_Value(t *testing.T) {
	t.Run("success", func(t *testing.T) {
		tcs := []struct {
			name string
			in   sqlutil.HTTPURL
			want string
		}{
			{
				"http",
				sqlutil.MustNewHTTPURLFromString("http://m0t0k1ch1.com"),
				"http://m0t0k1ch1.com",
			},
			{
				"https",
				sqlutil.MustNewHTTPURLFromString("https://m0t0k1ch1.com"),
				"https://m0t0k1ch1.com",
			},
		}

		for _, tc := range tcs {
			t.Run(tc.name, func(t *testing.T) {
				v, err := tc.in.Value()
				require.NoError(t, err)
				require.Equal(t, tc.want, v)
			})
		}
	})
}

func TestHTTPURL_Scan(t *testing.T) {
	t.Run("failure", func(t *testing.T) {
		tcs := []struct {
			name string
			in   any
			want string
		}{
			{
				"nil",
				nil,
				"invalid source: nil",
			},
			{
				"bool",
				true,
				"unsupported source type: bool",
			},
			{
				"string: empty",
				"",
				"invalid source: invalid URL string: empty",
			},
			{
				"string: invalid url.URL: invalid host: empty",
				"http://",
				"invalid source: invalid url.URL: invalid host: empty",
			},
			{
				"string: invalid url.URL: invalid scheme: ftp",
				"ftp://m0t0k1ch1.com",
				"invalid source: invalid url.URL: invalid scheme: must be http or https",
			},
		}

		for _, tc := range tcs {
			t.Run(tc.name, func(t *testing.T) {
				var hu sqlutil.HTTPURL
				err := hu.Scan(tc.in)
				require.EqualError(t, err, tc.want)
			})
		}
	})

	t.Run("success", func(t *testing.T) {
		tcs := []struct {
			name string
			in   any
			want string
		}{
			{
				"string: http",
				"http://m0t0k1ch1.com",
				"http://m0t0k1ch1.com",
			},
			{
				"[]byte: https",
				[]byte("https://m0t0k1ch1.com"),
				"https://m0t0k1ch1.com",
			},
		}

		for _, tc := range tcs {
			t.Run(tc.name, func(t *testing.T) {
				var hu sqlutil.HTTPURL
				err := hu.Scan(tc.in)
				require.NoError(t, err)
				require.Equal(t, tc.want, hu.String())
			})
		}
	})
}
