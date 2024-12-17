package httpclient

import "net/http"
import "net/url"

func GetV1InternalFiles(baseUrl string, uuid string) (*http.Response, error) {
	uri, err := url.JoinPath(baseUrl, "/api/v1/internal/files", uuid)
	if err != nil {
		return nil, err
	}
	return http.Get(uri)
}
