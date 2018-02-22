// Copyright 2014 Google Inc. All Rights Reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

// Package requester provides commands to run load tests and display results.
package requester

import (
	"bytes"
	"crypto/tls"
	"io"
	"io/ioutil"
	"net/http"
	"net/http/httptrace"
	"net/url"
	"os"
	"sync"
	"time"
	"math/rand"
	"strconv"

	"golang.org/x/net/http2"
)

const seed = 0

func init() {
	rand.Seed(seed)
}
var rgen = rand.New(rand.NewSource(seed))

const baseUrl = "/api/service"
// Max size of the buffer of result channel.
const maxResult = 1000000
const maxIdleConn = 500

type result struct {
	err           error
	statusCode    int
	duration      time.Duration
	connDuration  time.Duration // connection setup(DNS lookup + Dial up) duration
	dnsDuration   time.Duration // dns lookup duration
	reqDuration   time.Duration // request "write" duration
	resDuration   time.Duration // response "read" duration
	delayDuration time.Duration // delay between response and request
	contentLength int64
}

type Config struct {
	Host []string
	Path string
	User int
	Service int
	Rate [][]string
}

type conf struct {
	num int
	snum uint64
	uid string
	rate float64
	dist string
}

type Work struct {
	Config Config
	// Rate the request per sec
	ServiceNum int

	// Request is the request to be made.
	Request []*http.Request

	RequestBody []byte

	// N is the total number of requests to make.
	N int

	// C is the concurrency level, the number of concurrent workers to run.
	C int

	// H2 is an option to make HTTP/2 requests
	H2 bool

	// Timeout in seconds.
	Timeout int

	// Qps is the rate limit in queries per second.
	QPS float64

	// DisableCompression is an option to disable compression in response
	DisableCompression bool

	// DisableKeepAlives is an option to prevents re-use of TCP connections between different HTTP requests
	DisableKeepAlives bool

	// DisableRedirects is an option to prevent the following of HTTP redirects
	DisableRedirects bool

	// Output represents the output type. If "csv" is provided, the
	// output will be dumped as a csv stream.
	Output string

	// ProxyAddr is the address of HTTP proxy server in the format on "host:port".
	// Optional.
	ProxyAddr *url.URL

	// Writer is where results will be written. If nil, results are written to stdout.
	Writer io.Writer

	results chan *result
	stopCh  chan struct{}
	start   time.Time

	report *report
}

func (b *Work) writer() io.Writer {
	if b.Writer == nil {
		return os.Stdout
	}
	return b.Writer
}

// Run makes all the requests, prints the summary. It blocks until
// all work is done.
func (b *Work) Run() {
	b.results = make(chan *result, min(b.C*1000, maxResult))
	b.stopCh = make(chan struct{}, b.C)
	b.start = time.Now()
	b.report = newReport(b.writer(), b.results, b.Output, b.N * b.C)
	// Run the reporter first, it polls the result channel until it is closed.
	go func() {
		runReporter(b.report)
	}()
	b.runWorkers()
	b.Finish()
}

func (b *Work) Stop() {
	// Send stop signal so that workers can stop gracefully.
	for i := 0; i < b.C; i++ {
		b.stopCh <- struct{}{}
	}
}

func (b *Work) Finish() {
	close(b.results)
	total := time.Now().Sub(b.start)
	// Wait until the reporter is done.
	<-b.report.done
	b.report.finalize(total)
}

func (b *Work) makeRequest(c *http.Client, uid string, sname uint64) {
	s := time.Now()
	var size int64
	var code int
	var dnsStart, connStart, resStart, reqStart, delayStart time.Time
	var dnsDuration, connDuration, resDuration, reqDuration, delayDuration time.Duration
	//FIXME hack
	req := cloneRequest(b.Request[rand.Intn(len(b.Config.Host))], b.RequestBody)
	req.URL.Path = b.Config.Path + strconv.FormatUint(sname, 10)
	req.URL.RawQuery = "uid=u" + uid

	trace := &httptrace.ClientTrace{
		DNSStart: func(info httptrace.DNSStartInfo) {
			dnsStart = time.Now()
		},
		DNSDone: func(dnsInfo httptrace.DNSDoneInfo) {
			dnsDuration = time.Now().Sub(dnsStart)
		},
		GetConn: func(h string) {
			connStart = time.Now()
		},
		GotConn: func(connInfo httptrace.GotConnInfo) {
			if !connInfo.Reused {
				connDuration = time.Now().Sub(connStart)
			}
			reqStart = time.Now()
		},
		WroteRequest: func(w httptrace.WroteRequestInfo) {
			reqDuration = time.Now().Sub(reqStart)
			delayStart = time.Now()
		},
		GotFirstResponseByte: func() {
			delayDuration = time.Now().Sub(delayStart)
			resStart = time.Now()
		},
	}
	req = req.WithContext(httptrace.WithClientTrace(req.Context(), trace))
	resp, err := c.Do(req)
	if err == nil {
		size = resp.ContentLength
		code = resp.StatusCode
		io.Copy(ioutil.Discard, resp.Body)
		resp.Body.Close()
	}
	t := time.Now()
	resDuration = t.Sub(resStart)
	finish := t.Sub(s)
	b.results <- &result{
		statusCode:    code,
		duration:      finish,
		err:           err,
		contentLength: size,
		connDuration:  connDuration,
		dnsDuration:   dnsDuration,
		reqDuration:   reqDuration,
		resDuration:   resDuration,
		delayDuration: delayDuration,
	}
}

func (b *Work) runWorker(client *http.Client, c conf) {
	if b.DisableRedirects {
		client.CheckRedirect = func(req *http.Request, via []*http.Request) error {
			return http.ErrUseLastResponse
		}
	}

	// var throttle <-chan time.Time
	// if b.QPS > 0 {
	// 	throttle = time.Tick(time.Duration(1e6/(b.QPS)) * time.Microsecond)
	// }

	// for i := 0; i < n; i++ {
	// 	// Check if application is stopped. Do not send into a closed channel.
	// 	select {
	// 	case <-b.stopCh:
	// 		return
	// 	default:
	// 		if b.QPS > 0 {
	// 			<-throttle
	// 		}
	// 		b.makeRequest(client)
	// 	}
	// }

	// The request interval follows exponential distribution,
	// which would be a Poisson distribution
	var wg sync.WaitGroup
	wg.Add(c.num)

	// FIXME this doesn't accept alpha < 1.0
	zipf := rand.NewZipf(rgen, 1.001, 1.0, c.snum)
	t := &IntervalTimer{method: c.dist, rate: c.rate}

	for i := 0; i < c.num; i++ {
		select {
		// case <-time.After(time.Duration(rndWaitGen.Exp(c.rate)) * time.Second):
		case <-t.nextTick():
			go func() {
				b.makeRequest(client, c.uid, zipf.Uint64())
				wg.Done()
			}()
		case <-b.stopCh:
			return
		}
	}
	wg.Wait()
}


type IntervalTimer struct {
	method string
	rate float64
}

func (t *IntervalTimer) nextTick() <-chan time.Time {
	var d float64
	switch t.method {
	case "fixed":
		d = 1e6/t.rate
	case "uniform":
		d = 2e6 / t.rate * rand.Float64()
	case "poisson":
		d = 1e6 / t.rate * rand.ExpFloat64()
	default:
		panic("unknown method: " + t.method)
	}
	delay := time.Duration(d)* time.Microsecond
	return time.After(delay)
}

func (b *Work) runWorkers() {
	var wg sync.WaitGroup
	wg.Add(b.C)

	tr := &http.Transport{
		TLSClientConfig: &tls.Config{
			InsecureSkipVerify: true,
			//ServerName:         b.Request.Host,
		},
		MaxIdleConnsPerHost: min(b.C, maxIdleConn),
		DisableCompression:  b.DisableCompression,
		DisableKeepAlives:   b.DisableKeepAlives,
		Proxy:               http.ProxyURL(b.ProxyAddr),
	}
	if b.H2 {
		http2.ConfigureTransport(tr)
	} else {
		tr.TLSNextProto = make(map[string]func(string, *tls.Conn) http.RoundTripper)
	}
	client := &http.Client{Transport: tr, Timeout: time.Duration(b.Timeout) * time.Second}

	confs := make([]conf, b.C)
	for i := range confs {
		confs[i].uid = strconv.Itoa(i)
		confs[i].rate, _ = strconv.ParseFloat(b.Config.Rate[i][0], 64)
		confs[i].dist = b.Config.Rate[i][1]
		confs[i].snum = uint64(b.Config.Service)
		confs[i].num = b.N
	}

	for i := 0; i < b.C; i++ {
		go func(i int) {
			b.runWorker(client, confs[i])
			wg.Done()
		}(i)
	}
	wg.Wait()
}

// cloneRequest returns a clone of the provided *http.Request.
// The clone is a shallow copy of the struct and its Header map.
func cloneRequest(r *http.Request, body []byte) *http.Request {
	// shallow copy of the struct
	r2 := new(http.Request)
	*r2 = *r
	// deep copy of the Header
	r2.Header = make(http.Header, len(r.Header))
	for k, s := range r.Header {
		r2.Header[k] = append([]string(nil), s...)
	}
	if len(body) > 0 {
		r2.Body = ioutil.NopCloser(bytes.NewReader(body))
	}
	return r2
}

func min(a, b int) int {
	if a < b {
		return a
	}
	return b
}
