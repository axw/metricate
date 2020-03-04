package main

import (
	"bytes"
	"context"
	"crypto/tls"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"log"
	"net/http"
	"os"
	"strings"
	"text/template"
	"time"

	"github.com/codahale/hdrhistogram"
	"github.com/elastic/go-elasticsearch/v7"
	"github.com/elastic/go-elasticsearch/v7/esapi"
)

const (
	sigfigs     = 3 // number of significant figures (precision) for histogram
	maxDuration = 10 * time.Minute
)

var (
	size     = flag.Int("size", 10000, "Maximum number of docs")
	interval = flag.Duration("interval", 5*time.Minute, "Interval for metrics docs")

	since              = flag.String("since", "now-1d", "Lower-bound on @timestamp")
	sourceIndexPattern = flag.String("source", "apm-*-transaction*", "Transaction index pattern to query")
	targetIndex        = flag.String("target", "transaction_histograms", "Target index for transaction histogram metrics")

	insecure = flag.Bool("insecure", false, "Disable TLS certificate verification")
)

var bodyTemplate = template.Must(template.New("").Parse(`{
  "size": {{.Size}},
  "sort": [{"@timestamp": "asc"}],
  "query": {
    "range": {
      "@timestamp": {
        "gte": "{{js .Since}}"
      }
    }
  }
}`))

const metricsIndexDefinition = `{
  "mappings": {
    "properties": {
      "@timestamp": {"type": "date"},
      "service": {
        "properties": {
          "name": {"type": "keyword"}
        }
      },
      "transaction": {
        "properties": {
          "type": {"type": "keyword"},
          "name": {"type": "keyword"},
          "result": {"type": "keyword"},
          "duration": {
            "properties": {
              "histogram": {"type": "histogram"}
            }
          }
        }
      }
    }
  }
}`

type TransactionDocsResult struct {
	Hits struct {
		Hits []struct {
			Source TransactionDoc `json:"_source"`
		}
	}
	ScrollID string `json:"_scroll_id"`
}

type TransactionDoc struct {
	Timestamp   time.Time `json:"@timestamp"`
	Service     ServiceKey
	Transaction struct {
		TransactionKey
		Duration struct {
			Micros int64 `json:"us"`
		}
	}
}

type MetricDoc struct {
	Timestamp   time.Time  `json:"@timestamp"`
	Service     ServiceKey `json:"service"`
	Transaction struct {
		TransactionKey
		Duration struct {
			Histogram struct {
				Values []int64 `json:"values"`
				Counts []int64 `json:"counts"`
			} `json:"histogram"`
		} `json:"duration"`
	} `json:"transaction"`
}

type ServiceKey struct {
	Name string `json:"name"`
}

type TransactionKey struct {
	Name   string `json:"name"`
	Type   string `json:"type"`
	Result string `json:"result"`
}

type aggregator struct {
	buckets map[aggregationKey]*transactionMetrics
}

type aggregationKey struct {
	service     ServiceKey
	transaction TransactionKey
}

type transactionMetrics struct {
	start     time.Time
	hist      *hdrhistogram.Histogram
	snapshots []transactionMetricsSnapshot
}

func (m *transactionMetrics) snapshot() {
	snap := transactionMetricsSnapshot{
		timestamp: m.start,
	}
	for _, bar := range m.hist.Distribution() {
		snap.values = append(snap.values, bar.To)
		snap.counts = append(snap.counts, bar.Count)
	}
	m.snapshots = append(m.snapshots, snap)
}

type transactionMetricsSnapshot struct {
	timestamp time.Time
	values    []int64
	counts    []int64
}

func newAggregator() *aggregator {
	return &aggregator{buckets: make(map[aggregationKey]*transactionMetrics)}
}

func (a *aggregator) aggregate(doc TransactionDoc) {
	key := aggregationKey{
		service:     doc.Service,
		transaction: doc.Transaction.TransactionKey,
	}
	bucket, ok := a.buckets[key]
	if !ok {
		bucket = &transactionMetrics{
			start: doc.Timestamp,
			hist:  hdrhistogram.New(0, maxDuration.Microseconds(), sigfigs),
		}
		a.buckets[key] = bucket
	} else if doc.Timestamp.Sub(bucket.start) > *interval {
		bucket.snapshot()
		bucket.hist.Reset()
		bucket.start = doc.Timestamp
	}
	bucket.hist.RecordValue(doc.Transaction.Duration.Micros)
}

func (a *aggregator) emitDocs(es *elasticsearch.Client) error {
	response, err := es.Indices.Exists([]string{*targetIndex})
	if err != nil {
		return err
	}
	if response.StatusCode == http.StatusNotFound {
		response.Body.Close()
		response, err := esapi.IndicesCreateRequest{
			Index: *targetIndex,
			Body:  strings.NewReader(metricsIndexDefinition),
		}.Do(context.Background(), es)
		if err != nil {
			return err
		}
		if response.IsError() {
			io.Copy(os.Stderr, response.Body)
			response.Body.Close()
			return errors.New("creating index mapping failed")
		}
		response.Body.Close()
	} else if response.IsError() {
		io.Copy(os.Stderr, response.Body)
		response.Body.Close()
		return errors.New("creating index mapping failed")
	} else {
		response.Body.Close()
	}

	var buf bytes.Buffer
	doBulkRequest := func() error {
		if buf.Len() == 0 {
			return nil
		}
		response, err = esapi.BulkRequest{
			Index: *targetIndex,
			Body:  &buf,
		}.Do(context.Background(), es)
		if err != nil {
			return err
		}
		defer response.Body.Close()
		if response.IsError() {
			io.Copy(os.Stderr, response.Body)
			response.Body.Close()
			return errors.New("bulk indexing failed")
		}
		return nil
	}

	const limit = 512 * 1024 // 512KiB limit for bulk request body
	var ndocs int
	enc := json.NewEncoder(&buf)
	for key, bucket := range a.buckets {
		bucket.snapshot()
		for _, snap := range bucket.snapshots {
			fmt.Fprintf(&buf, `{"index": {}}`+"\n")
			doc := MetricDoc{
				Timestamp: snap.timestamp,
				Service:   key.service,
			}
			doc.Transaction.TransactionKey = key.transaction
			doc.Transaction.Duration.Histogram.Values = snap.values
			doc.Transaction.Duration.Histogram.Counts = snap.counts
			ndocs++
			if err := enc.Encode(&doc); err != nil {
				return err
			}
			if buf.Len() >= limit {
				if err := doBulkRequest(); err != nil {
					return err
				}
				buf.Reset()
			}
		}
	}
	if err := doBulkRequest(); err != nil {
		return err
	}
	log.Printf("Indexed %d metrics docs into %s", ndocs, *targetIndex)
	return nil
}

func Main() error {
	flag.Parse()
	if *insecure {
		httpTransport := http.DefaultTransport.(*http.Transport)
		httpTransport.TLSClientConfig = &tls.Config{InsecureSkipVerify: true}
	}

	es, err := elasticsearch.NewDefaultClient()
	if err != nil {
		return err
	}

	var templateData struct {
		Size  int
		Since string
	}
	templateData.Size = *size
	templateData.Since = *since

	var body bytes.Buffer
	if err := bodyTemplate.Execute(&body, &templateData); err != nil {
		return err
	}
	log.Printf("Executing querying: %s", body.String())

	agg := newAggregator()
	response, err := esapi.SearchRequest{
		Index:  []string{*sourceIndexPattern},
		Body:   &body,
		Scroll: time.Minute,
	}.Do(context.Background(), es)
	if err != nil {
		return err
	}
	for {
		if response.IsError() {
			io.Copy(os.Stderr, response.Body)
			response.Body.Close()
			return errors.New("search failed")
		}

		var result TransactionDocsResult
		if json.NewDecoder(response.Body).Decode(&result); err != nil {
			response.Body.Close()
			return err
		}
		response.Body.Close()

		for _, hit := range result.Hits.Hits {
			agg.aggregate(hit.Source)
		}

		if result.ScrollID == "" || len(result.Hits.Hits) < *size {
			// No more results.
			break
		}
		response, err = esapi.ScrollRequest{
			ScrollID: result.ScrollID,
			Scroll:   time.Minute,
		}.Do(context.Background(), es)
		if err != nil {
			return err
		}
	}
	return agg.emitDocs(es)
}

func main() {
	if err := Main(); err != nil {
		log.Fatal(err)
	}
}
