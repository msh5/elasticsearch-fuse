package main

import (
	"context"
	"strings"

	elastic "gopkg.in/olivere/elastic.v5"
)

type ElasticsearchClient struct {
	raw *elastic.Client
}

func DeserializeDRLs(urls string) []string {
	return strings.Split(urls, ",")
}

func NewElasticsearchClient(urls []string) (*ElasticsearchClient, error) {
	raw, err := elastic.NewClient(elastic.SetURL(urls...))
	if err != nil {
		return nil, err
	}
	var c ElasticsearchClient
	c.raw = raw
	return &c, nil
}

func (c *ElasticsearchClient) GetIndexNames() ([]string, error) {
	return c.raw.IndexNames()
}

func (c *ElasticsearchClient) GetDocumentTypes(index string) ([]string, error) {
	mappings, err := c.raw.GetMapping().Do(context.Background())
	if err != nil {
		return nil, err
	}
	var dtypes []string
	for curIndex, curMappings := range mappings {
		if curIndex == index {
			mappingsByIndex := curMappings.(map[string]interface{})["mappings"].(map[string]interface{})
			for dtype := range mappingsByIndex {
				dtypes = append(dtypes, dtype)
			}
			break
		}
	}
	return dtypes, nil
}

func (c *ElasticsearchClient) CountDocuments(index string, dtype string) (int64, error) {
	result, err := c.raw.Search().Index(index).Type(dtype).Size(0).Do(context.Background())
	if err != nil {
		return 0, err
	}
	return result.Hits.TotalHits, nil
}

func (c *ElasticsearchClient) GetDocuments(index string, dtype string, from int, size int) (map[string][]byte, error) {
	docs := make(map[string][]byte)
	result, err := c.raw.Search().Index(index).Type(dtype).From(from).Size(size).Do(context.Background())
	if err != nil {
		return nil, err
	}
	for _, hit := range result.Hits.Hits {
		docSource, err := hit.Source.MarshalJSON()
		if err != nil {
			return nil, err
		}
		docs[hit.Id] = docSource
	}
	return docs, nil
}
