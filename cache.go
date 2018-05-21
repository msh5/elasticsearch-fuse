package main

type ElasticsearchCache struct {
	db         *ElasticsearchClient
	pageSize   int
	indexNames []string
	docTypes   map[string][]string
	docTotals  map[string]map[string]int64
	docs       map[string]map[string]map[int]map[string][]byte
}

func NewElasticsearchCache(urls string, pageSize int) (*ElasticsearchCache, error) {
	db, err := NewElasticsearchClient(DeserializeDRLs(urls))
	if err != nil {
		return nil, err
	}
	var c ElasticsearchCache
	c.db = db
	c.pageSize = pageSize
	return &c, nil
}

func (c *ElasticsearchCache) EnsureIndexNames() ([]string, error) {
	indexNames, err := c.db.GetIndexNames()
	if err != nil {
		return nil, err
	}
	c.indexNames = indexNames
	return indexNames, nil
}

func (c *ElasticsearchCache) EnsureDocumentTypes(index string) ([]string, error) {
	docTypes, err := c.db.GetDocumentTypes(index)
	if err != nil {
		return nil, err
	}
	if c.docTypes == nil {
		c.docTypes = make(map[string][]string)
	}
	c.docTypes[index] = docTypes
	return docTypes, nil
}

func (c *ElasticsearchCache) EnsureDocumentTotal(index string, docType string) (int64, error) {
	total, err := c.db.CountDocuments(index, docType)
	if err != nil {
		return 0, err
	}
	if c.docTotals == nil {
		c.docTotals = make(map[string]map[string]int64)
	}
	_, ok := c.docTotals[index]
	if !ok {
		c.docTotals[index] = make(map[string]int64)
	}
	c.docTotals[index][docType] = total
	return total, nil
}

func (c *ElasticsearchCache) EnsureDocuments(index string, docType string, page int) (map[string][]byte, error) {
	docs, err := c.db.GetDocuments(index, docType, c.pageSize*page, c.pageSize)
	if err != nil {
		return nil, err
	}
	if c.docs == nil {
		c.docs = make(map[string]map[string]map[int]map[string][]byte)
	}
	_, ok := c.docs[index]
	if !ok {
		c.docs[index] = make(map[string]map[int]map[string][]byte)
	}
	_, ok = c.docs[index][docType]
	if !ok {
		c.docs[index][docType] = make(map[int]map[string][]byte)
	}
	c.docs[index][docType][page] = docs
	return docs, nil
}
