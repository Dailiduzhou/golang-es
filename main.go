package main

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"log"
	"os"
	"time"

	"github.com/elastic/go-elasticsearch/v8"
	"github.com/elastic/go-elasticsearch/v8/esutil"
)

type Patent struct {
	PublicationTitle  string   `json:"publication_title"`
	PublicationNumber string   `json:"publication_number"`
	PublicationDate   string   `json:"publication_date"`
	GrantDate         *string  `json:"grant_date"`
	ApplicationNumber string   `json:"application_number"`
	ApplicationType   string   `json:"application_type"`
	ApplicationDate   string   `json:"application_date"`
	ApplicationStatus string   `json:"application_status"`
	Authors           []string `json:"authors"`
	Organizations     []string `json:"organizations"`
	CPCIPC            []string `json:"CPC/IPC"`
	Abstract          string   `json:"abstract"`
	Descriptions      string   `json:"descriptions"`
	Claims            string   `json:"claims"`
	AbstractZh        *string  `json:"abstract_zh"`
	DescriptionsZh    *string  `json:"descriptions_zh"`
	ClaimsZh          *string  `json:"claims_zh"`
}

func main() {
	es, err := elasticsearch.NewDefaultClient()
	if err != nil {
		log.Fatalf("创建 ES 客户端失败: %s", err)
	}

	bi, err := esutil.NewBulkIndexer(esutil.BulkIndexerConfig{
		Index:         "patents_index", // 指定要写入的 ES 索引名
		Client:        es,
		NumWorkers:    4,                // 并发协程数
		FlushBytes:    5e+6,             // 达到 5MB 时强制刷新写入
		FlushInterval: 30 * time.Second, // 或者每 30 秒刷新一次
	})
	if err != nil {
		log.Fatalf("创建 BulkIndexer 失败: %s", err)
	}

	file, err := os.Open("data/btd-5000.jsonl")
	if err != nil {
		log.Fatalf("打开文件失败: %s", err)
	}
	defer file.Close()

	scanner := bufio.NewScanner(file)

	const maxCapacity = 512 * 1024
	buf := make([]byte, maxCapacity)
	scanner.Buffer(buf, maxCapacity)

	count := 0
	for scanner.Scan() {

		lineBytes := scanner.Bytes()

		var patent Patent
		if err := json.Unmarshal(lineBytes, &patent); err != nil {
			log.Printf("解析 JSON 失败, 跳过此行: %s", err)
			continue
		}

		err = bi.Add(
			context.Background(),
			esutil.BulkIndexerItem{
				Action:     "index",
				DocumentID: patent.PublicationNumber,
				Body:       bytes.NewReader(lineBytes),

				OnFailure: func(ctx context.Context, item esutil.BulkIndexerItem, res esutil.BulkIndexerResponseItem, err error) {
					if err != nil {
						log.Printf("写入 ES 失败: %s", err)
					} else {
						log.Printf("写入 ES 失败 (类型: %s, 报错: %s)", res.Error.Type, res.Error.Reason)
					}
				},
			},
		)
		if err != nil {
			log.Fatalf("添加数据到 BulkIndexer 失败: %s", err)
		}
		count++
	}

	if err := scanner.Err(); err != nil {
		log.Fatalf("读取文件发生错误: %s", err)
	}

	if err := bi.Close(context.Background()); err != nil {
		log.Fatalf("关闭 BulkIndexer 发生错误: %s", err)
	}

	stats := bi.Stats()
	log.Printf("完成！成功解析并写入 %d 条数据。", count)
	log.Printf("Elasticsearch 新增: %d, 更新: %d, 失败: %d", stats.NumAdded, stats.NumUpdated, stats.NumFailed)

	SearchPatents(es, "patents_index", "scratch removal", "2009")

	SearchPatents(es, "patents_index", "NOVEL SCREENING METHOD", "")
	SearchPatents(es, "patents_index", "Semiconductor device manufacturing method and semiconductor device manufacturing system", "")
}

func SearchPatents(es *elasticsearch.Client, indexName, keyword, year string) {
	// 1. 动态构建 Bool 查询体
	boolQuery := map[string]interface{}{
		"must": []map[string]interface{}{
			{
				"multi_match": map[string]interface{}{
					"query": keyword,
					// 标题匹配的权重设置为摘要的 2 倍
					"fields": []string{"publication_title^2", "abstract"},
				},
			},
		},
	}

	// 2. 如果传入了年份，则动态追加 Filter 条件
	if year != "" {
		boolQuery["filter"] = []map[string]interface{}{
			{
				"prefix": map[string]interface{}{
					"publication_date": year, // 利用前缀匹配 YYYYMMDD
				},
			},
		}
	}

	// 组装最终的查询 Body
	queryBody := map[string]interface{}{
		"query": map[string]interface{}{
			"bool": boolQuery,
		},
		"size": 10, // 仅返回前 10 条结果
	}

	// 将查询结构体转为 JSON 字节流
	var buf bytes.Buffer
	if err := json.NewEncoder(&buf).Encode(queryBody); err != nil {
		log.Fatalf("解析查询条件失败: %s", err)
	}

	// 3. 发起 Search 请求
	res, err := es.Search(
		es.Search.WithContext(context.Background()),
		es.Search.WithIndex(indexName),
		es.Search.WithBody(&buf),
		es.Search.WithTrackTotalHits(true), // 返回真实的命中总数
	)
	if err != nil {
		log.Fatalf("搜索请求失败: %s", err)
	}
	defer res.Body.Close()

	if res.IsError() {
		log.Fatalf("Elasticsearch 返回错误: %s", res.String())
	}

	// 4. 解析搜索结果
	var result map[string]interface{}
	if err := json.NewDecoder(res.Body).Decode(&result); err != nil {
		log.Fatalf("解析响应结果失败: %s", err)
	}

	// 提取 hits (命中结果)
	hits := result["hits"].(map[string]interface{})
	total := int(hits["total"].(map[string]interface{})["value"].(float64))
	docs := hits["hits"].([]interface{})

	fmt.Printf("搜索关键字 [%s] (年份: %s), 找到 %d 条结果:\n", keyword, year, total)

	for i, hit := range docs {
		doc := hit.(map[string]interface{})
		source := doc["_source"].(map[string]interface{})
		score := doc["_score"].(float64)

		title := source["publication_title"].(string)
		pubDate := source["publication_date"].(string)

		fmt.Printf("%d. [得分: %.4f] [%s] %s\n", i+1, score, pubDate, title)

		// 打印截取的摘要
		abstract := source["abstract"].(string)
		if len(abstract) > 100 {
			fmt.Printf("   摘要: %s...\n", abstract[:100])
		} else {
			fmt.Printf("   摘要: %s\n", abstract)
		}
	}
}
