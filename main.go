package main

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
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
}
