package main

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"log"
	"math/big"
	"time"

	"github.com/confluentinc/confluent-kafka-go/kafka"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/bson/primitive"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
	"go.mongodb.org/mongo-driver/mongo/readpref"
)

type Root struct {
	Client   *mongo.Client
	Products *mongo.Collection
	Consumer *kafka.Consumer
}

func InitRoot() *Root {
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	client, err := mongo.Connect(ctx,
		options.Client().ApplyURI("mongodb://localhost:27017"))
	if err != nil {
		log.Panic(err)
	}

	err = client.Ping(ctx, readpref.Primary())
	if err != nil {
		log.Panic(err)
	}

	database := client.Database("baseweb")
	products := database.Collection("product")

	consumer, err := kafka.NewConsumer(&kafka.ConfigMap{
		"bootstrap.servers":  "localhost:9092",
		"group.id":           "product-mongo",
		"auto.offset.reset":  "earliest",
		"enable.auto.commit": "false",
	})
	if err != nil {
		log.Panic(err)
	}
	err = consumer.SubscribeTopics([]string{"DBTestServer.public.product"}, nil)
	if err != nil {
		log.Panic(err)
	}

	return &Root{
		Client:   client,
		Products: products,
		Consumer: consumer,
	}
}

type KafkaWeight struct {
	Scale int    `json:"scale"`
	Value string `json:"value"`
}

type Product struct {
	Id          int64        `json:"id"`
	Name        string       `json:"name"`
	Weight      *KafkaWeight `json:"weight"`
	WeightUomId string       `json:"weight_uom_id"`
	UnitUomId   string       `json:"unit_uom_id"`
	Description string       `json:"description"`
	CreatedAt   time.Time    `json:"created_at"`
	UpdatedAt   time.Time    `json:"updated_at"`
}

type KafkaMessage struct {
	Before *Product `json:"before"`
	After  *Product `json:"after"`
}

type KafkaKey struct {
	Id int64 `json:"id"`
}

func (weight *KafkaWeight) ToDecimal() *primitive.Decimal128 {
	if weight == nil {
		return nil
	}

	bytes, err := base64.StdEncoding.DecodeString(weight.Value)
	if err != nil {
		log.Panic(err)
	}
	value := big.NewInt(0)
	value.SetBytes(bytes)

	result, ok := primitive.ParseDecimal128FromBigInt(value, -weight.Scale)
	if !ok {
		return nil
	}
	return &result
}

func (root *Root) DeleteProduct(productId int64) {
	ctx := context.Background()

	filter := bson.D{{Key: "_id", Value: productId}}
	_, err := root.Products.DeleteOne(ctx, filter)
	if err != nil {
		log.Panic(err)
	}
}

func (root *Root) SaveProduct(product *Product) {
	ctx := context.Background()

	opts := options.Update().SetUpsert(true)
	filter := bson.D{{Key: "_id", Value: product.Id}}
	update := bson.D{{
		Key: "$set",
		Value: bson.M{
			"name":          product.Name,
			"weight":        product.Weight.ToDecimal(),
			"weight_uom_id": product.WeightUomId,
			"unit_uom_id":   product.UnitUomId,
			"description":   product.Description,
			"created_at":    product.CreatedAt,
			"updated_at":    product.UpdatedAt,
		},
	}}

	_, err := root.Products.UpdateOne(ctx, filter, update, opts)
	if err != nil {
		log.Panic(err)
	}
}

func main() {
	root := InitRoot()

	for {
		msg, err := root.Consumer.ReadMessage(-1)
		if err != nil {
			log.Panic(err)
		}

		fmt.Println(msg.TopicPartition, string(msg.Key), string(msg.Value))

		if len(msg.Value) == 0 {
			kafkaKey := KafkaKey{}
			err = json.Unmarshal(msg.Key, &kafkaKey)
			if err != nil {
				log.Panic(err)
			}
			root.DeleteProduct(kafkaKey.Id)
		} else {
			kafkaMsg := KafkaMessage{}
			err = json.Unmarshal(msg.Value, &kafkaMsg)
			if err != nil {
				log.Panic(err)
			}

			if kafkaMsg.After == nil {
				root.DeleteProduct(kafkaMsg.Before.Id)
			} else {
				root.SaveProduct(kafkaMsg.After)
			}
		}

		_, err = root.Consumer.CommitMessage(msg)
		if err != nil {
			log.Panic(err)
		}
	}

}
