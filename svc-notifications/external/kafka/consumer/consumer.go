package consumer

import (
    "context"
    "encoding/json"
    "time"
	"errors"

    "github.com/IBM/sarama"

    "svc-notifications/internal/notifications"
    "svc-notifications/util/logger"
)

const (
    maxRetries   = 10               // bounded-retry 
    retryBackoff = 500 * time.Millisecond // backoff between retries
)

type Consumer struct {
    group     sarama.ConsumerGroup
    topic     string
    processor notifications.TransactionEventProcessor
}

func NewConsumer(ctx context.Context, brokers []string, groupID, topic string,
	p notifications.TransactionEventProcessor) (*Consumer, error) {
	log := logger.Ctx(ctx)
	cfg := sarama.NewConfig()
	cfg.Version = sarama.V3_5_0_0
	cfg.Consumer.Offsets.Initial = sarama.OffsetOldest // start from the earliest offset when goingup
	group, err := sarama.NewConsumerGroup(brokers, groupID, cfg)
	if err != nil {
		log.Error().Err(err).Msg("Failed to create Kafka consumer group")
		return nil, err
	}
	return &Consumer{
		group: group,
		topic: topic,
		processor: p,
	}, nil
}

func (c *Consumer) Run(ctx context.Context) {
    log := logger.Ctx(ctx)
    for {
        err := c.group.Consume(ctx, []string{c.topic}, &handler{processor: c.processor})
        if err != nil {
            if errors.Is(err, sarama.ErrClosedConsumerGroup) || ctx.Err() != nil {
                return    // return when the group is closed or the context is canceled
            }
            log.Error().Err(err).Msg("consume cycle failed, retrying")
            time.Sleep(time.Second)         // broker blips, rebalance hiccups
        }
        if ctx.Err() != nil {
            return
        }
    }
}
func (c *Consumer) Close() error { return c.group.Close() }

type handler struct{ processor notifications.TransactionEventProcessor }

var _ sarama.ConsumerGroupHandler = (*handler)(nil)

func (h *handler) Setup(sarama.ConsumerGroupSession) error   { return nil }
func (h *handler) Cleanup(sarama.ConsumerGroupSession) error { return nil }

func (h *handler) ConsumeClaim(s sarama.ConsumerGroupSession, claim sarama.ConsumerGroupClaim) error {
	log := logger.Ctx(s.Context())
	for msg := range claim.Messages() {
		var evt notifications.TransactionEvent
		if err := json.Unmarshal(msg.Value, &evt); err != nil {
			log.Error().Err(err).Msgf("Failed to unmarshal transaction event: topic=%s partition=%d offset=%d", msg.Topic, msg.Partition, msg.Offset)
			s.MarkMessage(msg,"") // mark the message as processed to avoid reprocessing
			continue
		}

		// normalize _id: the connector delivers it as a STRING containing {"$oid": "..."}
		var oid struct {
			OID string `json:"$oid"`
		}
		if err := json.Unmarshal([]byte(evt.ID), &oid); err == nil && oid.OID != "" {
			evt.ID = oid.OID
		}

		// tripwire: an empty txn id would poison the dedup index (every event
		// colliding on transaction_id="") — skip loudly instead of silently
		if evt.ID == "" {
			log.Error().Msgf("Transaction event with empty _id, skipping: topic=%s partition=%d offset=%d payload=%s", msg.Topic, msg.Partition, msg.Offset, string(msg.Value))
			s.MarkMessage(msg, "")
			continue
		}

		var err error
		for i := 0; i < maxRetries; i++ {
			err = h.processor.ProcessTransactionEvent(s.Context(), evt) // the process is idempotent, so we can retry safely
			if err == nil { // the process done
				break
			}
			time.Sleep(retryBackoff)
		}
		// log the error and its data as i will skip the msg and commit the offset
		if err != nil {
			log.Error().Err(err).Msgf("Failed to process transaction event after %d retries: topic=%s partition=%d offset=%d payload=%s", maxRetries, msg.Topic, msg.Partition, msg.Offset, string(msg.Value))
		}
		s.MarkMessage(msg,"") // commits after process (at least once delivery)
	}
	return nil
}