package notifications

import "time"

type CreatePushNotificationRequest struct {
	DeviceToken   string // the phonenumber for now (put in the grpc server)
	Title         string // put in the service layer
	Message       string
	EventType     string
	Amount        int64
	Balance       int64
	TransactionID string
	WalletID      string
	NationalID     string
	CreatedAt     time.Time
}

type CreateSMSNotificationRequest struct {
	PhoneNumber   string
	Message       string
	EventType     string
	Amount        int64
	Balance       int64
	TransactionID string
	WalletID      string
	NationalID     string
	CreatedAt     time.Time
}

type CreatePushNotificationResponse struct {
	Success        bool
	NotificationID string
}

type CreateSMSNotificationResponse struct {
	Success        bool
	NotificationID string
}


// kafka dtos
const (
	EventWalletCredited = "WALLET_CREDITED"
	EventWalletDebited  = "WALLET_DEBITED"
)


// OBSERVED wire shape on transactions_db.transactions (verified via console-consumer):
// {
//   "_id": "{\"$oid\": \"6a96ed4026e9db4bb4a3703b\"}",   <- STRING containing extended JSON
//   "phone_number": "01511111111",
//   "sender_phone": "",
//   "receiver_phone": "",
//   "type": "DEPOSIT",
//   "amount": 50,
//   "wallet_id": "6a96e647c3449c68b46c7a32",
//   "balance_after": 300,
//   "created_at": 1788276032891                          <- plain epoch-milliseconds
// }
// ($project strips: reference_id, balance_before, status, sequence_number)
type TransactionEvent struct {
	EventType           string  `json:"type"`  // raw vocab: DEPOSIT | WITHDRAWAL | TRANSFER (legs derived in render)
	ID                  string  `json:"_id"`   // string containing {"$oid": "..."} — normalized in the consumer
	SenderPhoneNumber   string  `json:"sender_phone"`
	ReceiverPhoneNumber string  `json:"receiver_phone"`
	WalletID            string  `json:"wallet_id"` // partition key
	PhoneNumber         string  `json:"phone_number"`
	NationalID          string  `json:"national_id"` // for the future will wire the user and their wallets
	Amount              int64   `json:"amount"`
	BalanceAfter        int64   `json:"balance_after"`
	OccurredAt          int64   `json:"created_at"` // business time when the money moved (epoch millis)
}

