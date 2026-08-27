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


// {
//   "_id": {"$oid": "67cf1a2b3c4d5e6f7a8b9c0d"},
//   "reference_id": "dep-123",
//   "phone_number": "01112223334",
//   "sender_phone": "",
//   "receiver_phone": "",
//   "type": "DEPOSIT",
//   "status": "POSTED",
//   "amount": 500,
//   "wallet_id": "67ae5f...",
//   "balance_before": 0,
//   "balance_after": 500,
//   "sequence_number": 1,
//   "created_at": {"$date": 1756300800000}
// }
type TransactionEvent struct {
	Type          string  			`json:"type"`		// "WALLET_CREDITED" | "WALLET_DEBITED" will need logic to be put
	ID                 struct {
		TxnID string `json:"$oid"`
	}		`json:"_id"`			 // transaction id
	SenderPhoneNumber   string		`json:"sender_phone"`	// when the transaction is transfer will be put with the sender	will need logic to be put
	ReceiverPhoneNumber string		`json:"receiver_phone"`	// when the transaction is transfer will be put with the receiver	will need logic to be put
	WalletID           string 		`json:"wallet_id"`			 // partition key
	PhoneNumber        string		`json:"phone_number"`
	NationalID         string 		`json:"national_id"`		// for the future will wire the user and their wallets
	Amount             int64  		`json:"amount"`				// amount of the txn
	BalanceAfter       int64  		`json:"balance_after"`
	OccurredAt         struct {
		Date int64 `json:"$date"` // may be string watch for testing
	} 	`json:"created_at"`		// business time when the money moved
	CoupledPhoneNumber string    	 // when the transaction is transfer will be put with the sender	will need logic to be put					
	// must be checked first in the notifications service
}

