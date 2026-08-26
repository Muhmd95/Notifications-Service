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
type TransactionEvent struct {
	EventType          string		`json:"event_type"`		 // "WALLET_CREDITED" | "WALLET_DEBITED"
	TxnID              string		`json:"txn_id"`			 // transaction id
	WalletID           string 		`json:"wallet_id"`			 // partition key
	PhoneNumber        string		`json:"phone_number"`
	NationalID         string 		`json:"national_id"`		// for the future will wire the user and their wallets
	Amount             int64  		`json:"amount"`				// amount of the txn
	BalanceAfter       int64  		`json:"balance_after"`
	OccurredAt         time.Time 	`json:"occurred_at"`		// business time when the money moved
	CoupledPhoneNumber string    	`json:"coupled_phone_number"` // when the transaction is transfer will be put with the sender						// must be checked first in the notifications service
}

