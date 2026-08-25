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
	EventType          string // "WALLET_CREDITED" | "WALLET_DEBITED"
	TxnID              string
	WalletID           string // partition key
	PhoneNumber        string
	NationalID         string // for the future will wire the user and their wallets
	Amount             int64  // amount of the txn
	BalanceAfter       int64
	OccurredAt         time.Time // business time when the money moved
	CoupledPhoneNumber string    // when the transaction is transfer will be put with the sender						// must be checked first in the notifications service
}

