package notifications

import (
	"time"

	"go.mongodb.org/mongo-driver/bson/primitive"
)

type NotificationStatus string

const (
	StatusSuccessful 	NotificationStatus = "SUCCESSFUL"
	StatusPending 		NotificationStatus = "PENDING"
	StatusProcessing	NotificationStatus = "PROCESSING"
	StatusFailed    	NotificationStatus = "FAILED"
)


type PushNotification struct {
	ID primitive.ObjectID 		`bson:"_id,omitempty"`
	DeviceToken string			`bson:"device_token"` 
	Title string 				`bson:"title"`
	MessageContent string 		`bson:"message_content"`
	Status NotificationStatus 	`bson:"status"`
	Amount int64 				`bson:"amount"`
	NewBalance int64 				`bson:"balance"`
	TransactionID string 		`bson:"transaction_id"`
	WalletID string 			`bson:"wallet_id"`
	FailedReason string    		`bson:"failed_reason,omitempty"`
	CreatedAt    time.Time 		`bson:"created_at"`
	UpdatedAt time.Time 		`bson:"updated_at"`
}


type SMSNotification struct {
	ID primitive.ObjectID 		`bson:"_id,omitempty"`
	PhoneNumber string			`bson:"phone_number"` 
	MessageContent string 		`bson:"message_content"`
	Status NotificationStatus 	`bson:"status"`
	Amount int64 				`bson:"amount"`
	NewBalance int64 				`bson:"balance"`
	TransactionID string 		`bson:"transaction_id"`
	WalletID string 			`bson:"wallet_id"`
	FailedReason string    		`bson:"failed_reason,omitempty"`
	CreatedAt    time.Time 		`bson:"created_at"`
	UpdatedAt time.Time 		`bson:"updated_at"`
}


// --- Domain Errors ---
// The service layer will check for these exact errors without knowing about
// MongoDB to completely separate service from db
//var (


//)


