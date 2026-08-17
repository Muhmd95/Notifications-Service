package notifications

import "time"

type CreatePushNotificationRequest struct {
	DeviceToken   string // the phonenumber for now (put in the grpc server)
	Title         string // put in the service layer
	Message       string
	Amount        int64
	Balance       int64
	TransactionID string
	WalletID      string
	CreatedAt     time.Time
}

type CreateSMSNotificationRequest struct {
	PhoneNumber   string
	Message       string
	Amount        int64
	Balance       int64
	TransactionID string
	WalletID      string
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
