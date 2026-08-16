package grpcserver

import (
	"context"
	

	notificationsv1 "github.com/Muhmd95/Contracts/notifications/v1"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"svc-notifications/internal/notifications"
	"svc-notifications/util/logger"
)

type NotificationServer struct {
	notificationsv1.UnimplementedNotificationServiceServer

	service *notifications.Service
}

func NewNotificationServer(service *notifications.Service) *NotificationServer {
	return &NotificationServer{service: service}
}

func (s *NotificationServer) SendSMSNotification(ctx context.Context, req *notificationsv1.SendNotificationRequest) (*notificationsv1.SendNotificationResponse, error) {
	log := logger.Ctx(ctx)

	createSMSNotificationRequest := &notifications.CreateSMSNotificationRequest{
		PhoneNumber:   req.GetPhoneNumber(),
		Message:       req.GetMessage(),
		TransactionID: req.GetTransactionId(),
		WalletID:      req.GetWalletId(),
		Amount:        req.GetAmount(),
		Balance:       req.GetBalance(),
	}

	response := &notificationsv1.SendNotificationResponse{}


	smsResponse, err := s.service.SendSMSNotification(ctx, createSMSNotificationRequest)
	if err != nil {
		log.Error().Err(err).Msg("Failed to send SMS notification (grpc server)")
		return nil, status.Error(codes.Internal, "Failed to send SMS notification")
	}
	response.NotificationId = smsResponse.NotificationID
	response.SmsSuccess = true


	createPushNotificationRequest := &notifications.CreatePushNotificationRequest{
		DeviceToken:   req.GetPhoneNumber(),
		Message:       req.GetMessage(),
		TransactionID: req.GetTransactionId(),
		WalletID:      req.GetWalletId(),
		Amount:        req.GetAmount(),
		Balance:       req.GetBalance(),
	}

	pushResponse, err := s.service.SendPushNotification(ctx, createPushNotificationRequest)
	if err != nil {
		log.Error().Err(err).Msg("Failed to send push notification (grpc server)")
		return nil, status.Error(codes.Internal, "Failed to send push notification")
	}
	response.NotificationId = pushResponse.NotificationID
	response.PushSuccess = true
	response.SmsSuccess = true

	return response, nil
}
