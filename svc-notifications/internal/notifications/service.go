package notifications

import (
	"context"
	"math/rand"
	"time"
	"svc-notifications/util/logger"
)

type Service struct {
	repo Repository // this is the repository interface that will be used to interact with the database
}

func NewService(repo Repository) *Service {
	return &Service{repo: repo}
}

func (s *Service) SendPushNotification(ctx context.Context, pn *CreatePushNotificationRequest) (*CreatePushNotificationResponse, error) {
	log := logger.Ctx(ctx)
	notification := &PushNotification{
		DeviceToken:    pn.DeviceToken,
		Title:          "Push Notification",
		MessageContent: pn.Message,
		Amount:         pn.Amount,
		Balance:        pn.Balance,
		TransactionID:  pn.TransactionID,
		WalletID:       pn.WalletID,
		Status:         StatusPending,
		CreatedAt:     time.Now(),
		UpdatedAt:     time.Now(),
	}
	err := s.repo.SavePushNotification(ctx, notification)
	if err != nil {
		log.Error().Err(err).Msg("couldn't save the push notification (service layer)")
		return nil, err
	}

	log.Info().Msg("starting processing the push notification to the user")
	err = s.repo.ModifyPushNotificationStatus(ctx, notification.ID.Hex(), StatusProcessing, "")
	if err != nil {
		log.Error().Err(err).Msg("couldn't start sending the push notification (service layer)")
		return nil, err
	}
	prop := rand.Float64()
	if prop < 0.8 {
		err = s.repo.ModifyPushNotificationStatus(ctx, notification.ID.Hex(), StatusSuccessful, "")
		if err != nil {
			log.Error().Err(err).Msg("couldn't mark the push notification as successful (service layer)")

			return nil, err
		}
		return &CreatePushNotificationResponse{
			Success: true,
			NotificationID: notification.ID.Hex(),
		}, nil
	} else {
		err = s.repo.ModifyPushNotificationStatus(ctx, notification.ID.Hex(), StatusFailed, "you're weak")
		if err != nil {
			log.Error().Err(err).Msg("couldn't mark the push notification as Failed (service layer)")

			return nil, err
		}
		return &CreatePushNotificationResponse{
			Success: false,
			NotificationID: notification.ID.Hex(),
		}, nil
	}

}


func (s *Service) SendSMSNotification(ctx context.Context, pn *CreateSMSNotificationRequest) (*CreateSMSNotificationResponse, error) {
	log := logger.Ctx(ctx)
	notification := &SMSNotification{
		PhoneNumber: pn.PhoneNumber,
		MessageContent: pn.Message,
		Amount:         pn.Amount,
		Balance:        pn.Balance,
		TransactionID:  pn.TransactionID,
		WalletID:       pn.WalletID,
		Status:         StatusPending,
		CreatedAt:     time.Now(),
		UpdatedAt:     time.Now(),
	}
	err := s.repo.SaveSMSNotification(ctx, notification)
	if err != nil {
		log.Error().Err(err).Msg("couldn't save the sms notification (service layer)")
		return nil, err
	}

	log.Info().Msg("starting processing the sms notification to the user")
	err = s.repo.ModifySMSNotificationStatus(ctx, notification.ID.Hex(), StatusProcessing, "")
	if err != nil {
		log.Error().Err(err).Msg("couldn't start sending the sms notification (service layer)")
		return nil, err
	}
	prop := rand.Float64()
	if prop < 0.8 {
		err = s.repo.ModifySMSNotificationStatus(ctx, notification.ID.Hex(), StatusSuccessful, "")
		if err != nil {
			log.Error().Err(err).Msg("couldn't mark the sms notification as successful (service layer)")
			return nil, err
		}
		return &CreateSMSNotificationResponse{
			Success: true,
			NotificationID: notification.ID.Hex(),
		}, nil
	} else {
		err = s.repo.ModifySMSNotificationStatus(ctx, notification.ID.Hex(), StatusFailed, "you're weak")
		if err != nil {
			log.Error().Err(err).Msg("couldn't mark the sms notification as Failed (service layer)")
			return nil, err
		}
		return &CreateSMSNotificationResponse{
			Success: false,
			NotificationID: notification.ID.Hex(),
		}, nil
	}

}
