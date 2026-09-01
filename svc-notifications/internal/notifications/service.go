package notifications

import (
	"context"
	"errors"
	"fmt"
	"math/rand"
	"svc-notifications/util/logger"
	"time"
)

// the consumer's ONLY view of the service
// this is the contrsct between the service and the consumer
type TransactionEventProcessor interface {
	ProcessTransactionEvent(ctx context.Context, evt TransactionEvent) error
}

type Service struct {
	repo Repository // this is the repository interface that will be used to interact with the database
}

func NewService(repo Repository) *Service {

	return &Service{repo: repo}
}

func (s *Service) SendPushNotification(ctx context.Context, pn *CreatePushNotificationRequest) error {
	log := logger.Ctx(ctx)
	notification := &PushNotification{
		DeviceToken:    pn.DeviceToken,
		Title:          "Push Notification",
		MessageContent: pn.Message,
		Amount:         pn.Amount,
		EventType:      pn.EventType,
		NewBalance:     pn.Balance,
		TransactionID:  pn.TransactionID,
		WalletID:       pn.WalletID,
		NationalID:     pn.NationalID,
		Status:         StatusPending,
		CreatedAt:      pn.CreatedAt,
		UpdatedAt:      time.Now(),
	}
	err := s.repo.SavePushNotification(ctx, notification)
	if err != nil {
		if errors.Is(err, ErrNotificationExists) {
			log.Info().Err(err).Msg("Duplicate push notification detected, already processed (service layer)")
			return nil
		}
		log.Error().Err(err).Msg("couldn't save the push notification (service layer)")
		return err
	}

	log.Info().Msg("starting processing the push notification to the user")
	err = s.repo.ModifyPushNotificationStatus(ctx, notification.ID.Hex(), StatusProcessing, "")
	if err != nil {
		log.Error().Err(err).Msg("couldn't start sending the push notification (service layer)")
		return err
	}
	prop := rand.Float64()
	if prop < 0.9 {
		log.Info().Msg("push notification sent successfully to the user")
		err = s.repo.ModifyPushNotificationStatus(ctx, notification.ID.Hex(), StatusSuccessful, "")
		if err != nil {
			log.Error().Err(err).Msg("couldn't mark the push notification as successful (service layer)")

			return err
		}
		return nil
	} else {
		log.Info().Msg("push notification failed to send to the user")
		err = s.repo.ModifyPushNotificationStatus(ctx, notification.ID.Hex(), StatusFailed, "you're weak")
		if err != nil {
			log.Error().Err(err).Msg("couldn't mark the push notification as Failed (service layer)")

			return err
		}
		return nil
	}

}

func (s *Service) SendSMSNotification(ctx context.Context, pn *CreateSMSNotificationRequest) error {
	log := logger.Ctx(ctx)
	notification := &SMSNotification{
		PhoneNumber:    pn.PhoneNumber,
		MessageContent: pn.Message,
		Amount:         pn.Amount,
		EventType:      pn.EventType,
		NewBalance:     pn.Balance,
		TransactionID:  pn.TransactionID,
		WalletID:       pn.WalletID,
		NationalID:     pn.NationalID,
		Status:         StatusPending,
		CreatedAt:      pn.CreatedAt,
		UpdatedAt:      time.Now(),
	}
	err := s.repo.SaveSMSNotification(ctx, notification)
	if err != nil {
		if errors.Is(err, ErrNotificationExists) {
			log.Info().Err(err).Msg("Duplicate sms notification detected, already processed (service layer)")
			return nil
		}
		log.Error().Err(err).Msg("couldn't save the sms notification (service layer)")
		return err
	}

	log.Info().Msg("starting processing the sms notification to the user")
	err = s.repo.ModifySMSNotificationStatus(ctx, notification.ID.Hex(), StatusProcessing, "")
	if err != nil {
		log.Error().Err(err).Msg("couldn't start sending the sms notification (service layer)")
		return err
	}
	prop := rand.Float64()
	if prop < 0.9 {
		log.Info().Msg("sms notification sent successfully to the user")
		err = s.repo.ModifySMSNotificationStatus(ctx, notification.ID.Hex(), StatusSuccessful, "")
		if err != nil {
			log.Error().Err(err).Msg("couldn't mark the sms notification as successful (service layer)")
			return err
		}
		return nil
	} else {
		log.Info().Msg("sms notification failed to send to the user")
		err = s.repo.ModifySMSNotificationStatus(ctx, notification.ID.Hex(), StatusFailed, "you're weak")
		if err != nil {
			log.Error().Err(err).Msg("couldn't mark the sms notification as Failed (service layer)")
			return err
		}
		return nil
	}
}

func (s *Service) ProcessTransactionEvent(ctx context.Context, evt TransactionEvent) error {

	message, err := renderNotificationText(evt)
	if err != nil {
		return err
	}
	pushErr := s.SendPushNotification(ctx, buildPush(evt, message))
	smsErr := s.SendSMSNotification(ctx, buildSMS(evt, message))
	return errors.Join(pushErr, smsErr)

}





// helper functions for service layer to build the notification requests from the transaction event
func buildPush(evt TransactionEvent, message string) *CreatePushNotificationRequest {
	return &CreatePushNotificationRequest{
		DeviceToken:   evt.PhoneNumber,
		Title:         "Transfer Notification",
		Message:       message,
		EventType:     evt.EventType,
		Amount:        evt.Amount,
		Balance:       evt.BalanceAfter,
		TransactionID: evt.ID,
		WalletID:      evt.WalletID,
		NationalID:    evt.NationalID,
		CreatedAt:     time.Now(),
	}
}

func buildSMS(evt TransactionEvent, message string) *CreateSMSNotificationRequest {
	return &CreateSMSNotificationRequest{
		PhoneNumber:   evt.PhoneNumber,
		Message:       message,
		EventType:     evt.EventType,
		Amount:        evt.Amount,
		Balance:       evt.BalanceAfter,
		TransactionID: evt.ID,
		WalletID:      evt.WalletID,
		NationalID:    evt.NationalID,
		CreatedAt:     time.Now(),
	}
}


func renderNotificationText(evt TransactionEvent) (string, error) {
	var transactionType string
	if evt.EventType == "WITHDRAWAL" {
		transactionType = "WITHDRAWAL"
	} else if evt.EventType == "DEPOSIT" {
		transactionType = "DEPOSIT"
	} else {	
		if evt.PhoneNumber == evt.SenderPhoneNumber {
			transactionType = "TRANSFER_SENDER"
		} else {
			transactionType = "TRANSFER_RECEIVER"
		}
		// sender is the phone number and the receiver is the coupled phonenumber
	}

	switch transactionType {
	case "DEPOSIT":
		{
			return fmt.Sprintf("Deposit of %d to your wallet of phone number: %s completed at %s. New balance: %d.", evt.Amount, evt.PhoneNumber, time.UnixMilli(evt.OccurredAt).Format("02 Jan 2006, 15:04"), evt.BalanceAfter), nil
		}
	case "WITHDRAWAL":
		{
			return fmt.Sprintf("Withdrawal of %d from your wallet of phone number: %s completed at %s. New balance: %d.", evt.Amount, evt.PhoneNumber, time.UnixMilli(evt.OccurredAt).Format("02 Jan 2006, 15:04"), evt.BalanceAfter), nil
		}
	case "TRANSFER_SENDER":
		{
			return fmt.Sprintf("Transfer of %d from your wallet of phone number: %s to phone number: %s completed at %s. New balance: %d.", evt.Amount, evt.PhoneNumber, evt.ReceiverPhoneNumber, time.UnixMilli(evt.OccurredAt).Format("02 Jan 2006, 15:04"), evt.BalanceAfter), nil
		}
	case "TRANSFER_RECEIVER":
		{
			return fmt.Sprintf("Transfer of %d to your wallet of phone number: %s from phone number: %s completed at %s. New balance: %d.", evt.Amount, evt.PhoneNumber, evt.SenderPhoneNumber, time.UnixMilli(evt.OccurredAt).Format("02 Jan 2006, 15:04"), evt.BalanceAfter), nil
		}
	default:
		return "", errors.New("unknown transaction type")
	}
}
