package mongodb

import (
	"context"
	"time"

	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/bson/primitive"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"

	//project imports
	"svc-notifications/internal/notifications"
	"svc-notifications/util/logger"
)

type mongoRepository struct {
	pushCollection *mongo.Collection // this is the collection in the mongo database where the push are stored
	smsCollection  *mongo.Collection // this is the collection in the mongo database where the sms are stored
}

// this is the constructor for the mongoRepository struct it takes a mongo database and
//
//	returns a repository interface (to make the service layer interact only with the interface functions)
func NewNotificationRepository(ctx context.Context, db *mongo.Database) (notifications.Repository, error) {
	log := logger.Ctx(ctx)
	pushColl := db.Collection("push_notifications") // this is the collection in the mongo database where the push notifications are stored
	smsColl := db.Collection("sms_notifications")   // this is the collection in the mongo database where the sms notifications are stored
	
	_, err := smsColl.Indexes().CreateOne(ctx, mongo.IndexModel{ 
		Keys:    bson.M{"transaction_id": 1},                                   
		Options: options.Index().SetUnique(true).SetName("unique_transaction"), 
	})
	if err != nil {
		log.Error().Err(err).Msg("Failed to create the unique transaction index (from repo layer)")
		return nil, err
	}


	_, err = pushColl.Indexes().CreateOne(ctx, mongo.IndexModel{
		Keys:    bson.M{"transaction_id": 1},                                   
		Options: options.Index().SetUnique(true).SetName("unique_transaction"), 
	})
	if err != nil {
		log.Error().Err(err).Msg("Failed to create the unique transaction index (from repo layer)")
		return nil, err
	}

	return &mongoRepository{pushCollection: pushColl, smsCollection: smsColl}, nil // return the mongoRepository struct with the collections (this is a repository)

}

func (r *mongoRepository) SavePushNotification(ctx context.Context, pn *notifications.PushNotification) error {
	log := logger.Ctx(ctx)
	result, err := r.pushCollection.InsertOne(ctx, pn) // this is the method that
	// will insert the push notification into the collection in the mongo database
	// context is passed to know the timeout of therequest
	// if the request takes too long it will be cancelled
	//the time out of the request is embedded in the ctx
	// beside ctx contains meta data
	if err != nil {
		if mongo.IsDuplicateKeyError(err) {
			return notifications.ErrNotificationExists // return the domain error to indicate that the notification has already been processed
		}
		log.Error().Err(err).Msg("Failed to insert push notification (from repo layer)")
		return err // return any other error
	}
	pn.ID = result.InsertedID.(primitive.ObjectID) // update the push notification object with the generated ID
	return nil
}

func (r *mongoRepository) ModifyPushNotificationStatus(ctx context.Context, notificationID string, status notifications.NotificationStatus, failedReason string) error {
	log := logger.Ctx(ctx)
	objID, err := primitive.ObjectIDFromHex(notificationID)
	if err != nil {
		log.Error().Err(err).Msg("Failed to convert notificationID to ObjectID (from repo layer)")
		return err
	}
	if status == notifications.StatusFailed && failedReason != "" {
		updatedNotification := r.pushCollection.FindOneAndUpdate(ctx, bson.M{"_id": objID}, bson.M{"$set": bson.M{"status": status, "failed_reason": failedReason, "updated_at": time.Now()}})
		if updatedNotification.Err() != nil {
			log.Error().Err(updatedNotification.Err()).Msg("Failed to modify push notification (from repo layer)")
			return updatedNotification.Err() // return any other error
		}
		return nil
	} else {
		updatedNotification := r.pushCollection.FindOneAndUpdate(ctx, bson.M{"_id": objID}, bson.M{"$set": bson.M{"status": status, "updated_at": time.Now()}})
		if updatedNotification.Err() != nil {
			log.Error().Err(updatedNotification.Err()).Msg("Failed to modify push notification (from repo layer)")
			return updatedNotification.Err() // return any other error
		}
		return nil
	}
}

func (r *mongoRepository) SaveSMSNotification(ctx context.Context, sn *notifications.SMSNotification) error {
	log := logger.Ctx(ctx)
	result, err := r.smsCollection.InsertOne(ctx, sn) // this is the method that
	// will insert the sms notification into the collection in the mongo database
	// context is passed to know the timeout of therequest
	// if the request takes too long it will be cancelled
	//the time out of the request is embedded in the ctx
	// beside ctx contains meta data
	if err != nil {
		if mongo.IsDuplicateKeyError(err) {
			return notifications.ErrNotificationExists // return the domain error to indicate that the notification has already been processed
		}
		log.Error().Err(err).Msg("Failed to insert sms notification (from repo layer)")
		return err // return any other error
	}
	sn.ID = result.InsertedID.(primitive.ObjectID) // update the sms notification object with the generated ID
	return nil
}

func (r *mongoRepository) ModifySMSNotificationStatus(ctx context.Context, notificationID string, status notifications.NotificationStatus, failedReason string) error {
	log := logger.Ctx(ctx)
	objID, err := primitive.ObjectIDFromHex(notificationID)
	if err != nil {
		log.Error().Err(err).Msg("Failed to convert notificationID to ObjectID (from repo layer)")
		return err
	}
	if status == notifications.StatusFailed && failedReason != "" {
		updatedNotification := r.smsCollection.FindOneAndUpdate(ctx, bson.M{"_id": objID}, bson.M{"$set": bson.M{"status": status, "failed_reason": failedReason, "updated_at": time.Now()}})
		if updatedNotification.Err() != nil {
			log.Error().Err(updatedNotification.Err()).Msg("Failed to modify sms notification (from repo layer)")
			return updatedNotification.Err() // return any other error
		}
		return nil
	} else {
		updatedNotification := r.smsCollection.FindOneAndUpdate(ctx, bson.M{"_id": objID}, bson.M{"$set": bson.M{"status": status, "updated_at": time.Now()}})
		if updatedNotification.Err() != nil {
			log.Error().Err(updatedNotification.Err()).Msg("Failed to modify sms notification (from repo layer)")
			return updatedNotification.Err() // return any other error
		}
		return nil
	}
}
