package main

import (
	"context"
	// notificationsv1 "github.com/Muhmd95/Contracts/notifications/v1"
	"github.com/joho/godotenv"
	// "go.opentelemetry.io/contrib/instrumentation/google.golang.org/grpc/otelgrpc"
	// "google.golang.org/grpc"
	// "net"
	"os"
	"os/signal"
	"syscall"

	// project paths
	// "svc-notifications/api/grpcserver"
	"svc-notifications/external/mongodb"
	"svc-notifications/internal/notifications"
	"svc-notifications/util/logger"
	"svc-notifications/util/tracer"
	"svc-notifications/external/kafka/consumer"
)

func main() {
	// Initialize logger
	logger.InitLogger("svc-notifications")
	logger.Log.Info().Msg("Starting svc-notifications ...")

	// init the tracer
	tp, err := tracer.InitTracer("svc-notifications")
	if err != nil {
		logger.Log.Fatal().Err(err).Msg("Failed to initialize tracer")
	}

	defer func() {
		if err := tp.Shutdown(context.Background()); err != nil {
			logger.Log.Error().Err(err).Msg("Failed to shutdown tracer")
		}
	}()

	// load .ENV file
	if err := godotenv.Load(".ENV"); err != nil {
		logger.Log.Info().Msg("No .ENV file found, relying on os environment") // because when using docker env variables will be injected
	}

	// get the notifications grpc port
	grpcPort := os.Getenv("GRPC_SERVER_PORT")
	if grpcPort == "" {
		grpcPort = "50050" // default grpc port
	}

	// get mongo uri
	mongoURI := os.Getenv("MONGO_URI")
	if mongoURI == "" {
		logger.Log.Fatal().Msg("MONGO_URI environment variable is required but not set")
		// fatal crashes the app dont need to use os.exit(1)
	}

	// get database name
	dbName := os.Getenv("MONGO_DB_NAME")
	if dbName == "" {
		dbName = "notifications_db" // default database name
	}
	// get kafka brokers
	brokers := os.Getenv("KAFKA_BROKERS")
	if brokers == "" {
		logger.Log.Fatal().Msg("KAFKA_BROKERS environment variable is required but not set")
	}
	kafkaBrokers := []string{brokers} // convert to slice of strings
	
	// get kafka group id
	kafkaGroupID := os.Getenv("KAFKA_GROUP_ID")
	if kafkaGroupID == "" {
		logger.Log.Fatal().Msg("KAFKA_GROUP_ID environment variable is required but not set")
	}
	
	// get kafka topic
	kafkaTopic := os.Getenv("KAFKA_TOPIC")
	if kafkaTopic == "" {
		logger.Log.Fatal().Msg("KAFKA_TOPIC environment variable is required but not set")
	}

	mongoClient, err := mongodb.ConnectMongoDB(mongoURI)
	if err != nil {
		logger.Log.Fatal().Err(err).Msg("Failed to connect to MongoDB")
		// We crash the app here because it cannot run without a database
	}

	// ensure to disconnect the database
	defer func() {
		if err := mongoClient.Disconnect(context.Background()); err != nil {
			logger.Log.Error().Err(err).Msg("Failed to disconnect MongoDB")
		}
	}()

	// init the database
	database := mongoClient.Database(dbName)
	logger.Log.Info().Str("dbName", dbName).Msg("Using database")

	notificationRepo, err := mongodb.NewNotificationRepository(context.Background(), database) // passing context because this may introduce delay
	// the rest are only memory connections
	if err != nil {
		logger.Log.Fatal().Err(err).Msg("Failed to create notification repository")
	}

	// Initialize the notification service
	service := notifications.NewService(notificationRepo)

	//init kafka
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel() // ensure the context is canceled when main exits
	kafkaConsumer, err := consumer.NewConsumer(context.Background(), kafkaBrokers, kafkaGroupID, kafkaTopic, service)
	if err != nil {
		logger.Log.Fatal().Err(err).Msg("Failed to create Kafka consumer")
	}
	go kafkaConsumer.Run(ctx) // run the consumer in a separate goroutine
	defer kafkaConsumer.Close() // close the consumer when the app exits

	// run the grpc server in a separate goroutine
	// // Initialize the gRPC server
	// grpcServer := grpc.NewServer(
	// 	grpc.StatsHandler(otelgrpc.NewServerHandler()), // adding the grpc interceptor
	// 	// to extract the trace id from the incoming requests
	// )
	// myNotificationServer := grpcserver.NewNotificationServer(service)
	// // Register the gRPC server
	// notificationsv1.RegisterNotificationServiceServer(grpcServer, myNotificationServer)
	// listener, err := net.Listen("tcp", ":"+grpcPort)
	// if err != nil {
	// 	logger.Log.Fatal().Err(err).Msg("Failed to listen on gRPC port")
	// }
	// go func() {
	// 	logger.Log.Info().Str("port", grpcPort).Msg("gRPC server is listening")
	// 	if err := grpcServer.Serve(listener); err != nil {
	// 		logger.Log.Fatal().Err(err).Msg("gRPC server crashed")
	// 	}
	// }()

	// 2. Set up the signal listener
	quit := make(chan os.Signal, 1)
	signal.Notify(quit, os.Interrupt, syscall.SIGTERM)

	// 3. Block until a shutdown signal is caught
	<-quit
	logger.Log.Info().Msg("Shutting down svc-notifications gracefully...")

	// // stop the grpc server gracefully
	// grpcServer.GracefulStop()

	logger.Log.Info().Msg("svc-notifications exited safely")
}
