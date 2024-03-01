package com.task04;

import com.amazonaws.services.lambda.runtime.Context;
import com.amazonaws.services.lambda.runtime.RequestHandler;
import com.amazonaws.services.lambda.runtime.events.SQSEvent;
import com.syndicate.deployment.annotations.events.SqsTriggerEventSource;
import com.syndicate.deployment.annotations.lambda.LambdaHandler;
import com.syndicate.deployment.annotations.resources.DependsOn;
import com.syndicate.deployment.model.ResourceType;

@DependsOn(
	resourceType = ResourceType.SQS_QUEUE,
	name = "async_queue"
)
@LambdaHandler(
    lambdaName = "sqs_handler",
	roleName = "sqs_handler-role"
)
@SqsTriggerEventSource(
	targetQueue = "async_queue",
	batchSize = 1
)
public class SqsHandler implements RequestHandler<SQSEvent, Void> {

	public Void handleRequest(SQSEvent sqsEvent, Context context) {
		for(SQSEvent.SQSMessage sqsMessage : sqsEvent.getRecords()) {
			processMessage(sqsMessage, context);
		}
		return null;
	}

	private void processMessage(SQSEvent.SQSMessage sqsMessage, Context context) {
		try {
			context.getLogger().log(sqsMessage.getBody());
		} catch (Exception e) {
			context.getLogger().log("Cannot process message [ " + sqsMessage.getMessageId() + " ]");
			throw e;
		}
	}
}
