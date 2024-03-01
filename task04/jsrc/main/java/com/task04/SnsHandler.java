package com.task04;

import com.amazonaws.services.lambda.runtime.Context;
import com.amazonaws.services.lambda.runtime.RequestHandler;
import com.amazonaws.services.lambda.runtime.events.SNSEvent;
import com.syndicate.deployment.annotations.events.SnsEventSource;
import com.syndicate.deployment.annotations.lambda.LambdaHandler;
import com.syndicate.deployment.annotations.resources.DependsOn;
import com.syndicate.deployment.model.ResourceType;

@DependsOn(
	resourceType = ResourceType.SNS_TOPIC,
	name = "lambda_topic"
)
@LambdaHandler(
	lambdaName = "sns_handler",
	roleName = "sns_handler-role"
)
@SnsEventSource(
	targetTopic = "lambda_topic"
)
public class SnsHandler implements RequestHandler<SNSEvent, Void> {

	public Void handleRequest(SNSEvent snsEvent, Context context) {
		for(SNSEvent.SNSRecord sqsMessage : snsEvent.getRecords()) {
			processMessage(sqsMessage, context);
		}
		return null;
	}

	private void processMessage(SNSEvent.SNSRecord snsRecord, Context context) {
		try {
			context.getLogger().log(snsRecord.getSNS().getMessage());
		} catch (Exception e) {
			context.getLogger().log("Cannot process message [ " + snsRecord.getSNS().getMessageId() + " ]");
			throw e;
		}
	}
}
