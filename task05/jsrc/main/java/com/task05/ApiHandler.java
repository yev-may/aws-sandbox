package com.task05;

import com.amazonaws.regions.Regions;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDB;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBClientBuilder;
import com.amazonaws.services.dynamodbv2.model.AttributeValue;
import com.amazonaws.services.lambda.runtime.Context;
import com.amazonaws.services.lambda.runtime.RequestHandler;
import com.syndicate.deployment.annotations.lambda.LambdaHandler;
import com.syndicate.deployment.annotations.resources.DependsOn;
import com.syndicate.deployment.model.DeploymentRuntime;
import com.syndicate.deployment.model.ResourceType;

import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;

@DependsOn(
	resourceType = ResourceType.DYNAMODB_TABLE,
	name = "Events"
)
@LambdaHandler(
	lambdaName = "api_handler",
	roleName = "api_handler-role",
	runtime = DeploymentRuntime.JAVA11
)
public class ApiHandler implements RequestHandler<RequestBody, ResponseBody> {

	private final AmazonDynamoDB db = AmazonDynamoDBClientBuilder.standard()
			.withRegion(Regions.EU_CENTRAL_1)
			.build();

	public ResponseBody handleRequest(RequestBody request, Context context) {
		context.getLogger().log("Request: [" + request + "]");

		Event event = createEventModel(request);

		Map<String, AttributeValue> eventDocument = createEventDocument(event);
		context.getLogger().log("Document: [" + eventDocument + "]");
		db.putItem("cmtr-fd7004e0-Events-test", eventDocument);

		return createResponseBody(event);
	}

	private Event createEventModel(RequestBody requestBody) {
		Event event = new Event();
		event.setId(String.valueOf(UUID.randomUUID()));
		event.setPrincipalId(requestBody.getPrincipalId());
		event.setCreatedAt(getCreateAt());
		event.setBody(requestBody.getContent());
		return event;
	}

	private String getCreateAt() {
		String format = "yyyy-MM-dd'T'HH:mm:ss.SSSSSS'Z'";
		DateTimeFormatter formatter = DateTimeFormatter.ofPattern(format);
		return formatter.format(LocalDateTime.now());
	}


	private Map<String, AttributeValue> createEventDocument(Event event) {
		Map<String, AttributeValue> document = new HashMap<>();
		document.put("id", new AttributeValue().withS(event.getId()));
		document.put("principalId", new AttributeValue().withN(String.valueOf(event.getPrincipalId())));
		document.put("createdAt", new AttributeValue().withS(event.getCreatedAt()));
		document.put("body", new AttributeValue().withM(toAttributeMap(event.getBody())));
		return document;
	}

	private Map<String, AttributeValue> toAttributeMap(Map<String, String> sourceMap) {
		Map<String, AttributeValue> attributeValueMap = new HashMap<>();
		for(Map.Entry<String, String> entry : sourceMap.entrySet()) {
			attributeValueMap.put(entry.getKey(), new AttributeValue(entry.getValue()));
		}
		return attributeValueMap;
	}


	private ResponseBody createResponseBody(Event event) {
		ResponseBody responseBody = new ResponseBody();
		responseBody.setStatusCode(201);
		responseBody.setEvent(event);
		return responseBody;
	}
}



