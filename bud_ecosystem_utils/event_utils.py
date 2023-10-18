import json
import logging
import os
from dapr.clients import DaprClient

logging.basicConfig(level=logging.DEBUG, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

EVENT_PUBSUB_NAME = os.getenv("EVENT_PUBSUB_NAME", "bud-redis-queue")
EVENT_PUBSUB_TOPIC = os.getenv("EVENT_PUBSUB_TOPIC", "activities")
RESULT_TOPIC = os.getenv("RESULT_TOPIC", "output-aggregator")

def publish_error_to_client(client: DaprClient, event: dict) -> None:
    try:
        payload = {
            "session_id": event['session_id'],
            "status": "error",
            "data": event["data"],
        }
        client.publish_event(
            pubsub_name=EVENT_PUBSUB_NAME,
            topic_name=RESULT_TOPIC,
            data=json.dumps(payload),
            data_content_type="application/json",
        )
        logger.info("Response Sent Successfully")
    except Exception as e:
        logger.error(f"Error in send_response: {e}")
        raise e
    
def publish_result(client: DaprClient, event: dict) -> None:
    try:
        payload = {
            "session_id": event['session_id'],
            "node_id": event["node_id"],
            "status": event["status"],
            "node_type":  event["node_type"],
            "data": event["data"],
        }
        client.publish_event(
            pubsub_name=EVENT_PUBSUB_NAME,
            topic_name=RESULT_TOPIC,
            data=json.dumps(payload),
            data_content_type="application/json",
        )
        logger.info("Response Sent Successfully")
    except Exception as e:
        logger.error(f"Error in send_response: {e}")
        raise e

def publish_activity(client: DaprClient, event: dict) -> None :
    """
    Publish an activity to the Dapr pub/sub component.

    Args:
        client (DaprClient): The Dapr client instance.
        event (dict): The event to publish.
    """
    try:
        
        activity = {
            "session_id": event['session_id'],
            "from": event["node_type"],
            "agent_id": event["agent_id"],
            "msg": event["msg"],
        }
        
        client.publish_event(
            pubsub_name=EVENT_PUBSUB_NAME,
            topic=EVENT_PUBSUB_TOPIC,
            data=json.dumps(activity),
            data_content_type="application/json",
        )
        logger.info(f"Published event: {activity}")
    except Exception as e:
        logger.error(f"Error in publish_activity: {str(e)}")
        raise e