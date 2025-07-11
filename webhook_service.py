from fastapi import FastAPI, Request, HTTPException
from supabase import create_client, Client
import os
from typing import Dict, Any
from datetime import datetime
import json
import logging


# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

app = FastAPI()

# Initialize Supabase client
SUPABASE_URL = os.environ.get("SUPABASE_URL")
SUPABASE_SERVICE_ROLE_KEY = os.environ.get("SUPABASE_SERVICE_ROLE_KEY")  # Use service role key instead
RESEND_WEBHOOK_SECRET = os.environ.get("RESEND_WEBHOOK_SECRET")

if not all([SUPABASE_URL, SUPABASE_SERVICE_ROLE_KEY]):
    raise ValueError("Missing required environment variables: SUPABASE_URL or SUPABASE_SERVICE_ROLE_KEY")

logger.info(f"Initializing with Supabase URL: {SUPABASE_URL[:20]}...")

# Initialize with service role key
supabase: Client = create_client(SUPABASE_URL, SUPABASE_SERVICE_ROLE_KEY)

@app.get("/")
def read_root():
    return {"status": "healthy"}

@app.post("/resend-webhook")
async def handle_webhook(request: Request):
    try:
        # Get the raw payload
        payload = await request.json()
        logger.info(f"Received webhook payload: {json.dumps(payload)[:200]}...")
        
        # Validate webhook signature if needed
        # TODO: Implement webhook signature validation using RESEND_WEBHOOK_SECRET
        
        # Extract relevant data from the webhook
        event_type = payload.get("type")
        if not event_type:
            logger.error("Missing event type in payload")
            raise HTTPException(status_code=400, detail="Missing event type")
            
        created_at = payload.get("created_at")
        data = payload.get("data", {})
        
        logger.info(f"Processing {event_type} event from {data.get('from')} to {data.get('to', [])}")
        
        # Prepare data for storage
        webhook_data = {
            "event_type": event_type,
            "created_at": created_at,
            "email_id": data.get("email_id"),
            "from_email": data.get("from"),
            "to_email": data.get("to", [])[0] if data.get("to") else None,
            "subject": data.get("subject"),
            "tags": json.dumps(data.get("tags", [])),
            "raw_payload": json.dumps(payload),
            "processed_at": datetime.utcnow().isoformat()
        }
        
        # Add event-specific data
        if event_type == "email.bounced":
            webhook_data.update({
                "bounce_type": data.get("bounce", {}).get("type"),
                "bounce_subtype": data.get("bounce", {}).get("subType"),
                "bounce_message": data.get("bounce", {}).get("message")
            })
        elif event_type == "email.clicked":
            click_data = data.get("click", {})
            webhook_data.update({
                "click_ip": click_data.get("ipAddress"),
                "click_link": click_data.get("link"),
                "click_user_agent": click_data.get("userAgent"),
                "click_timestamp": click_data.get("timestamp")
            })
        elif event_type == "email.opened":
            # Add specific opened event data
            webhook_data.update({
                "opened_count": 1,  # Initial open
                "first_opened_at": created_at,
                "last_opened_at": created_at,
                "device_info": json.dumps(data.get("device_info", {})),
                "location_info": json.dumps(data.get("location_info", {}))
            })
            
            # Check if this email was already opened
            try:
                logger.info(f"Checking for existing opened event for email_id: {data.get('email_id')}")
                existing = supabase.table("email_events").select("*").eq("email_id", data.get("email_id")).eq("event_type", "email.opened").execute()
                if existing.data:
                    # Update existing opened event
                    existing_event = existing.data[0]
                    webhook_data["opened_count"] = existing_event.get("opened_count", 0) + 1
                    webhook_data["first_opened_at"] = existing_event.get("first_opened_at", created_at)
                    webhook_data["last_opened_at"] = created_at
                    
                    logger.info(f"Updating existing opened event with count: {webhook_data['opened_count']}")
                    # Update instead of insert
                    result = supabase.table("email_events").update(webhook_data).eq("id", existing_event["id"]).execute()
                    return {"status": "success", "message": f"Updated {event_type} event"}
            except Exception as e:
                logger.error(f"Error checking existing opened event: {str(e)}")
                raise
        
        # Store in Supabase
        logger.info("Inserting new event into Supabase")
        result = supabase.table("email_events").insert(webhook_data).execute()
        
        if not result.data:
            logger.error("Failed to store webhook data in Supabase")
            raise HTTPException(status_code=500, detail="Failed to store webhook data")
            
        logger.info(f"Successfully stored {event_type} event")
        return {"status": "success", "message": f"Stored {event_type} event"}
        
    except Exception as e:
        logger.error(f"Error processing webhook: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/test-connection")
async def test_connection():
    try:
        # Test Supabase connection
        logger.info("Testing Supabase connection...")
        result = supabase.table("email_events").select("count").limit(1).execute()
        
        return {
            "status": "healthy",
            "supabase_connected": True,
            "supabase_url": SUPABASE_URL[:20] + "...",  # Only show part of the URL for security
            "environment_vars_set": {
                "SUPABASE_URL": bool(SUPABASE_URL),
                "SUPABASE_SERVICE_ROLE_KEY": bool(SUPABASE_SERVICE_ROLE_KEY),
                "RESEND_WEBHOOK_SECRET": bool(RESEND_WEBHOOK_SECRET)
            }
        }
    except Exception as e:
        logger.error(f"Connection test failed: {str(e)}")
        return {
            "status": "error",
            "error": str(e),
            "supabase_connected": False,
            "environment_vars_set": {
                "SUPABASE_URL": bool(SUPABASE_URL),
                "SUPABASE_SERVICE_ROLE_KEY": bool(SUPABASE_SERVICE_ROLE_KEY),
                "RESEND_WEBHOOK_SECRET": bool(RESEND_WEBHOOK_SECRET)
            }
        }

if __name__ == "__main__":
    import uvicorn
    logger.info("Starting webhook service...")
    uvicorn.run(app, host="0.0.0.0", port=8000) 
