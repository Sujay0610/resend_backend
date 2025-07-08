from fastapi import FastAPI, Request, HTTPException
from supabase import create_client, Client
import os
from typing import Dict, Any
from datetime import datetime
import json

app = FastAPI()

# Initialize Supabase client
SUPABASE_URL = os.environ.get("SUPABASE_URL")
SUPABASE_KEY = os.environ.get("SUPABASE_KEY")
RESEND_WEBHOOK_SECRET = os.environ.get("RESEND_WEBHOOK_SECRET")

if not all([SUPABASE_URL, SUPABASE_KEY]):
    raise ValueError("Missing required environment variables")

supabase: Client = create_client(SUPABASE_URL, SUPABASE_KEY)

@app.get("/")
def read_root():
    return {"status": "healthy"}

@app.post("/resend-webhook")
async def handle_webhook(request: Request):
    try:
        # Get the raw payload
        payload = await request.json()
        
        # Validate webhook signature if needed
        # TODO: Implement webhook signature validation using RESEND_WEBHOOK_SECRET
        
        # Extract relevant data from the webhook
        event_type = payload.get("type")
        if not event_type:
            raise HTTPException(status_code=400, detail="Missing event type")
            
        created_at = payload.get("created_at")
        data = payload.get("data", {})
        
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
                "device_info": data.get("device_info", {}),
                "location_info": data.get("location_info", {})
            })
            
            # Check if this email was already opened
            try:
                existing = supabase.table("email_events").select("*").eq("email_id", data.get("email_id")).eq("event_type", "email.opened").execute()
                if existing.data:
                    # Update existing opened event
                    existing_event = existing.data[0]
                    webhook_data["opened_count"] = existing_event.get("opened_count", 0) + 1
                    webhook_data["first_opened_at"] = existing_event.get("first_opened_at", created_at)
                    webhook_data["last_opened_at"] = created_at
                    
                    # Update instead of insert
                    result = supabase.table("email_events").update(webhook_data).eq("id", existing_event["id"]).execute()
                    return {"status": "success", "message": f"Updated {event_type} event"}
            except Exception as e:
                print(f"Error checking existing opened event: {str(e)}")
        
        # Store in Supabase
        result = supabase.table("email_events").insert(webhook_data).execute()
        
        if not result.data:
            raise HTTPException(status_code=500, detail="Failed to store webhook data")
            
        return {"status": "success", "message": f"Stored {event_type} event"}
        
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000) 