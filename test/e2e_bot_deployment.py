import asyncio
import json
import logging
import sys
import time
import uuid
from typing import Dict, Any

import httpx
import websockets

# Configuration
BASE_URL = "http://localhost:8000"
WS_URL = "ws://localhost:8000/ws/executors"
AUTH = ("admin", "admin")
TIMEOUT = 60  # seconds

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

async def test_deployment_flow(success_case: bool = True):
    instance_name = f"e2e-test-{'success' if success_case else 'fail'}-{int(time.time())}"
    
    logger.info(f"Starting E2E test for {'SUCCESS' if success_case else 'FAILURE'} case")
    logger.info(f"Instance name: {instance_name}")

    # 1. Prepare deployment payload
    # This config matches the user's requested recurring buy setup for APT-USDT
    payload = {
        "instance_name": instance_name,
        "credentials_profile": "master_account",
        "controllers_config": ["usdc-usdt-recurring-buy"] if success_case else ["non-existent-config"],
        "image": "hummingbot/hummingbot:latest",
        "headless": True
    }
    
    logger.info(f"Using controllers_config: {payload['controllers_config']}")

    async with httpx.AsyncClient(auth=AUTH, timeout=10.0) as client:
        # 2. Trigger deployment
        logger.info("Triggering deployment via REST...")
        response = await client.post(f"{BASE_URL}/bot-orchestration/deploy-v2-controllers", json=payload)
        
        if response.status_code != 200:
            logger.error(f"Deployment failed to trigger: {response.text}")
            return False
            
        resp_data = response.json()
        unique_name = resp_data.get("unique_instance_name")
        logger.info(f"Deployment triggered. Unique name: {unique_name}")

        # 3. Connect to WebSocket and track status
        logger.info(f"Connecting to WebSocket: {WS_URL}")
        # Build auth header for WS
        import base64
        auth_str = f"{AUTH[0]}:{AUTH[1]}"
        auth_b64 = base64.b64encode(auth_str.encode()).decode()
        headers = {"Authorization": f"Basic {auth_b64}"}

        try:
            async with websockets.connect(WS_URL, additional_headers=headers) as ws:
                # Subscribe to bot_deployment
                sub_msg = {
                    "action": "subscribe",
                    "type": "bot_deployment",
                    "instance_name": unique_name,
                    "update_interval": 1.0
                }
                await ws.send(json.dumps(sub_msg))
                logger.info(f"Subscribed to bot_deployment for {unique_name}")

                start_time = time.time()
                terminal_reached = False
                final_status = None

                while time.time() - start_time < TIMEOUT:
                    try:
                        msg_str = await asyncio.wait_for(ws.recv(), timeout=5.0)
                        msg = json.loads(msg_str)
                        
                        msg_type = msg.get("type")
                        data = msg.get("data", {})
                        
                        if msg_type == "bot_deployment":
                            status = data.get("overall_status")
                            logger.info(f"WS Update: status={status}")
                            if status in ("running", "failed"):
                                final_status = status
                            
                        elif msg_type == "bot_deployment_resolved":
                            final_status = msg.get("final_status")
                            logger.info(f"WS Resolved: final_status={final_status}")
                            terminal_reached = True
                            break
                            
                        elif msg_type == "error":
                            logger.error(f"WS Error: {msg.get('message')}")
                            break
                            
                    except asyncio.TimeoutError:
                        continue

                # Wait a moment for background tasks to sync to DB
                await asyncio.sleep(5)

                if not terminal_reached:
                    logger.error("Timed out waiting for terminal state via WebSocket")
                    return False

                # 4. Final verification via DB
                logger.info("Verifying final state via REST/DB...")
                db_response = await client.get(
                    f"{BASE_URL}/bot-orchestration/bot-runs", 
                    params={"bot_name": unique_name}
                )
                
                if db_response.status_code == 200:
                    bot_runs = db_response.json().get("data", [])
                    if bot_runs:
                        run = bot_runs[0]
                        logger.info(f"DB State: run_status={run.get('run_status')}, deployment_status={run.get('deployment_status')}")
                        if not success_case:
                            error_msg = run.get('error_message')
                            if error_msg:
                                logger.info(f"Captured Logs (first 100 chars): {error_msg[0][:100]}...")
                            else:
                                logger.warning("No error message captured in DB yet")
                    else:
                        logger.error("BotRun record not found in DB")

                # Check if final status matches expectation
                expected_status = "running" if success_case else "failed"
                if final_status == expected_status:
                    logger.info(f"Test PASSED: Reached expected terminal state '{expected_status}'")
                    return True
                else:
                    logger.error(f"Test FAILED: Reached state '{final_status}', expected '{expected_status}'")
                    # If it hit 'running' but then crashed, it's a success of the plumbing but a failure of the test scenario config
                    return False


        except Exception as e:
            logger.error(f"WebSocket connection or tracking failed: {e}")
            return False

async def main():
    # Run success case as requested by user
    success_result = await test_deployment_flow(success_case=True)
    
    if not success_result:
        sys.exit(1)
    
    logger.info("E2E tests completed successfully")

if __name__ == "__main__":
    asyncio.run(main())
