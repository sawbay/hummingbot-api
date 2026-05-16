import asyncio
import logging
import os
from datetime import datetime
from pathlib import Path

import yaml

from fastapi import APIRouter, BackgroundTasks, Depends, HTTPException

from config import settings
from database import AsyncDatabaseManager, BotRunRepository
from deps import get_bot_archiver, get_bots_orchestrator, get_database_manager, get_docker_service
from models import StartBotAction, StopBotAction, V2ControllerDeployment, V2ScriptDeployment
from services.bots_orchestrator import BotsOrchestrator
from services.docker_service import DockerService
from utils.bot_archiver import BotArchiver
from utils.file_system import fs_util

# Create module-specific logger
logger = logging.getLogger(__name__)

router = APIRouter(tags=["Bot Orchestration"], prefix="/bot-orchestration")

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------
# How long (seconds) to keep polling after a container is created before
# giving up and declaring it healthy (it may just be slow to start).
_DEPLOY_HEALTH_CHECK_TIMEOUT = 60
# Interval (seconds) between container status polls during the health check.
_DEPLOY_HEALTH_CHECK_INTERVAL = 3

def _r2_key(path: str) -> str:
    prefix = settings.r2.prefix.strip("/")
    clean_path = path.strip("/")
    return f"{prefix}/{clean_path}" if prefix else clean_path

def _build_orchestration_payload(
    *,
    instance_name: str,
    strategy_type: str,
    strategy_name: str,
    credentials_profile: str,
    script_config: str | None,
    controllers_config: list[str],
    deployment_config: dict,
) -> dict:
    return {
        "request_id": f"orch-{instance_name}",
        "instance_name": instance_name,
        "strategy_type": strategy_type,
        "strategy_name": strategy_name,
        "credentials_profile": credentials_profile,
        "script_config": script_config,
        "controllers_config": controllers_config,
        "r2": {
            "prefix": settings.r2.prefix,
            "keys": {
                "credential_profile": _r2_key(f"credentials/{credentials_profile}/"),
                "script_config": _r2_key(f"conf/scripts/{script_config}") if script_config else None,
                "controllers": [_r2_key(f"conf/controllers/{controller}") for controller in controllers_config],
                "scripts_runtime": _r2_key("scripts/"),
                "controllers_runtime": _r2_key("controllers/"),
            },
        },
        "deployment_config": deployment_config,
    }


# ---------------------------------------------------------------------------
# Pre-flight validation
# ---------------------------------------------------------------------------

def validate_deployment_config(config: "V2ControllerDeployment") -> list[str]:
    """
    Synchronous pre-flight validator that checks all filesystem paths referenced
    by a deployment config before any Docker SDK call is made.

    Returns a list of error strings.  An empty list means the config is valid.

    Checks performed (in order):
    1. Credentials profile directory exists under ``bots/credentials/``.
    2. If ``script_config`` is set, the YAML file exists under
       ``bots/conf/scripts/`` and every ``controllers_config`` entry it
       references exists under ``bots/conf/controllers/``.
    """
    errors: list[str] = []

    # ── 1. Credentials profile ────────────────────────────────────────────────
    creds_dir = Path("bots/credentials") / config.credentials_profile
    if not creds_dir.is_dir():
        errors.append(
            f"Credentials profile '{config.credentials_profile}' not found at "
            f"bots/credentials/{config.credentials_profile}"
        )

    # ── 2. Script config (optional) ───────────────────────────────────────────
    if config.script_config:
        script_path = Path("bots/conf/scripts") / config.script_config
        if not script_path.exists():
            errors.append(
                f"Script config '{config.script_config}' not found at "
                f"bots/conf/scripts/{config.script_config}"
            )
        else:
            # Parse YAML and validate every controller referenced inside it
            try:
                with open(script_path) as fh:
                    script_data = yaml.safe_load(fh) or {}

                controller_entries = script_data.get("controllers_config", [])
                for entry in controller_entries:
                    ctrl_path = Path("bots/conf/controllers") / entry
                    if not ctrl_path.exists():
                        errors.append(
                            f"Controller config '{entry}' referenced in script config "
                            f"not found at bots/conf/controllers/{entry}"
                        )
            except Exception as e:
                errors.append(
                    f"Failed to parse script config '{config.script_config}': {e}"
                )

    return errors


@router.get("/status")
def get_active_bots_status(bots_manager: BotsOrchestrator = Depends(get_bots_orchestrator)):
    """
    Get the status of all active bots.

    Args:
        bots_manager: Bot orchestrator service dependency

    Returns:
        Dictionary with status and data containing all active bot statuses
    """
    return {"status": "success", "data": bots_manager.get_all_bots_status()}


@router.get("/mqtt")
def get_mqtt_status(bots_manager: BotsOrchestrator = Depends(get_bots_orchestrator)):
    """
    Get MQTT connection status and discovered bots.

    Args:
        bots_manager: Bot orchestrator service dependency

    Returns:
        Dictionary with MQTT connection status, discovered bots, and broker information
    """
    mqtt_connected = bots_manager.mqtt_manager.is_connected()
    discovered_bots = bots_manager.mqtt_manager.get_discovered_bots()
    active_bots = list(bots_manager.active_bots.keys())

    # Check client state
    client_state = "connected" if bots_manager.mqtt_manager.is_connected() else "disconnected"

    return {
        "status": "success",
        "data": {
            "mqtt_connected": mqtt_connected,
            "discovered_bots": discovered_bots,
            "active_bots": active_bots,
            "broker_host": bots_manager.broker_host,
            "broker_port": bots_manager.broker_port,
            "broker_username": bots_manager.broker_username,
            "client_state": client_state
        }
    }


@router.get("/{bot_name}/status")
def get_bot_status(bot_name: str, bots_manager: BotsOrchestrator = Depends(get_bots_orchestrator)):
    """
    Get the status of a specific bot.

    Args:
        bot_name: Name of the bot to get status for
        bots_manager: Bot orchestrator service dependency

    Returns:
        Dictionary with bot status information

    Raises:
        HTTPException: 404 if bot not found
    """
    response = bots_manager.get_bot_status(bot_name)
    if not response:
        raise HTTPException(status_code=404, detail="Bot not found")
    return {
        "status": "success",
        "data": response
    }


@router.get("/{bot_name}/history")
async def get_bot_history(
    bot_name: str,
    days: int = 0,
    verbose: bool = False,
    precision: int = None,
    timeout: float = 30.0,
    bots_manager: BotsOrchestrator = Depends(get_bots_orchestrator)
):
    """
    Get trading history for a bot with optional parameters.

    Args:
        bot_name: Name of the bot to get history for
        days: Number of days of history to retrieve (0 for all)
        verbose: Whether to include verbose output
        precision: Decimal precision for numerical values
        timeout: Timeout in seconds for the operation
        bots_manager: Bot orchestrator service dependency

    Returns:
        Dictionary with bot trading history
    """
    response = await bots_manager.get_bot_history(
        bot_name,
        days=days,
        verbose=verbose,
        precision=precision,
        timeout=timeout
    )
    return {"status": "success", "response": response}


@router.post("/start-bot")
async def start_bot(
    action: StartBotAction,
    bots_manager: BotsOrchestrator = Depends(get_bots_orchestrator),
    db_manager: AsyncDatabaseManager = Depends(get_database_manager)
):
    """
    Start a bot with the specified configuration.

    Args:
        action: StartBotAction containing bot configuration parameters
        bots_manager: Bot orchestrator service dependency
        db_manager: Database manager dependency

    Returns:
        Dictionary with status and response from bot start operation
    """
    response = await bots_manager.start_bot(
        action.bot_name, log_level=action.log_level, script=action.script,
        conf=action.conf, async_backend=action.async_backend
    )

    # Bot run tracking simplified - only track deployment and stop times

    return {"status": "success", "response": response}


@router.post("/stop-bot")
async def stop_bot(
    action: StopBotAction,
    bots_manager: BotsOrchestrator = Depends(get_bots_orchestrator),
    db_manager: AsyncDatabaseManager = Depends(get_database_manager)
):
    """
    Stop a bot with the specified configuration.

    Args:
        action: StopBotAction containing bot stop parameters
        bots_manager: Bot orchestrator service dependency
        db_manager: Database manager dependency

    Returns:
        Dictionary with status and response from bot stop operation
    """
    # Capture final status BEFORE stopping (performance data is cleared on stop)
    final_status = None
    try:
        final_status = bots_manager.get_bot_status(action.bot_name)
        logger.info(f"Captured final status for {action.bot_name} before stopping")
    except Exception as e:
        logger.warning(f"Failed to capture final status for {action.bot_name}: {e}")

    response = await bots_manager.stop_bot(
        action.bot_name, skip_order_cancellation=action.skip_order_cancellation,
        async_backend=action.async_backend
    )

    # Update bot run status to STOPPED if stop was successful
    if response.get("success"):
        try:
            async with db_manager.get_session_context() as session:
                bot_run_repo = BotRunRepository(session)
                await bot_run_repo.update_bot_run_stopped(
                    action.bot_name,
                    final_status=final_status
                )
                logger.info(f"Updated bot run status to STOPPED for {action.bot_name}")
        except Exception as e:
            logger.error(f"Failed to update bot run status: {e}")
            # Don't fail the stop operation if bot run update fails

    return {"status": "success", "response": response}


@router.get("/bot-runs")
async def get_bot_runs(
    bot_name: str = None,
    account_name: str = None,
    strategy_type: str = None,
    strategy_name: str = None,
    run_status: str = None,
    deployment_status: str = None,
    limit: int = 100,
    offset: int = 0,
    db_manager: AsyncDatabaseManager = Depends(get_database_manager)
):
    """
    Get bot runs with optional filtering.

    Args:
        bot_name: Filter by bot name
        account_name: Filter by account name
        strategy_type: Filter by strategy type (script or controller)
        strategy_name: Filter by strategy name
        run_status: Filter by run status (CREATED, RUNNING, STOPPED, ERROR)
        deployment_status: Filter by deployment status (DEPLOYED, FAILED, ARCHIVED)
        limit: Maximum number of results to return
        offset: Number of results to skip
        db_manager: Database manager dependency

    Returns:
        List of bot runs with their details
    """
    try:
        async with db_manager.get_session_context() as session:
            bot_run_repo = BotRunRepository(session)
            bot_runs = await bot_run_repo.get_bot_runs(
                bot_name=bot_name,
                account_name=account_name,
                strategy_type=strategy_type,
                strategy_name=strategy_name,
                run_status=run_status,
                deployment_status=deployment_status,
                limit=limit,
                offset=offset
            )

            # Convert bot runs to dictionaries for JSON serialization
            runs_data = []
            for run in bot_runs:
                run_dict = {
                    "id": run.id,
                    "bot_name": run.bot_name,
                    "instance_name": run.instance_name,
                    "deployed_at": run.deployed_at.isoformat() if run.deployed_at else None,
                    "stopped_at": run.stopped_at.isoformat() if run.stopped_at else None,
                    "strategy_type": run.strategy_type,
                    "strategy_name": run.strategy_name,
                    "config_name": run.config_name,
                    "account_name": run.account_name,
                    "image_version": run.image_version,
                    "deployment_status": run.deployment_status,
                    "run_status": run.run_status,
                    "deployment_config": run.deployment_config,
                    "final_status": run.final_status,
                    "error_message": run.error_message
                }
                runs_data.append(run_dict)

            return {
                "status": "success",
                "data": runs_data,
                "total": len(runs_data),
                "limit": limit,
                "offset": offset
            }
    except Exception as e:
        logger.error(f"Failed to get bot runs: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/bot-runs/{bot_run_id}")
async def get_bot_run_by_id(
    bot_run_id: int,
    db_manager: AsyncDatabaseManager = Depends(get_database_manager)
):
    """
    Get a specific bot run by ID.

    Args:
        bot_run_id: ID of the bot run
        db_manager: Database manager dependency

    Returns:
        Bot run details

    Raises:
        HTTPException: 404 if bot run not found
    """
    try:
        async with db_manager.get_session_context() as session:
            bot_run_repo = BotRunRepository(session)
            bot_run = await bot_run_repo.get_bot_run_by_id(bot_run_id)

            if not bot_run:
                raise HTTPException(status_code=404, detail=f"Bot run {bot_run_id} not found")

            run_dict = {
                "id": bot_run.id,
                "bot_name": bot_run.bot_name,
                "instance_name": bot_run.instance_name,
                "deployed_at": bot_run.deployed_at.isoformat() if bot_run.deployed_at else None,
                "stopped_at": bot_run.stopped_at.isoformat() if bot_run.stopped_at else None,
                "strategy_type": bot_run.strategy_type,
                "strategy_name": bot_run.strategy_name,
                "config_name": bot_run.config_name,
                "account_name": bot_run.account_name,
                "image_version": bot_run.image_version,
                "deployment_status": bot_run.deployment_status,
                "run_status": bot_run.run_status,
                "deployment_config": bot_run.deployment_config,
                "final_status": bot_run.final_status,
                "error_message": bot_run.error_message
            }

            return {"status": "success", "data": run_dict}
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Failed to get bot run {bot_run_id}: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/bot-runs/stats")
async def get_bot_run_stats(
    db_manager: AsyncDatabaseManager = Depends(get_database_manager)
):
    """
    Get statistics about bot runs.

    Args:
        db_manager: Database manager dependency

    Returns:
        Bot run statistics
    """
    try:
        async with db_manager.get_session_context() as session:
            bot_run_repo = BotRunRepository(session)
            stats = await bot_run_repo.get_bot_run_stats()

            return {"status": "success", "data": stats}
    except Exception as e:
        logger.error(f"Failed to get bot run stats: {e}")
        raise HTTPException(status_code=500, detail=str(e))


# ---------------------------------------------------------------------------
# Post-deploy health check
# ---------------------------------------------------------------------------

async def _post_deploy_health_check(
    instance_name: str,
    docker_manager: DockerService,
    bots_manager: BotsOrchestrator,
    db_manager: AsyncDatabaseManager,
):
    """
    Background task that monitors a newly-created container for the first
    `_DEPLOY_HEALTH_CHECK_TIMEOUT` seconds.

    - If the container exits with a non-zero code   → mark BotRun ERROR/FAILED,
      capture Docker logs, update pending registry.
    - If the container is still running after timeout → mark BotRun RUNNING
      (MQTT will catch up naturally).
    """
    logger.info(f"[health-check] Starting post-deploy check for {instance_name}")
    elapsed = 0

    while elapsed < _DEPLOY_HEALTH_CHECK_TIMEOUT:
        await asyncio.sleep(_DEPLOY_HEALTH_CHECK_INTERVAL)
        elapsed += _DEPLOY_HEALTH_CHECK_INTERVAL

        health = docker_manager.get_container_health(instance_name)

        if not health["found"]:
            # Container disappeared – treat as failure
            error_msg = f"Container not found after deployment: {health.get('error', 'unknown')}"
            logger.error(f"[health-check] {instance_name}: {error_msg}")
            bots_manager.mark_pending_bot_failed(instance_name, error_msg)
            try:
                async with db_manager.get_session_context() as session:
                    repo = BotRunRepository(session)
                    await repo.update_bot_run_failed(instance_name, error_msg)
            except Exception as db_err:
                logger.error(f"[health-check] DB update failed: {db_err}")
            return

        if not health["running"] and health["exit_code"] not in (None, 0):
            # Container crashed
            error_msg = health.get("error", f"Container exited with code {health['exit_code']}")
            container_logs = health.get("logs", "")
            logger.error(f"[health-check] {instance_name} crashed: {error_msg}")
            bots_manager.mark_pending_bot_failed(instance_name, error_msg)
            try:
                async with db_manager.get_session_context() as session:
                    repo = BotRunRepository(session)
                    await repo.update_bot_run_failed(instance_name, error_msg, container_logs)
            except Exception as db_err:
                logger.error(f"[health-check] DB update failed: {db_err}")
            return

        if health["running"]:
            # Container is running — mark RUNNING and stop polling
            logger.info(f"[health-check] {instance_name} is running after {elapsed}s")
            bots_manager.resolve_pending_bot(instance_name)
            try:
                async with db_manager.get_session_context() as session:
                    repo = BotRunRepository(session)
                    await repo.update_bot_run_running(instance_name)
            except Exception as db_err:
                logger.error(f"[health-check] DB update failed: {db_err}")
            return

        logger.debug(f"[health-check] {instance_name}: status={health['status']} ({elapsed}s elapsed)")

    # Timed out — container is still in a non-running, non-crashed state (e.g. 'created').
    # Optimistically mark as RUNNING; the bot may still be initialising.
    logger.warning(
        f"[health-check] {instance_name}: timed out after {_DEPLOY_HEALTH_CHECK_TIMEOUT}s — "
        "assuming still starting up"
    )
    bots_manager.resolve_pending_bot(instance_name)
    try:
        async with db_manager.get_session_context() as session:
            repo = BotRunRepository(session)
            await repo.update_bot_run_running(instance_name)
    except Exception as db_err:
        logger.error(f"[health-check] DB update failed: {db_err}")


async def _background_stop_and_archive(
    bot_name: str,
    container_name: str,
    bot_name_for_orchestrator: str,
    skip_order_cancellation: bool,
    archive_locally: bool,
    s3_bucket: str,
    bots_manager: BotsOrchestrator,
    docker_manager: DockerService,
    bot_archiver: BotArchiver,
    db_manager: AsyncDatabaseManager
):
    """Background task to handle the stop and archive process"""
    try:
        logger.info(f"Starting background stop-and-archive for {bot_name}")

        # Step 1: Capture bot final status before stopping (while bot is still running)
        logger.info(f"Capturing final status for {bot_name_for_orchestrator}")
        final_status = None
        try:
            final_status = bots_manager.get_bot_status(bot_name_for_orchestrator)
            logger.info(f"Captured final status for {bot_name_for_orchestrator}: {final_status}")
        except Exception as e:
            logger.warning(f"Failed to capture final status for {bot_name_for_orchestrator}: {e}")

        # Step 2: Update bot run with stopped_at timestamp and final status before stopping
        try:
            async with db_manager.get_session_context() as session:
                bot_run_repo = BotRunRepository(session)
                await bot_run_repo.update_bot_run_stopped(
                    bot_name,
                    final_status=final_status
                )
                logger.info(f"Updated bot run with stopped_at timestamp and final status for {bot_name}")
        except Exception as e:
            logger.error(f"Failed to update bot run with stopped status: {e}")
            # Continue with stop process even if database update fails

        # Step 3: Mark the bot as stopping, and stop the bot trading process
        bots_manager.set_bot_stopping(bot_name_for_orchestrator)
        logger.info(f"Stopping bot trading process for {bot_name_for_orchestrator}")
        stop_response = await bots_manager.stop_bot(
            bot_name_for_orchestrator,
            skip_order_cancellation=skip_order_cancellation,
            async_backend=True  # Always use async for background tasks
        )

        if not stop_response or not stop_response.get("success", False):
            error_msg = stop_response.get('error', 'Unknown error') if stop_response else 'No response from bot orchestrator'
            logger.error(f"Failed to stop bot process: {error_msg}")
            return

        # Step 4: Wait for graceful shutdown (15 seconds as requested)
        logger.info(f"Waiting 15 seconds for bot {bot_name} to gracefully shutdown")
        await asyncio.sleep(15)

        # Step 5: Stop the container with monitoring
        max_retries = 10
        retry_interval = 2
        container_stopped = False

        for i in range(max_retries):
            logger.info(f"Attempting to stop container {container_name} (attempt {i+1}/{max_retries})")
            docker_manager.stop_container(container_name)

            # Check if container is already stopped
            container_status = docker_manager.get_container_status(container_name)
            if container_status.get("state", {}).get("status") == "exited":
                container_stopped = True
                logger.info(f"Container {container_name} is already stopped")
                break

            await asyncio.sleep(retry_interval)

        if not container_stopped:
            logger.error(f"Failed to stop container {container_name} after {max_retries} attempts")
            return

        # Step 6: Archive the bot data
        instance_dir = os.path.join('bots', 'instances', container_name)
        logger.info(f"Archiving bot data from {instance_dir}")

        try:
            if archive_locally:
                bot_archiver.archive_locally(container_name, instance_dir)
            else:
                bot_archiver.archive_and_upload(container_name, instance_dir, bucket_name=s3_bucket)
            logger.info(f"Successfully archived bot data for {container_name}")
        except Exception as e:
            logger.error(f"Archive failed: {str(e)}")
            # Continue with removal even if archive fails

        # Step 7: Remove the container
        logging.info(f"Removing container {container_name}")
        remove_response = docker_manager.remove_container(container_name, force=False)

        if not remove_response.get("success"):
            # If graceful remove fails, try force remove
            logging.warning("Graceful container removal failed, attempting force removal")
            remove_response = docker_manager.remove_container(container_name, force=True)

        if remove_response.get("success"):
            logging.info(f"Successfully completed stop-and-archive for bot {bot_name}")

            # Step 8: Update bot run deployment status to ARCHIVED
            try:
                async with db_manager.get_session_context() as session:
                    bot_run_repo = BotRunRepository(session)
                    await bot_run_repo.update_bot_run_archived(bot_name)
                    logger.info(f"Updated bot run deployment status to ARCHIVED for {bot_name}")
            except Exception as e:
                logger.error(f"Failed to update bot run to archived: {e}")
        else:
            logging.error(f"Failed to remove container {container_name}")

            # Update bot run with error status (but keep stopped_at timestamp from earlier)
            try:
                async with db_manager.get_session_context() as session:
                    bot_run_repo = BotRunRepository(session)
                    await bot_run_repo.update_bot_run_stopped(
                        bot_name,
                        error_message="Failed to remove container during archive process"
                    )
                    logger.info(f"Updated bot run with error status for {bot_name}")
            except Exception as e:
                logger.error(f"Failed to update bot run with error: {e}")

    except Exception as e:
        logging.error(f"Error in background stop-and-archive for {bot_name}: {str(e)}")

        # Update bot run with error status
        try:
            async with db_manager.get_session_context() as session:
                bot_run_repo = BotRunRepository(session)
                await bot_run_repo.update_bot_run_stopped(
                    bot_name,
                    error_message=str(e)
                )
                logger.info(f"Updated bot run with error status for {bot_name}")
        except Exception as db_error:
            logger.error(f"Failed to update bot run with error: {db_error}")
    finally:
        # Always clear the stopping status when the background task completes
        bots_manager.clear_bot_stopping(bot_name_for_orchestrator)
        logger.info(f"Cleared stopping status for bot {bot_name}")

        # Remove bot from active_bots and clear all MQTT data
        if bot_name_for_orchestrator in bots_manager.active_bots:
            bots_manager.mqtt_manager.clear_bot_data(bot_name_for_orchestrator)
            del bots_manager.active_bots[bot_name_for_orchestrator]
            logger.info(f"Removed bot {bot_name_for_orchestrator} from active_bots and cleared MQTT data")


@router.post("/stop-and-archive-bot/{bot_name}")
async def stop_and_archive_bot(
    bot_name: str,
    background_tasks: BackgroundTasks,
    skip_order_cancellation: bool = True,
    archive_locally: bool = True,
    s3_bucket: str = None,
    bots_manager: BotsOrchestrator = Depends(get_bots_orchestrator),
    docker_manager: DockerService = Depends(get_docker_service),
    bot_archiver: BotArchiver = Depends(get_bot_archiver),
    db_manager: AsyncDatabaseManager = Depends(get_database_manager)
):
    """
    Gracefully stop a bot and archive its data in the background.
    This initiates a background task that will:
    1. Stop the bot trading process via MQTT
    2. Wait 15 seconds for graceful shutdown
    3. Monitor and stop the Docker container
    4. Archive the bot data (locally or to S3)
    5. Remove the container

    Returns immediately with a success message while the process continues in the background.
    """
    try:
        # Step 1: Normalize bot name and container name
        # Container name is now the same as bot name (no prefix added)
        actual_bot_name = bot_name
        container_name = bot_name

        logging.info(f"Normalized bot_name: {actual_bot_name}, container_name: {container_name}")

        # Step 2: Validate bot exists in active bots
        active_bots = list(bots_manager.active_bots.keys())

        # Check if bot exists in active bots (could be stored as either format)
        bot_found = (actual_bot_name in active_bots) or (container_name in active_bots)

        if not bot_found:
            return {
                "status": "error",
                "message": (
                    f"Bot '{actual_bot_name}' not found in active bots. "
                    f"Active bots: {active_bots}. Cannot perform graceful shutdown."
                ),
                "details": {
                    "input_name": bot_name,
                    "actual_bot_name": actual_bot_name,
                    "container_name": container_name,
                    "active_bots": active_bots,
                    "reason": "Bot must be actively managed via MQTT for graceful shutdown"
                }
            }

        # Use the format that's actually stored in active bots
        bot_name_for_orchestrator = container_name if container_name in active_bots else actual_bot_name

        # Add the background task
        background_tasks.add_task(
            _background_stop_and_archive,
            bot_name=actual_bot_name,
            container_name=container_name,
            bot_name_for_orchestrator=bot_name_for_orchestrator,
            skip_order_cancellation=skip_order_cancellation,
            archive_locally=archive_locally,
            s3_bucket=s3_bucket,
            bots_manager=bots_manager,
            docker_manager=docker_manager,
            bot_archiver=bot_archiver,
            db_manager=db_manager
        )

        return {
            "status": "success",
            "message": f"Stop and archive process started for bot {actual_bot_name}",
            "details": {
                "input_name": bot_name,
                "actual_bot_name": actual_bot_name,
                "container_name": container_name,
                "process": (
                    "The bot will be gracefully stopped, archived, and removed in the background. "
                    "This process typically takes 20-30 seconds."
                )
            }
        }

    except Exception as e:
        logging.error(f"Error initiating stop_and_archive_bot for {bot_name}: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e))


@router.post("/{bot_name}/controllers/{controller_id}/start")
async def start_controller(
    bot_name: str,
    controller_id: str,
    bots_manager: BotsOrchestrator = Depends(get_bots_orchestrator),
):
    """Start a single controller inside a running bot without restarting it."""
    result = await bots_manager.start_controller(bot_name, controller_id)
    if not result.get("success"):
        raise HTTPException(
            status_code=404 if "not found" in result.get("message", "") else 500,
            detail=result.get("message")
        )
    return {"status": "success", "data": result}


@router.post("/{bot_name}/controllers/{controller_id}/stop")
async def stop_controller(
    bot_name: str,
    controller_id: str,
    bots_manager: BotsOrchestrator = Depends(get_bots_orchestrator),
):
    """Stop a single controller inside a running bot without stopping other controllers."""
    result = await bots_manager.stop_controller(bot_name, controller_id)
    if not result.get("success"):
        raise HTTPException(
            status_code=404 if "not found" in result.get("message", "") else 500,
            detail=result.get("message")
        )
    return {"status": "success", "data": result}


@router.put("/{bot_name}/controllers/{controller_id}/config")
async def update_controller_config(
    bot_name: str,
    controller_id: str,
    params: dict,
    bots_manager: BotsOrchestrator = Depends(get_bots_orchestrator),
):
    """Hot-reload a single controller's config parameters while the bot keeps running."""
    result = await bots_manager.set_controller_config(bot_name, controller_id, params)
    if not result.get("success"):
        raise HTTPException(
            status_code=404 if "not found" in result.get("message", "") else 500,
            detail=result.get("message")
        )
    return {"status": "success", "data": result}


@router.post("/deploy-v2-controllers")
async def deploy_v2_controllers(
    deployment: V2ControllerDeployment,
    bots_manager: BotsOrchestrator = Depends(get_bots_orchestrator),
    db_manager: AsyncDatabaseManager = Depends(get_database_manager)
):
    """
    Deploy a V2 strategy with controllers by generating the script config and creating the instance.
    This endpoint simplifies the deployment process for V2 controller strategies.

    Args:
        deployment: V2ControllerDeployment configuration
        docker_manager: Docker service dependency

    Returns:
        Dictionary with deployment response and generated configuration details

    Raises:
        HTTPException: 500 if deployment fails
    """
    # Pre-flight validation — fast, synchronous, no Docker calls
    errors = validate_deployment_config(deployment)
    if errors:
        raise HTTPException(
            status_code=400,
            detail={"errors": errors, "message": "Deployment config validation failed"}
        )

    try:
        # Generate unique script config filename with timestamp
        timestamp = datetime.now().strftime("%Y%m%d-%H%M%S")
        script_config_filename = f"{deployment.instance_name}-{timestamp}.yml"
        # Use the same name with timestamp for the instance to ensure uniqueness
        unique_instance_name = f"{deployment.instance_name}-{timestamp}"

        # Ensure controller config names have .yml extension
        controllers_with_extension = []
        for controller in deployment.controllers_config:
            if not controller.endswith('.yml'):
                controllers_with_extension.append(f"{controller}.yml")
            else:
                controllers_with_extension.append(controller)

        # Create the script config content
        # Note: candles_config and markets removed - they're optional and empty,
        # and older hummingbot versions don't expect them in the config
        script_config_content = {
            "script_file_name": "v2_with_controllers.py",
            "controllers_config": controllers_with_extension,
        }

        # Add optional drawdown parameters if provided
        if deployment.max_global_drawdown_quote is not None:
            script_config_content["max_global_drawdown_quote"] = deployment.max_global_drawdown_quote
        if deployment.max_controller_drawdown_quote is not None:
            script_config_content["max_controller_drawdown_quote"] = deployment.max_controller_drawdown_quote

        # Save the script config to the scripts directory
        scripts_dir = os.path.join("conf", "scripts")

        script_config_path = os.path.join(scripts_dir, script_config_filename)
        fs_util.dump_dict_to_yaml(script_config_path, script_config_content)

        logging.info(f"Generated script config: {script_config_filename} with content: {script_config_content}")

        # Set generated config on the deployment and queue orchestration
        deployment.instance_name = unique_instance_name
        deployment.script_config = script_config_filename
        deployment_config = deployment.dict()

        # Register the bot as deploying so it shows up in status immediately
        bots_manager.register_pending_bot(
            unique_instance_name,
            {"strategy": "v2_with_controllers", "account": deployment.credentials_profile}
        )

        try:
            async with db_manager.get_session_context() as session:
                bot_run_repo = BotRunRepository(session)
                await bot_run_repo.create_bot_run(
                    bot_name=unique_instance_name,
                    instance_name=unique_instance_name,
                    strategy_type="controller",
                    strategy_name="v2_with_controllers",
                    account_name=deployment.credentials_profile,
                    config_name=script_config_filename,
                    image_version=deployment.image,
                    deployment_config=deployment_config
                )
                logger.info(f"Created queued bot run record for controller deployment {unique_instance_name}")
        except Exception as e:
            logger.error(f"Failed to create bot run record: {e}")
            bots_manager.mark_pending_bot_failed(unique_instance_name, str(e))
            raise HTTPException(status_code=500, detail=f"Failed to create bot run record: {e}")

        orchestration_payload = _build_orchestration_payload(
            instance_name=unique_instance_name,
            strategy_type="controller",
            strategy_name="v2_with_controllers",
            credentials_profile=deployment.credentials_profile,
            script_config=script_config_filename,
            controllers_config=controllers_with_extension,
            deployment_config=deployment_config,
        )
        published = await bots_manager.publish_orchestration_request(orchestration_payload)
        if not published:
            error_msg = "Failed to publish orchestration request to hbot/orchestrate"
            bots_manager.mark_pending_bot_failed(unique_instance_name, error_msg)
            try:
                async with db_manager.get_session_context() as session:
                    bot_run_repo = BotRunRepository(session)
                    await bot_run_repo.update_orchestration_failed(unique_instance_name, error_msg)
            except Exception as db_err:
                logger.error(f"Failed to update failed orchestration BotRun: {db_err}")
            raise HTTPException(status_code=503, detail=error_msg)

        return {
            "success": True,
            "message": f"Deployment {unique_instance_name} queued for rs-orchestrator.",
            "unique_instance_name": unique_instance_name,
            "script_config_generated": script_config_filename,
            "controllers_deployed": controllers_with_extension,
            "orchestration_status": "queued",
        }

    except HTTPException:
        raise
    except Exception as e:
        logging.error(f"Error deploying V2 controllers: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e))


@router.post("/deploy-v2-script")
async def deploy_v2_script(
    deployment: V2ScriptDeployment,
    bots_manager: BotsOrchestrator = Depends(get_bots_orchestrator),
    db_manager: AsyncDatabaseManager = Depends(get_database_manager)
):
    """
    Deploy a V2 script bot with optional script configuration.
    This endpoint creates and starts a Hummingbot instance running the specified script.

    Args:
        deployment: V2ScriptDeployment configuration containing instance name, credentials,
                   optional script name and configuration
        docker_manager: Docker service dependency
        db_manager: Database manager dependency

    Returns:
        Dictionary with deployment response including instance details

    Raises:
        HTTPException: 500 if deployment fails
    """
    # Pre-flight validation — fast, synchronous, no Docker calls
    errors = validate_deployment_config(deployment)
    if errors:
        raise HTTPException(
            status_code=400,
            detail={"errors": errors, "message": "Deployment config validation failed"}
        )

    try:
        # Generate unique instance name with timestamp
        timestamp = datetime.now().strftime("%Y%m%d-%H%M%S")
        unique_instance_name = f"{deployment.instance_name}-{timestamp}"

        # Update deployment with unique name
        deployment.instance_name = unique_instance_name

        deployment_config = deployment.dict()
        controllers_with_extension = []
        if deployment.script_config:
            try:
                script_config_content = fs_util.read_yaml_file(f"conf/scripts/{deployment.script_config}")
                controllers_with_extension = script_config_content.get("controllers_config", []) or []
            except Exception as e:
                raise HTTPException(status_code=400, detail=f"Failed to read script config controllers: {e}")

        bots_manager.register_pending_bot(
            unique_instance_name,
            {"strategy": deployment.script or "default", "account": deployment.credentials_profile}
        )

        try:
            async with db_manager.get_session_context() as session:
                bot_run_repo = BotRunRepository(session)
                await bot_run_repo.create_bot_run(
                    bot_name=unique_instance_name,
                    instance_name=unique_instance_name,
                    strategy_type="script",
                    strategy_name=deployment.script or "default",
                    account_name=deployment.credentials_profile,
                    config_name=deployment.script_config,
                    image_version=deployment.image,
                    deployment_config=deployment_config
                )
                logger.info(f"Created queued bot run record for script deployment {unique_instance_name}")
        except Exception as e:
            logger.error(f"Failed to create bot run record: {e}")
            bots_manager.mark_pending_bot_failed(unique_instance_name, str(e))
            raise HTTPException(status_code=500, detail=f"Failed to create bot run record: {e}")

        orchestration_payload = _build_orchestration_payload(
            instance_name=unique_instance_name,
            strategy_type="script",
            strategy_name=deployment.script or "default",
            credentials_profile=deployment.credentials_profile,
            script_config=deployment.script_config,
            controllers_config=controllers_with_extension,
            deployment_config=deployment_config,
        )
        published = await bots_manager.publish_orchestration_request(orchestration_payload)
        if not published:
            error_msg = "Failed to publish orchestration request to hbot/orchestrate"
            bots_manager.mark_pending_bot_failed(unique_instance_name, error_msg)
            try:
                async with db_manager.get_session_context() as session:
                    bot_run_repo = BotRunRepository(session)
                    await bot_run_repo.update_orchestration_failed(unique_instance_name, error_msg)
            except Exception as db_err:
                logger.error(f"Failed to update failed orchestration BotRun: {db_err}")
            raise HTTPException(status_code=503, detail=error_msg)

        return {
            "success": True,
            "message": f"Deployment {unique_instance_name} queued for rs-orchestrator.",
            "unique_instance_name": unique_instance_name,
            "orchestration_status": "queued",
        }

    except HTTPException:
        raise
    except Exception as e:
        logging.error(f"Error deploying V2 script: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/deployment-status/{instance_name}")
async def get_deployment_status(
    instance_name: str,
    bots_manager: BotsOrchestrator = Depends(get_bots_orchestrator),
    docker_manager: DockerService = Depends(get_docker_service),
    db_manager: AsyncDatabaseManager = Depends(get_database_manager),
):
    """
    Fast polling endpoint for tracking a newly-deployed bot.

    Returns a unified status from three sources:
    - **db**: run_status (CREATED, RUNNING, ERROR) and deployment_status from the database.
    - **container**: real-time Docker container state (running, exited, …).
    - **orchestrator**: whether the bot has been discovered via Docker/MQTT.

    Frontend should poll this at ~2-3 second intervals until
    `overall_status` is `running` or `failed`.
    """
    # 1. Pending registry (in-memory, fastest)
    pending_meta = bots_manager.pending_bots.get(instance_name)

    # 2. Active bots (MQTT/Docker discovered)
    is_active = instance_name in bots_manager.active_bots

    # 3. Docker container health (one quick API call)
    container_health = docker_manager.get_container_health(instance_name)

    # 4. Database status
    db_status = None
    try:
        async with db_manager.get_session_context() as session:
            repo = BotRunRepository(session)
            db_status = await repo.get_deployment_status(instance_name)
    except Exception as e:
        logger.error(f"Failed to fetch deployment status from DB for {instance_name}: {e}")

    # Derive an overall_status that the frontend can act on immediately
    if is_active:
        overall_status = "running"
    elif pending_meta and pending_meta.get("status") == "failed":
        overall_status = "failed"
    elif container_health.get("exit_code") not in (None, 0) and not container_health.get("running"):
        overall_status = "failed"
    elif db_status and db_status.get("run_status") == "ERROR":
        overall_status = "failed"
    elif db_status and db_status.get("run_status") == "RUNNING":
        overall_status = "running"
    else:
        overall_status = "deploying"

    return {
        "status": "success",
        "data": {
            "instance_name": instance_name,
            "overall_status": overall_status,
            "orchestrator": {
                "is_active": is_active,
                "is_pending": pending_meta is not None,
                "pending_status": pending_meta.get("status") if pending_meta else None,
                "pending_error": pending_meta.get("error") if pending_meta else None,
            },
            "container": {
                "found": container_health.get("found"),
                "status": container_health.get("status"),
                "running": container_health.get("running"),
                "exit_code": container_health.get("exit_code"),
                "error": container_health.get("error"),
                # Include container logs only on failure to keep the response lightweight
                "logs": container_health.get("logs") if overall_status == "failed" else None,
            },
            "db": db_status,
        },
    }
