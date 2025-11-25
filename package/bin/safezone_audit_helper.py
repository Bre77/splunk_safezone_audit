import json
import logging
from datetime import datetime, timedelta

import requests
from solnlib import conf_manager, log
from solnlib.modular_input import checkpointer
from splunklib import modularinput as smi
from splunklib.modularinput.validation_definition import ET

ADDON_NAME = "safezone_audit"


def logger_for_input(input_name: str) -> logging.Logger:
    return log.Logs().get_logger(f"{ADDON_NAME.lower()}_{input_name}")


def get_account_config(session_key: str, account_name: str):
    cfm = conf_manager.ConfManager(
        session_key,
        ADDON_NAME,
        realm=f"__REST_CREDENTIAL__#{ADDON_NAME}#configs/conf-safezone_audit_account",
    )
    account_conf_file = cfm.get_conf("safezone_audit_account")
    account_config = account_conf_file.get(account_name)
    return {
        "username": account_config.get("username"),
        "password": account_config.get("password"),
        "customername": account_config.get("customername"),
    }


def get_checkpoint_key(input_name: str, account_name: str) -> str:
    """Generate a unique checkpoint key for this input and account combination."""
    return f"{input_name}_{account_name}_last_end_date"


def get_last_end_date(ckpt: checkpointer.KVStoreCheckpointer, key: str) -> datetime:
    """Get the last end date from checkpoint, or default to 90 days ago."""
    try:
        checkpoint_data = ckpt.get(key)
        if checkpoint_data and "last_end_date" in checkpoint_data:
            return datetime.fromisoformat(checkpoint_data["last_end_date"])
    except Exception:
        pass

    # Default to 90 days ago if no checkpoint exists
    return datetime.utcnow() - timedelta(days=90)


def save_checkpoint(
    ckpt: checkpointer.KVStoreCheckpointer, key: str, end_date: datetime
):
    """Save the end date from this run as the start date for the next run."""
    checkpoint_data = {
        "last_end_date": end_date.isoformat(),
        "updated_at": datetime.utcnow().isoformat(),
    }
    ckpt.update(key, checkpoint_data)


def get_data_from_api(
    logger: logging.Logger,
    config: dict[str, str],
    start_date: datetime,
    end_date: datetime,
):
    events = []
    total_records = 0

    with requests.Session() as session:
        # Add authentication
        if config.get("username") and config.get("password"):
            session.auth = (config["username"], config["password"])

        logger.info(f"Fetching safezones from {start_date} to {end_date}")

        # Get safezones list
        safezones_url = (
            f"https://{config['customername']}.criticalarc.net/api/safezones"
        )

        ids = session.get(safezones_url)
        ids.raise_for_status()

        # Parse XML with namespace handling
        xml_data = ET.fromstring(ids.content)
        namespace = {"ns": "urn:criticalarc:x:safezone"}
        safezones = xml_data.findall("ns:safezone", namespace)

        # Fallback to non-namespaced query if needed
        if len(safezones) == 0:
            safezones = xml_data.findall("safezone")

        logger.info(f"Found {len(safezones)} safezones to process")

        for safezone in safezones:
            safezone_id = safezone.attrib.get("id", "unknown")
            logger.debug(f"Processing safezone: {safezone_id}")

            # Format dates for API
            from_param = start_date.strftime("%Y-%m-%dT%H:%M:%SZ")
            to_param = end_date.strftime("%Y-%m-%dT%H:%M:%SZ")

            # Get audit records
            audit_url = f"https://{config['customername']}.criticalarc.net/api/audit/{safezone_id}/records"
            audit_params = {"from": from_param, "to": to_param}

            audit = session.get(audit_url, params=audit_params)
            audit.raise_for_status()

            # Parse audit XML with namespace handling
            audit_data = ET.fromstring(audit.content)
            audit_namespace = {"ns": "urn:intrinsic:x:auditing"}
            records = audit_data.findall("ns:record", audit_namespace)

            # Fallback to non-namespaced query if needed
            if len(records) == 0:
                records = audit_data.findall("record")

            logger.info(f"Found {len(records)} records for safezone {safezone_id}")
            total_records += len(records)

            for record in records:
                # Extract timestamp and safezone_id for separate fields
                timestamp = record.attrib.get("timestamp")

                # Convert XML element to dictionary (excluding timestamp and id)
                record_dict = {
                    "record_id": record.attrib.get("id"),
                }

                # Add all record attributes except 'id' and 'timestamp' (handled separately)
                for attr, value in record.attrib.items():
                    if attr not in ["id", "timestamp"]:
                        record_dict[attr] = value

                # Process child elements
                for child in record:
                    # Remove namespace from tag name if present
                    tag_name = (
                        child.tag.split("}")[-1] if "}" in child.tag else child.tag
                    )

                    if tag_name == "desc":
                        record_dict["description"] = child.text
                    elif tag_name == "params":
                        # Extract parameters
                        params = {}
                        for param in child:
                            name = param.attrib.get("name")
                            tag_id = param.attrib.get("tag-id")
                            param_type = param.attrib.get("type")
                            value = param.text

                            # Create unique key for parameter
                            if tag_id:
                                key = f"{name}_{tag_id}" if name else tag_id
                            else:
                                key = name if name else f"param_{param_type}"

                            params[key] = {
                                "value": value,
                                "type": param_type,
                                "name": name,
                                "tag_id": tag_id,
                            }
                        record_dict["params"] = params
                    elif tag_name == "tags":
                        # Extract tags
                        tags = []
                        for tag in child:
                            if tag.text:
                                tags.append(tag.text)
                        record_dict["tags"] = tags
                    else:
                        record_dict[tag_name] = child.text

                # Create event structure with separated time, source, and data
                event = {"time": timestamp, "source": safezone_id, "data": record_dict}

                events.append(event)

        logger.info(f"Total records processed: {total_records}")
        return events


def validate_input(definition: smi.ValidationDefinition):
    return


def stream_events(inputs: smi.InputDefinition, event_writer: smi.EventWriter):
    session_key = inputs.metadata["session_key"]

    # Initialize KV Store checkpointer
    try:
        ckpt = checkpointer.KVStoreCheckpointer(
            collection_name=f"{ADDON_NAME}_checkpoints",
            session_key=session_key,
            app=ADDON_NAME,
        )
    except Exception as e:
        logging.error(f"Failed to initialize KVStore checkpointer: {e}")
        return

    for input_name, input_item in inputs.inputs.items():
        normalized_input_name = input_name.split("/")[-1]
        logger = logger_for_input(normalized_input_name)

        try:
            log_level = conf_manager.get_log_level(
                logger=logger,
                session_key=session_key,
                app_name=ADDON_NAME,
                conf_name="safezone_audit_settings",
            )
            logger.setLevel(log_level)
            log.modular_input_start(logger, normalized_input_name)

            account_name = input_item.get("account")
            config = get_account_config(session_key, account_name)

            # Get checkpoint key and determine date range for this run
            checkpoint_key = get_checkpoint_key(normalized_input_name, account_name)
            start_date = get_last_end_date(ckpt, checkpoint_key)
            end_date = datetime.utcnow()

            logger.info(f"Processing data from {start_date} to {end_date}")

            data = get_data_from_api(logger, config, start_date, end_date)
            sourcetype = "safezone:audit"

            events_written = 0
            for event in data:
                # Use timestamp from event if available
                event_time = None
                if event.get("time"):
                    try:
                        event_time = datetime.fromisoformat(
                            event["time"].replace("Z", "+00:00")
                        ).timestamp()
                    except (ValueError, AttributeError):
                        pass

                # Use safezone_id as source
                event_source = event.get(
                    "source", f"{config['customername']}.criticalarc.net/api/audit"
                )

                event_writer.write_event(
                    smi.Event(
                        time=event_time,
                        data=json.dumps(event["data"], ensure_ascii=False, default=str),
                        index=input_item.get("index"),
                        sourcetype=sourcetype,
                        source=event_source,
                    )
                )
                events_written += 1

            # Save checkpoint with this run's end date
            save_checkpoint(ckpt, checkpoint_key, end_date)
            logger.info(f"Updated checkpoint with end date: {end_date}")

            log.events_ingested(
                logger,
                input_name,
                sourcetype,
                events_written,
                input_item.get("index"),
                account=account_name,
            )
            log.modular_input_end(logger, normalized_input_name)

        except Exception as e:
            log.log_exception(
                logger,
                e,
                "checkpoint_error",
                msg_before=f"Exception raised while ingesting data for {normalized_input_name}: ",
            )
