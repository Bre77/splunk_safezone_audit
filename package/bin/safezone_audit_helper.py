import json
import logging

import import_declare_test
import requests
from solnlib import conf_manager, log
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


def get_data_from_api(logger: logging.Logger, config: dict[str, str]):
    events = []
    with requests.Session() as session:
        ids = session.get(
            f"https://{config['customername']}.criticalarc.net/api/safezones",
        )
        ids.raise_for_status()

        # Parse XML
        xml_data = ET.fromstring(ids.content)
        safezones = xml_data.findall("safezone")

        for safezone in safezones:
            # Process each safezone element here
            audit = session.get(
                f"https://{config['customername']}.criticalarc.net/api/audit/{safezone.attrib['id']}/records",
                params={"from": "2022-01-01T00:00:00Z", "to": "2022-12-31T23:59:59Z"},
            )
            audit.raise_for_status()
            audit_data = ET.fromstring(audit.content)
            records = audit_data.findall("record")
            for record in records:
                # Process each record element here
                pass
        return ids


def validate_input(definition: smi.ValidationDefinition):
    return


def stream_events(inputs: smi.InputDefinition, event_writer: smi.EventWriter):
    # inputs.inputs is a Python dictionary object like:
    # {
    #   "safezone_audit://<input_name>": {
    #     "account": "<account_name>",
    #     "disabled": "0",
    #     "host": "$decideOnStartup",
    #     "index": "<index_name>",
    #     "interval": "<interval_value>",
    #     "python.version": "python3",
    #   },
    # }
    for input_name, input_item in inputs.inputs.items():
        normalized_input_name = input_name.split("/")[-1]
        logger = logger_for_input(normalized_input_name)
        try:
            session_key = inputs.metadata["session_key"]
            log_level = conf_manager.get_log_level(
                logger=logger,
                session_key=session_key,
                app_name=ADDON_NAME,
                conf_name="safezone_audit_settings",
            )
            logger.setLevel(log_level)
            log.modular_input_start(logger, normalized_input_name)
            config = get_account_config(session_key, input_item.get("account"))
            data = get_data_from_api(logger, config)
            sourcetype = "dummy-data"
            for line in data:
                event_writer.write_event(
                    smi.Event(
                        data=json.dumps(line, ensure_ascii=False, default=str),
                        index=input_item.get("index"),
                        sourcetype=sourcetype,
                    )
                )
            log.events_ingested(
                logger,
                input_name,
                sourcetype,
                len(data),
                input_item.get("index"),
                account=input_item.get("account"),
            )
            log.modular_input_end(logger, normalized_input_name)
        except Exception as e:
            log.log_exception(
                logger,
                e,
                "my custom error type",
                msg_before="Exception raised while ingesting data for demo_input: ",
            )
