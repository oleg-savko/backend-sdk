import logging
from pathlib import Path
from typing import Any, Dict

import yaml
from preset_cli.api.clients.superset import SupersetClient
from preset_cli.cli.superset.sync.dbt.lib import is_match_tags

_logger = logging.getLogger(__name__)


def sync_roles(  # pylint: disable=too-many-locals, too-many-branches
        client: SupersetClient,
        manifest_path: Path,
        database: Any,
        tags: [str],
) -> Dict[Any, list]:
    """
    Read the DBT manifest and import models as datasets with metrics.
    """
    with open(manifest_path, encoding="utf-8") as input_:
        manifest = yaml.load(input_, Loader=yaml.SafeLoader)

    tags = set(tags)

    # add roles
    db_name = database['result']['database_name']
    role_map = {}
    configs = list(manifest["sources"].values()) + list(manifest["nodes"].values())
    for config in configs:

        model_tags = set(config['tags'])
        if not is_match_tags(tags, model_tags) or config["resource_type"] not in ["model", "source"]:
            continue

        for tag in model_tags:
            if tag not in role_map:
                role_map[tag] = []
            role_map[tag].append({
                'db': db_name,
                'table': config["name"]
            })

    for role, datasources in role_map.items():
        role_name = f"[{db_name}] {role}"
        _logger.info(f"Start sync Role {role_name}: {datasources}")
        client.sync_role(name=role_name, datasources=datasources)
        _logger.info(f"Role {role_name} synced: {datasources}")

    return role_map
