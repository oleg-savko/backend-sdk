"""
Sync DBT datasets/metrics to Superset.
"""

# pylint: disable=consider-using-f-string

import json
import logging
from collections import defaultdict
from pathlib import Path
from typing import Any, Dict, List

import yaml
from preset_cli.api.clients.superset import SupersetClient
from preset_cli.api.operators import OneToMany
from preset_cli.cli.superset.sync.dbt.lib import is_match_tags
from yarl import URL

_logger = logging.getLogger(__name__)


def get_metric_expression(metric: Dict[str, Any], dataset_metrics: List[Dict[str, Any]]) -> str:
    """
    Return a SQL expression for a given DBT metric.
    """
    if metric["calculation_method"] == "derived":
        metric_expression: str = metric["expression"]
        for m in dataset_metrics:
            metric_expression = metric_expression.replace(m["name"], "{calculation_method}({expression})".format(**m))
        return metric_expression
    else:
        return "{calculation_method}({expression})".format(**metric)


def sync_datasets(  # pylint: disable=too-many-locals, too-many-branches
        client: SupersetClient,
        manifest_path: Path,
        database: Any,
        disallow_edits: bool,
        external_url_prefix: str,
        tags: List[str],
) -> List[Any]:
    """
    Read the DBT manifest and import models as datasets with metrics.
    """
    base_url = URL(external_url_prefix) if external_url_prefix else None

    with open(manifest_path, encoding="utf-8") as input_:
        manifest = yaml.load(input_, Loader=yaml.SafeLoader)

    # extract metrics
    metrics: Dict[str, List[Dict[str, Any]]] = defaultdict(list)
    all_metrics = manifest["metrics"].values()
    for metric in all_metrics:
        if metric["calculation_method"] == "derived":
            derived_metric_unique_id = metric["depends_on"]["nodes"][0]
            derived_model_unique_id = next(m["depends_on"]["nodes"][0] for m in all_metrics if m['unique_id'] == derived_metric_unique_id)
            unique_id = derived_model_unique_id
        else:
            unique_id = metric["depends_on"]["nodes"][0]
        metrics[unique_id].append(metric)

    # add datasets
    datasets = []
    configs = list(manifest["sources"].values()) + list(manifest["nodes"].values())
    for config in configs:

        model_tags = config['tags']
        if not is_match_tags(tags, model_tags) or config["resource_type"] not in ["model", "source"]:
            continue

        filters = {
            "database": OneToMany(database["id"]),
            # "schema": config["schema"],
            "table_name": config["name"],
        }
        existing = client.get_datasets(**filters)
        if len(existing) > 1:
            print(existing)
            raise Exception("More than one dataset found")
            

        if existing:
            dataset = existing[0]
            _logger.info("Updating dataset %s", config["unique_id"])
        else:
            _logger.info("Creating dataset %s", config["unique_id"])
            try:
                dataset = client.create_dataset(
                    database=database["id"],
                    schema=config["schema"],
                    table_name=config["name"],
                )
            except Exception:  # pylint: disable=broad-except
                # Superset can't add tables from different BigQuery projects
                continue

        extra = {k: config[k] for k in ["resource_type", "unique_id"]}
        if config["resource_type"] == "source":
            extra["depends_on"] = "source('{schema}', '{name}')".format(**config)
        else:  # config["resource_type"] == "model"
            extra["depends_on"] = "ref('{name}')".format(**config)

        dataset_metrics = [
        ]
        if config["resource_type"] == "model":
            model_metrics = metrics[config["unique_id"]]
            for metric in model_metrics:
                expression = get_metric_expression(metric, dataset_metrics=model_metrics)
                dataset_metrics.append(
                    {
                        "expression": expression,
                        "metric_name": metric["name"],
                        "metric_type": metric["calculation_method"],
                        "verbose_name": metric["label"],
                        "description": metric["description"],
                        **metric["meta"],
                    },
                )
                _logger.info("Updating metric %s: %s", metric["name"], expression)

        # update dataset clearing metrics...
        update = {
            "description": config["description"],
            "extra": json.dumps(extra),
            "is_managed_externally": disallow_edits,
            "metrics": []
        }
        if base_url:
            fragment = "!/{resource_type}/{unique_id}".format(**config)
            update["external_url"] = str(base_url.with_fragment(fragment))
        query_args = {"override_columns": "true"}
        client.update_dataset(dataset["id"], **update)

        # ...then update metrics
        dataset_metrics.append({
            "expression": 'count(*)',
            "metric_name": 'count',
            "metric_type": 'count',
            "verbose_name": 'count(*)',
            "description": '',
        })
        if dataset_metrics:
            update = {
                "metrics": dataset_metrics,
            }
            client.update_dataset(dataset["id"], query_args, **update)

        datasets.append(dataset)

    return datasets
