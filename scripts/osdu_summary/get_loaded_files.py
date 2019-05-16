import csv
import json
import logging
import operator
from typing import Optional, Iterable

import attr
import click
from attr.validators import instance_of, optional

from osdu_commons.clients.cognito_client import get_cognito_headers
from osdu_commons.clients.delivery_client import DeliveryClient
from osdu_commons.clients.search_client import SearchClient, SearchResultFile
from osdu_commons.services.delivery_service import DeliveryService, DeliveredResource
from osdu_commons.services.search_service import SearchService
from osdu_commons.utils.srn import SRN

logging.basicConfig(level=logging.DEBUG)
logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)


@attr.s(frozen=True)
class OSDUFileSummary:
    srn: str = attr.ib(validator=instance_of(str))
    original_file_path: Optional[str] = attr.ib(validator=optional(instance_of(str)), default=None)
    staging_file_path: Optional[str] = attr.ib(validator=optional(instance_of(str)), default=None)
    file_source: Optional[str] = attr.ib(validator=optional(instance_of(str)), default=None)

    def asdict(self):
        return {
            'SRN': self.srn,
            'OriginalFilePath': self.original_file_path,
            'StagingFilePath': self.staging_file_path,
            'FileSource': self.file_source,
        }


def get_delivery_file_data_field(delivered_file: DeliveredResource, field_name: str):
    try:
        return delivered_file.data['GroupTypeProperties'][field_name]
    except (KeyError, TypeError):
        return None


def get_delivery_files(delivery_service: DeliveryService,
                       search_result_files: Iterable[SearchResultFile]) -> Iterable[OSDUFileSummary]:
    delivered_files = delivery_service.get_resources(SRN.from_string(f.srn) for f in search_result_files)
    for delivered_file in filter(operator.attrgetter('exists'), delivered_files):
        yield OSDUFileSummary(
            srn=str(delivered_file.srn),
            original_file_path=get_delivery_file_data_field(delivered_file, 'OriginalFilePath'),
            staging_file_path=get_delivery_file_data_field(delivered_file, 'StagingFilePath'),
            file_source=get_delivery_file_data_field(delivered_file, 'FileSource'),
        )


def save_file_summary_results(output_csv_path: str, osdu_file_summary_iter: Iterable[OSDUFileSummary]):
    with open(output_csv_path, 'w', newline='') as csvfile:
        fieldnames = ['SRN', 'OriginalFilePath', 'StagingFilePath', 'FileSource']
        writer = csv.DictWriter(csvfile, fieldnames=fieldnames)

        writer.writeheader()
        writer.writerows(file_summary.asdict() for file_summary in osdu_file_summary_iter)


@click.command()
@click.option('--config_path', help='Path to config file', required=True, type=str)
@click.option('--output_path', help='Path to output csv', required=True, type=str)
def main(config_path, output_path):
    with open(config_path) as config_fd:
        config = json.load(config_fd)

    cognito_headers = get_cognito_headers(config)
    delivery_client = DeliveryClient(base_url=config['DELIVERY_SERVICE_URL'], cognito_headers=cognito_headers)
    delivery_service = DeliveryService(delivery_client)
    search_client = SearchClient(base_url=config['SEARCH_SERVICE_URL'], cognito_headers=cognito_headers)
    search_service = SearchService(search_client)

    osdu_search_files_meta_gen = search_service.get_search_files()
    sdu_files_summary = get_delivery_files(delivery_service, osdu_search_files_meta_gen)
    save_file_summary_results(output_path, sdu_files_summary)


if __name__ == '__main__':
    main()
