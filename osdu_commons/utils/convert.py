from copy import deepcopy
from typing import Union
import re

from osdu_commons.model.enums import SECURITY_CLASSIFICATION_TO_SRN, ResourceSecurityClassification, \
    ResourceCurationStatus, ResourceLifecycleStatus
from osdu_commons.utils.srn import SRN, SRNFormatException


def identity(item):
    return item


def srn(srn_or_type: Union[SRN, str]) -> SRN:
    if isinstance(srn_or_type, SRN):
        return srn_or_type

    if isinstance(srn_or_type, str):
        return SRN.from_string(srn_or_type)

    raise TypeError(f'Cannot convert {srn_or_type} to srn')


def resource_security_classification(srn_or_str: Union[SRN, str]) -> SRN:
    security_classification_srn = None

    if isinstance(srn_or_str, SRN):
        security_classification_srn = srn_or_str
    elif isinstance(srn_or_str, str):
        security_classification_srn = _convert_security_classification_from_short_str_or_none(srn_or_str)
        if not security_classification_srn:
            security_classification_srn = _convert_security_classification_from_srn_str_or_none(srn_or_str)

    if security_classification_srn is None:
        raise TypeError(f'Cannot convert {srn_or_str} to srn')

    _validate_security_classification_srn(security_classification_srn)
    return security_classification_srn


def _convert_security_classification_from_short_str_or_none(input_str):
    try:
        security_classification_type = ResourceSecurityClassification(input_str)
        return SECURITY_CLASSIFICATION_TO_SRN[security_classification_type]
    except ValueError:
        pass


def _convert_security_classification_from_srn_str_or_none(input_str):
    try:
        return SRN.from_string(input_str)
    except SRNFormatException:
        pass


def _validate_security_classification_srn(srn_):
    if srn_ not in SECURITY_CLASSIFICATION_TO_SRN.values():
        raise TypeError(f'Wrong ResourceSecurityClassification type {srn_}')


def list_(inner_converter=identity):
    def converter(input_iterable):
        return [inner_converter(item) for item in input_iterable]

    return converter


def copy(item):
    return deepcopy(item)


def class_from_dict(class_type):
    def converter(input_dict):
        if isinstance(input_dict, class_type):
            return input_dict
        else:
            return class_type(**input_dict)

    return converter


def dict_to_snake_case(dict_converter):
    def to_snake_case(name):
        s1 = re.sub('([a-z0-9])([A-Z][a-z]+)', r'\1_\2', name)
        return re.sub('([a-z0-9])([A-Z])', r'\1_\2', s1).lower()

    def converter(input_dict):
        if isinstance(input_dict, dict):
            dict_with_snake_case = {}
            for key, value in input_dict.items():
                dict_with_snake_case[to_snake_case(key)] = value
            return dict_converter(dict_with_snake_case)
        else:
            return dict_converter(input_dict)

    return converter


def class_from_camel_dict(class_type):
    return dict_to_snake_case(class_from_dict(class_type))


def resource_curation_status(curation_status_or_str: Union[ResourceCurationStatus, str, SRN]) -> ResourceCurationStatus:
    if isinstance(curation_status_or_str, ResourceCurationStatus):
        return curation_status_or_str
    if isinstance(curation_status_or_str, (str, SRN)):
        return ResourceCurationStatus(srn(curation_status_or_str))
    raise TypeError(f'Cannot convert {curation_status_or_str}')


def resource_lifecycle_status(lifecycle_status_or_str: Union[ResourceLifecycleStatus, str, SRN]) -> ResourceLifecycleStatus:
    if isinstance(lifecycle_status_or_str, ResourceLifecycleStatus):
        return lifecycle_status_or_str
    if isinstance(lifecycle_status_or_str, (str, SRN)):
        return ResourceLifecycleStatus(srn(lifecycle_status_or_str))
    raise TypeError(f'Cannot convert {lifecycle_status_or_str}')
